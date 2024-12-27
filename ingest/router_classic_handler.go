package main

import (
	"fmt"
	kafka2 "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gin-gonic/gin"
	"github.com/jitsucom/bulker/eventslog"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/jsonorder"
	"github.com/jitsucom/bulker/jitsubase/timestamp"
	"github.com/jitsucom/bulker/jitsubase/types"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/jitsucom/bulker/jitsubase/uuid"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	TokenName       = "token"
	APIKeyName      = "api_key"
	TokenHeaderName = "x-auth-token"

	JitsuAnonymIDCookie   = "__eventn_id"
	CookiePolicyParameter = "cookie_policy"
	IPPolicyParameter     = "ip_policy"

	KeepValue   = "keep"
	StrictValue = "strict"
	ComplyValue = "comply"
)

func (r *Router) ClassicHandler(c *gin.Context) {
	domain := ""
	var eventsLogId string
	var rError *appbase.RouterError
	var body []byte
	var ingestMessageBytes []byte
	var asyncDestinations []string
	ingestType := IngestTypeWriteKeyDefined
	var s2sEndpoint bool

	defer func() {
		if len(ingestMessageBytes) == 0 {
			ingestMessageBytes = body
		}
		if len(ingestMessageBytes) > 0 {
			_ = r.backupsLogger.Log(utils.DefaultString(eventsLogId, "UNKNOWN"), ingestMessageBytes)
		}
		if rError != nil && rError.ErrorType != ErrNoDst {
			obj := map[string]any{"body": string(ingestMessageBytes), "error": rError.PublicError.Error(), "status": utils.Ternary(rError.ErrorType == ErrThrottledType, "SKIPPED", "FAILED")}
			r.eventsLogService.PostAsync(&eventslog.ActorEvent{EventType: eventslog.EventTypeIncoming, Level: eventslog.LevelError, ActorId: eventsLogId, Event: obj})
			IngestHandlerRequests(domain, utils.Ternary(rError.ErrorType == ErrThrottledType, "throttled", "error"), rError.ErrorType).Inc()
			_ = r.producer.ProduceAsync(r.config.KafkaDestinationsDeadLetterTopicName, uuid.New(), ingestMessageBytes, map[string]string{"error": rError.Error.Error()}, kafka2.PartitionAny)
		} else {
			obj := map[string]any{"body": string(ingestMessageBytes), "asyncDestinations": asyncDestinations}
			if len(asyncDestinations) > 0 {
				obj["status"] = "SUCCESS"
			} else {
				obj["status"] = "SKIPPED"
				obj["error"] = ErrNoDst
			}
			r.eventsLogService.PostAsync(&eventslog.ActorEvent{EventType: eventslog.EventTypeIncoming, Level: eventslog.LevelInfo, ActorId: eventsLogId, Event: obj})
			IngestHandlerRequests(domain, "success", "").Inc()
		}
	}()
	defer func() {
		if rerr := recover(); rerr != nil {
			rError = r.ResponseError(c, http.StatusInternalServerError, "panic", true, fmt.Errorf("%v", rerr), true, true)
		}
	}()
	c.Set(appbase.ContextLoggerName, "ingest")
	if !strings.HasSuffix(c.ContentType(), "application/json") && !strings.HasSuffix(c.ContentType(), "text/plain") {
		rError = r.ResponseError(c, http.StatusBadRequest, "invalid content type", false, fmt.Errorf("%s. Expected: application/json", c.ContentType()), true, true)
		return
	}
	if c.FullPath() == "/api/v1/s2s/event" {
		// may still be overridden by write key type
		s2sEndpoint = true
	}
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		err = fmt.Errorf("Client Ip: %s: %v", utils.NvlString(c.GetHeader("X-Real-Ip"), c.GetHeader("X-Forwarded-For"), c.ClientIP()), err)
		rError = r.ResponseError(c, utils.Ternary(s2sEndpoint, http.StatusBadRequest, http.StatusOK), "error reading HTTP body", false, err, true, true)
		return
	}
	var message types.Json
	err = jsonorder.Unmarshal(body, &message)
	if err != nil {
		rError = r.ResponseError(c, utils.Ternary(s2sEndpoint, http.StatusBadRequest, http.StatusOK), "error parsing message", false, fmt.Errorf("%v: %s", err, string(body)), true, true)
		return
	}
	messageId := message.GetS("eventn_ctx_event_id")
	if messageId == "" {
		messageId = uuid.New()
	} else {
		messageId = utils.ShortenString(messageIdUnsupportedChars.ReplaceAllString(messageId, "_"), 64)
	}
	c.Set(appbase.ContextMessageId, messageId)
	loc, err := r.getDataLocator(c, ingestType, func() (token string) {
		token = utils.NvlString(c.Query(TokenName), c.GetHeader(TokenHeaderName), c.GetHeader(APIKeyName))
		if token != "" {
			return
		}
		for k, v := range c.Request.URL.Query() {
			if strings.HasPrefix(k, "p_") && len(v) > 0 {
				return v[0]
			}
		}
		return
	})
	if err != nil {
		rError = r.ResponseError(c, utils.Ternary(s2sEndpoint, http.StatusBadRequest, http.StatusOK), "error processing message", false, fmt.Errorf("%v: %s", err, string(body)), true, true)
		return
	}

	domain = utils.DefaultString(loc.Slug, loc.Domain)
	c.Set(appbase.ContextDomain, domain)

	stream := r.getStream(&loc, false, s2sEndpoint)
	if stream == nil {
		rError = r.ResponseError(c, utils.Ternary(s2sEndpoint, http.StatusUnauthorized, http.StatusOK), "stream not found", false, fmt.Errorf("for: %+v", loc), true, true)
		return
	}
	s2sEndpoint = s2sEndpoint || loc.IngestType == IngestTypeS2S

	eventsLogId = stream.Stream.Id
	//if err = r.checkOrigin(c, &loc, stream); err != nil {
	//	r.Warnf("%v", err)
	//}
	err = patchClassicEvent(c, messageId, message, loc.IngestType)
	if err != nil {
		rError = r.ResponseError(c, utils.Ternary(s2sEndpoint, http.StatusBadRequest, http.StatusOK), "event error", false, err, true, true)
		return
	}
	_, ingestMessageBytes, err = r.buildIngestMessage(c, messageId, message, "classic", loc, stream)
	if err != nil {
		rError = r.ResponseError(c, utils.Ternary(s2sEndpoint, http.StatusBadRequest, http.StatusOK), "event error", false, err, true, true)
		return
	}
	if len(stream.AsynchronousDestinations) == 0 {
		rError = r.ResponseError(c, http.StatusOK, ErrNoDst, false, fmt.Errorf(stream.Stream.Id), true, false)
		return
	}
	asyncDestinations, _, rError = r.sendToRotor(c, ingestMessageBytes, stream, true)
	if rError != nil {
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
	return
}

func patchClassicEvent(c *gin.Context, messageId string, ev types.Json, ingestType IngestType) error {
	ip := strings.TrimSpace(strings.Split(utils.NvlString(c.GetHeader("X-Real-Ip"), c.GetHeader("X-Forwarded-For"), c.ClientIP()), ",")[0])
	ipPolicy := c.Query(IPPolicyParameter)
	switch ipPolicy {
	case StrictValue, ComplyValue:
		ip = ipStripLastOctet(ip)
	}

	if ingestType == IngestTypeBrowser {
		//if ip comes from browser, don't trust it!
		if ip != "" {
			ev.Set("source_ip", ip)
		}
		ev.SetIfAbsentFunc("user_agent", func() any {
			return c.GetHeader("User-Agent")
		})
		ev.SetIfAbsentFunc("user_language", func() any {
			return strings.TrimSpace(strings.Split(c.GetHeader("Accept-Language"), ",")[0])
		})
	}
	nowIsoDate := time.Now().UTC().Format(timestamp.JsonISO)
	ev.Set("_timestamp", nowIsoDate)
	ev.SetIfAbsent("utc_time", nowIsoDate)
	ev.SetIfAbsent("eventn_ctx_event_id", messageId)
	return nil
}
