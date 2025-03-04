package main

import (
	"compress/gzip"
	"fmt"
	kafka2 "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gin-gonic/gin"
	"github.com/jitsucom/bulker/eventslog"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/jsoniter"
	"github.com/jitsucom/bulker/jitsubase/jsonorder"
	"github.com/jitsucom/bulker/jitsubase/types"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/jitsucom/bulker/jitsubase/uuid"
	"io"
	"net/http"
	"strings"
)

//var ids = sync.Map{}
//
//func init() {
//	ticker := time.NewTicker(1 * time.Minute)
//	go func() {
//		for range ticker.C {
//			arr := make([]string, 0)
//			ids.Range(func(key, value interface{}) bool {
//				arr = append(arr, key.(string))
//				return true
//			})
//			logging.Infof("[S2S] %s", strings.Join(arr, ","))
//		}
//	}()
//}

func (r *Router) IngestHandler(c *gin.Context) {
	domain := ""
	// TODO: use workspaceId as default for all stream identification errors
	var eventsLogId string
	var rError *appbase.RouterError
	var body []byte
	var ingestMessageBytes []byte
	var asyncDestinations []string
	var tagsDestinations []string
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
			obj := map[string]any{"body": string(ingestMessageBytes), "asyncDestinations": asyncDestinations, "tags": tagsDestinations}
			if len(asyncDestinations) > 0 || len(tagsDestinations) > 0 {
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
	if c.FullPath() == "/api/s/s2s/:tp" {
		// may still be overridden by write key type
		s2sEndpoint = true
	}
	tp := c.Param("tp")
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
	messageId := message.GetS("messageId")
	if messageId == "" {
		messageId = uuid.New()
	} else {
		messageId = utils.ShortenString(messageIdUnsupportedChars.ReplaceAllString(messageId, "_"), 64)
	}
	c.Set(appbase.ContextMessageId, messageId)
	//loc, err := r.getDataLocator(c, ingestType, func() string { return message.GetS("writeKey") })
	loc, err := r.getDataLocator(c, ingestType, nil)
	if err != nil {
		rError = r.ResponseError(c, utils.Ternary(s2sEndpoint, http.StatusBadRequest, http.StatusOK), "error processing message", false, fmt.Errorf("%v: %s", err, string(body)), true, true)
		return
	}

	domain = utils.DefaultString(loc.Slug, loc.Domain)
	c.Set(appbase.ContextDomain, domain)

	stream := r.getStream(&loc, false, s2sEndpoint)
	if stream == nil {
		rError = r.ResponseError(c, utils.Ternary(s2sEndpoint, http.StatusUnauthorized, http.StatusOK), "stream not found", false, fmt.Errorf("for: %s", loc.String()), true, true)
		return
	}
	s2sEndpoint = s2sEndpoint || loc.IngestType == IngestTypeS2S

	eventsLogId = stream.Stream.Id
	//if err = r.checkOrigin(c, &loc, stream); err != nil {
	//	r.Warnf("%v", err)
	//}
	ingestMessage, ingestMessageBytes, err := r.buildIngestMessage(c, messageId, message, nil, tp, loc, stream, patchEvent, "")
	if err != nil {
		rError = r.ResponseError(c, utils.Ternary(s2sEndpoint, http.StatusBadRequest, http.StatusOK), "event error", false, err, true, true)
		return
	}
	if len(stream.AsynchronousDestinations) == 0 && (len(stream.SynchronousDestinations) == 0 || s2sEndpoint) {
		rError = r.ResponseError(c, http.StatusOK, ErrNoDst, false, fmt.Errorf(stream.Stream.Id), true, false)
		return
	}
	asyncDestinations, tagsDestinations, rError = r.sendToRotor(c, ingestMessageBytes, stream, true)
	if rError != nil {
		return
	}
	if len(tagsDestinations) == 0 || s2sEndpoint {
		c.JSON(http.StatusOK, gin.H{"ok": true})
		return
	}
	resp := r.processSyncDestination(ingestMessage, stream, ingestMessageBytes)
	if resp != nil {
		if r.ShouldCompress(c.Request) {
			c.Header("Content-Encoding", "gzip")
			c.Header("Content-Type", "application/json")
			c.Header("Vary", "Accept-Encoding")
			gz := gzip.NewWriter(c.Writer)
			_ = jsoniter.NewEncoder(gz).Encode(resp)
			_ = gz.Close()
		} else {
			c.JSON(http.StatusOK, resp)
		}
	} else {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	}
}
