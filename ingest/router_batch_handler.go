package main

import (
	"compress/gzip"
	"fmt"
	kafka2 "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gin-gonic/gin"
	"github.com/jitsucom/bulker/eventslog"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/jsonorder"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/jitsucom/bulker/jitsubase/uuid"
	"net/http"
	"strings"
)

func (r *Router) BatchHandler(c *gin.Context) {
	var rError *appbase.RouterError
	var payload BatchPayload
	domain := "BATCH"
	defer func() {
		if rError != nil {
			IngestHandlerRequests(domain, "error", rError.ErrorType).Inc()
		}
	}()
	defer func() {
		if rerr := recover(); rerr != nil {
			rError = r.ResponseError(c, http.StatusInternalServerError, "panic", true, fmt.Errorf("%v", rerr), true)
		}
	}()
	c.Set(appbase.ContextLoggerName, "batch")
	if !strings.HasSuffix(c.ContentType(), "application/json") && !strings.HasSuffix(c.ContentType(), "text/plain") {
		rError = r.ResponseError(c, http.StatusBadRequest, "invalid content type", false, fmt.Errorf("%s. Expected: application/json", c.ContentType()), true)
		return
	}
	bodyReader := c.Request.Body
	var err error
	if strings.Contains(c.GetHeader("Content-Encoding"), "gzip") {
		bodyReader, err = gzip.NewReader(bodyReader)
	}
	if err == nil {
		err = jsonorder.NewDecoder(bodyReader).Decode(&payload)
	}
	if err != nil {
		err = fmt.Errorf("Client Ip: %s: %v", utils.NvlString(c.GetHeader("X-Real-Ip"), c.GetHeader("X-Forwarded-For"), c.ClientIP()), err)
		rError = r.ResponseError(c, http.StatusOK, "error parsing message", false, err, true)
		return
	}
	var ingestType IngestType
	if c.FullPath() == "/api/s/s2s/batch" {
		ingestType = IngestTypeS2S
	} else {
		ingestType = IngestTypeWriteKeyDefined
	}
	loc, err := r.getDataLocator(c, ingestType, func() string { return payload.WriteKey })
	if err != nil {
		rError = r.ResponseError(c, http.StatusOK, "error processing message", false, err, true)
		return
	}
	domain = utils.DefaultString(loc.Slug, loc.Domain)
	c.Set(appbase.ContextDomain, domain)

	stream := r.getStream(&loc)
	if stream == nil {
		rError = r.ResponseError(c, http.StatusOK, "stream not found", false, fmt.Errorf("for: %+v", loc), true)
		return
	}
	eventsLogId := stream.Stream.Id
	okEvents := 0
	errors := make([]string, 0)
	for _, event := range payload.Batch {
		messageId := event.GetS("messageId")
		if messageId == "" {
			messageId = uuid.New()
		} else {
			messageId = utils.ShortenString(messageIdUnsupportedChars.ReplaceAllString(messageId, "_"), 64)
		}
		c.Set(appbase.ContextMessageId, messageId)
		_, ingestMessageBytes, err1 := r.buildIngestMessage(c, messageId, event, payload.Context, "event", loc, stream)
		var asyncDestinations, tagsDestinations []string
		if err1 == nil {
			if len(stream.AsynchronousDestinations) == 0 {
				rError = r.ResponseError(c, http.StatusOK, ErrNoDst, false, fmt.Errorf(stream.Stream.Id), false)
			} else {
				asyncDestinations, tagsDestinations, rError = r.sendToRotor(c, ingestMessageBytes, stream, false)
			}
		} else {
			rError = r.ResponseError(c, http.StatusOK, "event error", false, err1, false)
		}
		if len(ingestMessageBytes) >= 0 {
			_ = r.backupsLogger.Log(utils.DefaultString(eventsLogId, "UNKNOWN"), ingestMessageBytes)
		}
		if rError != nil && rError.ErrorType != ErrNoDst {
			obj := map[string]any{"body": string(ingestMessageBytes), "error": rError.PublicError.Error(), "status": utils.Ternary(rError.ErrorType == ErrThrottledType, "SKIPPED", "FAILED")}
			r.eventsLogService.PostAsync(&eventslog.ActorEvent{EventType: eventslog.EventTypeIncoming, Level: eventslog.LevelError, ActorId: eventsLogId, Event: obj})
			IngestHandlerRequests(domain, utils.Ternary(rError.ErrorType == ErrThrottledType, "throttled", "error"), rError.ErrorType).Inc()
			_ = r.producer.ProduceAsync(r.config.KafkaDestinationsDeadLetterTopicName, uuid.New(), ingestMessageBytes, map[string]string{"error": rError.Error.Error()}, kafka2.PartitionAny)
			errors = append(errors, fmt.Sprintf("Message ID: %s: %v", messageId, rError.PublicError))
		} else {
			obj := map[string]any{"body": string(ingestMessageBytes), "asyncDestinations": asyncDestinations, "tags": tagsDestinations}
			if len(asyncDestinations) > 0 || len(tagsDestinations) > 0 {
				okEvents++
				obj["status"] = "SUCCESS"
			} else {
				obj["status"] = "SKIPPED"
				obj["error"] = "no destinations found for stream"
				errors = append(errors, fmt.Sprintf("Message ID: %s: %v", messageId, rError.PublicError))
			}
			r.eventsLogService.PostAsync(&eventslog.ActorEvent{EventType: eventslog.EventTypeIncoming, Level: eventslog.LevelInfo, ActorId: eventsLogId, Event: obj})
			IngestHandlerRequests(domain, "success", "").Inc()
		}
	}
	batchSize := len(payload.Batch)
	if batchSize == okEvents {
		c.JSON(http.StatusOK, gin.H{"ok": true, "receivedEvents": batchSize, "okEvents": okEvents})
	} else {
		c.JSON(http.StatusOK, gin.H{"ok": false, "errors": errors, "receivedEvents": batchSize, "okEvents": okEvents})
	}
}
