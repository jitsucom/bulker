package main

import (
	"crypto/sha512"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gin-gonic/gin"
	"github.com/jitsucom/bulker/eventslog"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/jitsucom/bulker/jitsubase/uuid"
	"github.com/jitsucom/bulker/kafkabase"
	jsoniter "github.com/json-iterator/go"
	timeout "github.com/vearne/gin-timeout"
	"io"
	"net/http"
	"net/http/pprof"
	"strings"
	"time"
)

type Router struct {
	*appbase.Router
	config           *Config
	kafkaConfig      *kafka.ConfigMap
	repository       *Repository
	producer         *kafkabase.Producer
	eventsLogService eventslog.EventsLogService
	backupsLogger    *BackupLogger
}

func NewRouter(appContext *Context) *Router {
	authTokens := strings.Split(appContext.config.AuthTokens, ",")
	tokenSecrets := strings.Split(appContext.config.TokenSecrets, ",")
	base := appbase.NewRouterBase(authTokens, tokenSecrets, []string{"/health"})

	router := &Router{
		Router:           base,
		config:           appContext.config,
		kafkaConfig:      appContext.kafkaConfig,
		producer:         appContext.producer,
		eventsLogService: appContext.eventsLogService,
		backupsLogger:    appContext.backupsLogger,
		repository:       appContext.repository,
	}
	engine := router.Engine()
	fast := engine.Group("")
	fast.Use(timeout.Timeout(timeout.WithTimeout(10 * time.Second)))
	fast.POST("/ingest", router.IngestHandler)

	engine.GET("/health", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	engine.GET("/debug/pprof/profile", gin.WrapF(pprof.Profile))
	engine.GET("/debug/pprof/heap", gin.WrapF(pprof.Handler("heap").ServeHTTP))
	engine.GET("/debug/pprof/goroutine", gin.WrapF(pprof.Handler("goroutine").ServeHTTP))
	engine.GET("/debug/pprof/block", gin.WrapF(pprof.Handler("block").ServeHTTP))
	engine.GET("/debug/pprof/threadcreate", gin.WrapF(pprof.Handler("threadcreate").ServeHTTP))
	engine.GET("/debug/pprof/cmdline", gin.WrapF(pprof.Handler("cmdline").ServeHTTP))
	engine.GET("/debug/pprof/symbol", gin.WrapF(pprof.Handler("symbol").ServeHTTP))
	engine.GET("/debug/pprof/trace", gin.WrapF(pprof.Handler("trace").ServeHTTP))
	engine.GET("/debug/pprof/mutex", gin.WrapF(pprof.Handler("mutex").ServeHTTP))
	engine.GET("/debug/pprof", gin.WrapF(pprof.Index))

	return router
}

func (r *Router) IngestHandler(c *gin.Context) {
	domain := ""
	// TODO: use workspaceId as default for all stream identification errors
	var eventsLogId string
	var rError *appbase.RouterError
	var body []byte
	var asyncDestinations []string
	var tagsDestinations []string

	defer func() {
		if len(body) > 0 {
			_ = r.backupsLogger.Log(utils.DefaultString(eventsLogId, "UNKNOWN"), body)
		}
		if rError != nil {
			obj := map[string]any{"body": string(body), "error": rError.PublicError.Error(), "status": "FAILED"}
			r.eventsLogService.PostAsync(&eventslog.ActorEvent{eventslog.EventTypeIncomingError, eventsLogId, obj})
			r.eventsLogService.PostAsync(&eventslog.ActorEvent{eventslog.EventTypeIncomingAll, eventsLogId, obj})
			IngestHandlerRequests(domain, "error", rError.ErrorType).Inc()
			_ = r.producer.ProduceAsync(r.config.KafkaDestinationsDeadLetterTopicName, uuid.New(), body, map[string]string{"error": rError.Error.Error()})
		} else {
			obj := map[string]any{"body": string(body), "asyncDestinations": asyncDestinations, "tags": tagsDestinations}
			if len(asyncDestinations) > 0 || len(tagsDestinations) > 0 {
				obj["status"] = "SUCCESS"
			} else {
				obj["status"] = "SKIPPED"
				obj["error"] = "no destinations found for stream"
			}
			r.eventsLogService.PostAsync(&eventslog.ActorEvent{eventslog.EventTypeIncomingAll, eventsLogId, obj})
			IngestHandlerRequests(domain, "success", "").Inc()
		}
	}()
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		rError = r.ResponseError(c, http.StatusBadRequest, "error reading HTTP body", false, err, "")
		return
	}
	ingestMessage := IngestMessage{}
	err = jsoniter.Unmarshal(body, &ingestMessage)
	if err != nil {
		rError = r.ResponseError(c, http.StatusBadRequest, "error parsing IngestMessage", false, fmt.Errorf("%v: %s", err, string(body)), "")
		return
	}
	messageId := ingestMessage.MessageId
	domain = utils.DefaultString(ingestMessage.Origin.Slug, ingestMessage.Origin.Domain)
	r.Debugf("[ingest] Message ID: %s Domain: %s", messageId, domain)
	logFormat := "[ingest] Message ID: %s Domain: %s"

	stream := r.getStream(ingestMessage)
	if stream == nil {
		rError = r.ResponseError(c, http.StatusBadRequest, "stream not found", false, nil, logFormat, messageId, domain)
		return
	}
	eventsLogId = stream.Stream.Id
	if len(stream.AsynchronousDestinations) == 0 && len(stream.SynchronousDestinations) == 0 {
		c.JSON(http.StatusOK, gin.H{"message": "no destinations found for stream"})
		return
	}
	asyncDestinations = utils.ArrayMap(stream.AsynchronousDestinations, func(d *ShortDestinationConfig) string { return d.ConnectionId })
	tagsDestinations = utils.ArrayMap(stream.SynchronousDestinations, func(d *ShortDestinationConfig) string { return d.ConnectionId })

	r.Infof("[ingest] Message ID: %s Domain: %s to Connections: [%s] Tags: [%s]", messageId, domain,
		strings.Join(asyncDestinations, ", "), strings.Join(tagsDestinations, ", "))
	for _, destination := range stream.AsynchronousDestinations {
		messageCopy := ingestMessage
		messageCopy.ConnectionId = destination.ConnectionId
		//multithreading, ok := destination.Options["multithreading"].(bool)
		topic := r.config.KafkaDestinationsTopicName
		messageKey := uuid.New()
		payload, err := json.Marshal(messageCopy)
		r.Debugf("[ingest] Message ID: %s Producing for: %s topic: %s key: %s", messageId, destination.ConnectionId, topic, messageKey)
		if err != nil {
			IngestedMessages(destination.ConnectionId, "error", "message marshal error").Inc()
			rError = r.ResponseError(c, http.StatusBadRequest, "message marshal error", false, err, logFormat, messageId, domain)
			continue
		}
		err = r.producer.ProduceAsync(topic, messageKey, payload, nil)
		if err != nil {
			IngestedMessages(destination.ConnectionId, "error", "producer error").Inc()
			rError = r.ResponseError(c, http.StatusInternalServerError, "producer error", true, err, logFormat, messageId, domain)
			continue
		}
		IngestedMessages(destination.ConnectionId, "success", "").Inc()
	}
	if len(stream.SynchronousDestinations) == 0 {
		c.JSON(http.StatusOK, gin.H{"ok": true})
		return
	}

	tags := make(map[string]TagDestinationConfig, len(stream.SynchronousDestinations))
	for _, destination := range stream.SynchronousDestinations {
		tags[destination.Id] = destination.TagDestinationConfig
		IngestedMessages(destination.ConnectionId, "success", "").Inc()
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "tags": tags})
}

func hashApiKey(token string, salt string, secret string) string {
	hash := sha512.New()
	hash.Write([]byte(token + salt + secret))
	res := hash.Sum(nil)
	return fmt.Sprintf("%x", res)
}

func (r *Router) checkHash(hash string, secret string) bool {
	pk := strings.SplitN(hash, ".", 2)
	salt := pk[0]
	hashPart := pk[1]
	for _, globalSecret := range r.config.GlobalHashSecrets {
		if hashPart == hashApiKey(secret, salt, globalSecret) {
			return true
		}
	}
	return false
}

type IngestMessageOrigin struct {
	BaseURL string `json:"baseUrl"`
	Slug    string `json:"slug"`
	Domain  string `json:"domain"`
}

type IngestMessage struct {
	IngestType     string              `json:"ingestType"`
	ConnectionId   string              `json:"connectionId"`
	MessageCreated time.Time           `json:"messageCreated"`
	WriteKey       string              `json:"writeKey"`
	MessageId      string              `json:"messageId"`
	Type           string              `json:"type"`
	Origin         IngestMessageOrigin `json:"origin"`
	HttpHeaders    map[string]string   `json:"httpHeaders"`
	HttpPayload    map[string]any      `json:"httpPayload"`
	Geo            map[string]any      `json:"geo"`
}

type StreamCredentials struct {
	Slug       string `json:"slug"`
	Domain     string `json:"domain"`
	WriteKey   string `json:"writeKey"`
	IngestType string `json:"ingestType"`
}

type StreamLocator func(message IngestMessage) *StreamWithDestinations

func (r *Router) getStream(ingestMessage IngestMessage) *StreamWithDestinations {
	var locators []StreamLocator
	if ingestMessage.IngestType == "s2s" {
		locators = []StreamLocator{r.WriteKeyStreamLocator, r.SlugStreamLocator, r.AmbiguousDomainStreamLocator}
	} else {
		locators = []StreamLocator{r.SlugStreamLocator, r.DomainStreamLocator, r.WriteKeyStreamLocator}
	}
	for _, locator := range locators {
		stream := locator(ingestMessage)
		if stream != nil {
			return stream
		}
	}
	return nil
}

func (r *Router) WriteKeyStreamLocator(ingestMessage IngestMessage) *StreamWithDestinations {
	if ingestMessage.WriteKey != "" {
		parts := strings.Split(ingestMessage.WriteKey, ":")
		if len(parts) == 1 {
			stream, err := r.repository.GetStreamById(ingestMessage.WriteKey)
			if err != nil {
				r.Errorf("error getting stream: %v", err)
			} else {
				return stream
			}
		} else {
			var binding *ApiKeyBinding
			binding, err := r.repository.getStreamByKeyId(parts[0])
			if err == nil && binding != nil {
				if binding.KeyType != ingestMessage.IngestType {
					r.Errorf("invalid key type: found %s, expected %s", binding.KeyType, ingestMessage.IngestType)
				} else if !r.checkHash(binding.Hash, parts[1]) {
					r.Errorf("invalid key secret")
				} else {
					stream, err := r.repository.GetStreamById(binding.StreamId)
					if err != nil {
						r.Errorf("error getting stream: %v", err)
					} else {
						return stream
					}
				}
			}
		}
	}
	return nil
}

func (r *Router) SlugStreamLocator(ingestMessage IngestMessage) *StreamWithDestinations {
	if ingestMessage.Origin.Slug != "" {
		stream, err := r.repository.GetStreamById(ingestMessage.Origin.Slug)
		if err != nil {
			r.Errorf("error getting stream: %v", err)
		} else {
			return stream
		}
	}
	return nil
}

func (r *Router) DomainStreamLocator(ingestMessage IngestMessage) *StreamWithDestinations {
	if ingestMessage.Origin.Domain != "" {
		streams, err := r.repository.GetStreamsByDomain(ingestMessage.Origin.Domain)
		if err != nil {
			r.Errorf("error getting stream: %v", err)
		} else if len(streams) == 1 {
			return streams[0]
		} else if ingestMessage.WriteKey == "" && len(streams) > 1 {
			return streams[0]
		}
	}
	return nil
}

func (r *Router) AmbiguousDomainStreamLocator(ingestMessage IngestMessage) *StreamWithDestinations {
	if ingestMessage.Origin.Domain != "" {
		streams, err := r.repository.GetStreamsByDomain(ingestMessage.Origin.Domain)
		if err != nil {
			r.Errorf("error getting stream: %v", err)
		} else if len(streams) > 0 {
			return streams[0]
		}
	}
	return nil
}
