package app

import (
	"context"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
	"github.com/hjson/hjson-go/v4"
	"github.com/jitsucom/bulker/app/metrics"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/objects"
	"github.com/jitsucom/bulker/base/timestamp"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/base/uuid"
	"github.com/jitsucom/bulker/bulker"
	jsoniter "github.com/json-iterator/go"
	"github.com/penglongli/gin-metrics/ginmetrics"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var TimestampPattern = regexp.MustCompile(`^\d{13}$`)

type Router struct {
	objects.ServiceBase
	engine           *gin.Engine
	config           *AppConfig
	kafkaConfig      *kafka.ConfigMap
	repository       *Repository
	topicManager     *TopicManager
	producer         *Producer
	eventsLogService EventsLogService
	fastStore        *FastStore
	authTokens       []string
	tokenSecrets     []string
	noAuthPaths      []string
}

func NewRouter(appContext *AppContext) *Router {
	base := objects.NewServiceBase("router")
	authTokens := strings.Split(appContext.config.AuthTokens, ",")
	if len(authTokens) == 1 && authTokens[0] == "" {
		authTokens = nil
		base.Warnf("⚠️ No auth tokens provided. All requests will be allowed")
	}
	tokenSecrets := strings.Split(appContext.config.TokenSecrets, ",")

	router := &Router{
		ServiceBase:      base,
		authTokens:       authTokens,
		config:           appContext.config,
		kafkaConfig:      appContext.kafkaConfig,
		repository:       appContext.repository,
		topicManager:     appContext.topicManager,
		producer:         appContext.producer,
		eventsLogService: appContext.eventsLogService,
		fastStore:        appContext.fastStore,
		tokenSecrets:     tokenSecrets,
		noAuthPaths:      []string{"/ready"},
	}
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	// get global Monitor object
	m := ginmetrics.GetMonitor()
	m.SetSlowTime(1)
	// set request duration, default {0.1, 0.3, 1.2, 5, 10}
	// used to p95, p99
	m.SetDuration([]float64{0.01, 0.05, 0.1, 0.3, 1.0, 2.0, 3.0, 10})
	m.SetMetricPath("/metrics")
	m.Use(engine)

	engine.Use(router.AuthMiddleware)
	engine.POST("/post/:destinationId", router.EventsHandler)
	engine.POST("/test", router.TestConnectionHandler)
	engine.POST("/ingest", router.IngestHandler)
	engine.GET("/failed/:destinationId", router.FailedHandler)
	engine.GET("/log/:eventType/:actorId", router.EventsLogHandler)

	engine.GET("/ready", func(c *gin.Context) {
		if router.topicManager.IsReady() {
			c.Status(http.StatusOK)
		} else {
			logging.Errorf("Health check: FAILED")
			c.AbortWithStatus(http.StatusServiceUnavailable)
		}
	})
	router.engine = engine
	return router
}

// GetEngine returns gin router
func (r *Router) GetEngine() *gin.Engine {
	return r.engine
}

func (r *Router) EventsHandler(c *gin.Context) {
	destinationId := c.Param("destinationId")
	tableName := c.Query("tableName")
	errorType := ""
	defer func() {
		if errorType != "" {
			metrics.EventsHandlerError(destinationId, tableName, errorType).Inc()
		} else {
			metrics.EventsHandlerSuccess(destinationId, tableName).Inc()
		}
	}()
	if tableName == "" {
		errorType = r.ResponseError(c, http.StatusBadRequest, "missing required parameter", false, fmt.Errorf("tableName query parameter is required"))
		return
	}
	destination := r.repository.GetDestination(destinationId)
	if destination == nil {
		errorType = r.ResponseError(c, http.StatusNotFound, "destination not found", false, fmt.Errorf("destination not found: %s", destinationId))
		return
	}
	topicId, err := destination.TopicId(tableName)
	if err != nil {
		errorType = r.ResponseError(c, http.StatusInternalServerError, "couldn't generate topicId", false, err)
		return
	}
	err = r.topicManager.EnsureDestinationTopic(destination, topicId)
	if err != nil {
		kafkaErr, ok := err.(kafka.Error)
		if ok && kafkaErr.Code() == kafka.ErrTopicAlreadyExists {
			r.Warnf("Topic %s already exists", topicId)
		} else {
			errorType = r.ResponseError(c, http.StatusInternalServerError, "couldn't create topic", false, fmt.Errorf("topicId %s: %w", topicId, err))
			return
		}
	}

	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		errorType = r.ResponseError(c, http.StatusBadRequest, "error reading HTTP body", false, err)
		return
	}
	err = r.producer.ProduceAsync(topicId, body)
	if err != nil {
		errorType = r.ResponseError(c, http.StatusInternalServerError, "producer error", true, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "ok"})
}

func (r *Router) IngestHandler(c *gin.Context) {
	slug := ""
	errorType := ""
	defer func() {
		if errorType != "" {
			metrics.IngestHandlerError(slug, errorType).Inc()
		} else {
			metrics.IngestHandlerSuccess(slug).Inc()
		}
	}()
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		errorType = r.ResponseError(c, http.StatusBadRequest, "error reading HTTP body", false, err)
		return
	}
	ingestMessage := IngestMessage{}
	err = json.Unmarshal(body, &ingestMessage)
	if err != nil {
		errorType = r.ResponseError(c, http.StatusBadRequest, "error parsing IngestMessage", false, fmt.Errorf("%w: %s", err, string(body)))
		return
	}
	r.Infof("[ingest] Message ID: %s Write key: %s Origin: %s", ingestMessage.MessageId, ingestMessage.WriteKey, utils.Nvl(ingestMessage.Origin.Slug, ingestMessage.Origin.Domain))

	var stream *StreamWithDestinations
	if ingestMessage.WriteKey != "" {
		stream, err = r.fastStore.GetStreamById(ingestMessage.WriteKey)
	} else if ingestMessage.Origin.Slug != "" {
		stream, err = r.fastStore.GetStreamById(ingestMessage.Origin.Slug)
	}
	if stream == nil && ingestMessage.Origin.Domain != "" {
		var streams []StreamWithDestinations
		streams, err = r.fastStore.GetStreamsByDomain(ingestMessage.Origin.Domain)
		if len(streams) > 1 {
			if ingestMessage.WriteKey == "" {
				errorType = r.ResponseError(c, http.StatusBadRequest, "error getting stream", false, fmt.Errorf("multiple streams found for domain %s. Please use 'writeKey' message property to select a concrete stream", ingestMessage.Origin.Domain))
				return
			}
			writeKey := ingestMessage.WriteKey
			for _, s := range streams {
				for _, k := range s.Stream.PublicKeys {
					key := strings.SplitN(k.Hash, ".", 2)
					salt := key[0]
					hash := key[1]
					for _, globalSecret := range r.config.GlobalHashSecrets {
						if hash == hashApiKey(writeKey, salt, globalSecret) {
							stream = &s
							break
						}
					}
				}
			}

		} else if len(streams) == 1 {
			stream = &streams[0]
		}
	}
	if err != nil {
		errorType = r.ResponseError(c, http.StatusInternalServerError, "error getting stream", false, err)
		return
	}
	if stream == nil {
		errorType = r.ResponseError(c, http.StatusNotFound, "stream not found", false, fmt.Errorf("stream not found by writeKey: %s and domain: %s", utils.NvlString(ingestMessage.WriteKey, ingestMessage.Origin.Slug), ingestMessage.Origin.Domain))
		return
	}
	if len(stream.AsynchronousDestinations) == 0 {
		c.JSON(http.StatusNoContent, gin.H{"message": "no destinations found for stream"})
		return
	}
	for _, destination := range stream.AsynchronousDestinations {
		messageCopy := ingestMessage
		messageCopy.ConnectionId = destination.ConnectionId
		payload, err := json.Marshal(messageCopy)
		r.Infof("[ingest] Message ID: %s Producing to: %s", messageCopy.MessageId, destination.ConnectionId)
		if err != nil {
			errorType = r.ResponseError(c, http.StatusInternalServerError, "message marshal error", false, err)
			return
		}
		err = r.producer.ProduceAsync(r.config.KafkaDestinationsTopicName, payload)
		if err != nil {
			errorType = r.ResponseError(c, http.StatusInternalServerError, "producer error", true, err)
			return
		}
	}
	if len(stream.SynchronousDestinations) == 0 {
		c.JSON(http.StatusOK, gin.H{"ok": true})
		return
	}
	tags := make(map[string]TagDestinationConfig, len(stream.SynchronousDestinations))
	for _, destination := range stream.SynchronousDestinations {
		tags[destination.Id] = destination.TagDestinationConfig
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "tags": tags})
}

func (r *Router) FailedHandler(c *gin.Context) {
	destinationId := c.Param("destinationId")
	tableName := c.Query("tableName")
	if tableName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "tableName query parameter is required"})
		return
	}
	topicId, _ := MakeTopicId(destinationId, "failed", tableName, false)
	consumerConfig := kafka.ConfigMap(utils.MapPutAll(kafka.ConfigMap{
		"auto.offset.reset":             "earliest",
		"group.id":                      uuid.New(),
		"enable.auto.commit":            false,
		"partition.assignment.strategy": r.config.KafkaConsumerPartitionsAssigmentStrategy,
		"isolation.level":               "read_committed",
	}, *r.kafkaConfig))

	consumer, err := kafka.NewConsumer(&consumerConfig)
	if err == nil {
		err = consumer.Assign([]kafka.TopicPartition{{Topic: &topicId, Partition: 0, Offset: kafka.OffsetBeginning}})
	}
	if err != nil {
		r.ResponseError(c, http.StatusInternalServerError, "consumer error", true, err)
		return
	}
	start := time.Now()
	c.Header("Content-Type", "application/x-ndjson")
	for {
		msg, err := consumer.ReadMessage(time.Second)
		json := make(map[string]any)
		if err != nil {
			kafkaErr := err.(kafka.Error)
			if kafkaErr.Code() == kafka.ErrTimedOut {
				break
			}
			errorID := uuid.NewLettersNumbers()
			err = fmt.Errorf("error# %s: couldn't read kafka message from topic: %s : %w", errorID, topicId, kafkaErr)
			r.Errorf(err.Error())
			json["ERROR"] = fmt.Errorf("error# %s: couldn't read kafka message", errorID).Error()
		} else {
			err = hjson.Unmarshal(msg.Value, &json)
			if err != nil {
				json["UNPARSABLE_MESSAGE"] = string(msg.Value)
			}
		}

		bytes, _ := jsoniter.Marshal(json)
		_, _ = c.Writer.Write(bytes)
		_, _ = c.Writer.Write([]byte("\n"))
		if msg.Timestamp.After(start) {
			break
		}
	}
	_ = consumer.Close()
}

func (r *Router) TestConnectionHandler(c *gin.Context) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		_ = r.ResponseError(c, http.StatusBadRequest, "error reading HTTP body", false, err)
		return
	}
	dstCfg := DestinationConfig{}
	err = utils.ParseObject(body, &dstCfg)
	if err != nil {
		_ = r.ResponseError(c, http.StatusUnprocessableEntity, "parse failed", false, err)
		return
	} else {
		r.Infof("[test] parsed config for destination %s: %+v", dstCfg.Id(), dstCfg)
		if !dstCfg.UsesBulker {
			_ = r.ResponseError(c, http.StatusUnprocessableEntity, "non bulker type", false, nil)
			return
		}
	}

	b, err := bulker.CreateBulker(dstCfg.Config)
	if err != nil {
		_ = r.ResponseError(c, http.StatusUnprocessableEntity, "error creating bulker", false, err)
		return
	}
	defer func() { _ = b.Close() }()
	if dstCfg.StreamConfig.BulkMode != "" || len(dstCfg.StreamConfig.Options) > 0 {
		options := bulker.StreamOptions{}
		for name, serializedOption := range dstCfg.StreamConfig.Options {
			opt, err := bulker.ParseOption(name, serializedOption)
			if err != nil {
				_ = r.ResponseError(c, http.StatusUnprocessableEntity, "option parse error", false, err)
				return
			}
			options.Add(opt)
		}
		str, err := b.CreateStream(dstCfg.Id(), dstCfg.TableName, dstCfg.BulkMode, options.Options...)
		if err != nil {
			_ = r.ResponseError(c, http.StatusUnprocessableEntity, "error creating bulker stream", false, err)
			return
		}
		_, _ = str.Abort(context.Background())
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// EventsLogHandler - gets events log by EventType, actor id. Filtered by date range and cursorId
func (r *Router) EventsLogHandler(c *gin.Context) {
	eventType := c.Param("eventType")
	actorId := c.Param("actorId")
	beforeId := c.Query("beforeId")
	start := c.Query("start")
	end := c.Query("end")
	limit := c.Query("limit")

	var err error
	eventsLogFilter := &EventsLogFilter{}
	eventsLogFilter.Start, err = parseDateQueryParam(start)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "'start' parameter must be either unix timestamp or date in '2006-01-02' format"})
		return
	}
	eventsLogFilter.End, err = parseDateQueryParam(end)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "'end' parameter must be either unix timestamp or date in '2006-01-02' format"})
		return
	}
	iLimit := 100
	if limit != "" {
		iLimit2, err := strconv.Atoi(limit)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "'limit' parameter must be an integer number"})
			return
		}
		if iLimit2 < 1000 {
			iLimit = iLimit2
		}
	}
	eventsLogFilter.BeforeId = EventsLogRecordId(beforeId)
	records, err := r.eventsLogService.GetEvents(EventType(eventType), actorId, eventsLogFilter, iLimit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get events log: " + err.Error()})
		return
	}
	c.Header("Content-Type", "application/x-ndjson")
	for _, record := range records {
		bytes, err := jsoniter.Marshal(record)
		if err != nil {
			bytes = []byte(fmt.Sprintf(`{"EVENTS_LOG_ERROR": "Failed to marshal event log record: %s", "OBJECT": "%+v"}`, err.Error(), record))
		}
		_, _ = c.Writer.Write(bytes)
		_, _ = c.Writer.Write([]byte("\n"))
	}
}

func (r *Router) AuthMiddleware(c *gin.Context) {
	if len(r.authTokens) == 0 {
		return
	}
	if utils.ArrayContains(r.noAuthPaths, c.FullPath()) {
		//no auth for this path
		return
	}
	authorizationHeader := c.GetHeader("Authorization")
	token := strings.TrimPrefix(authorizationHeader, "Bearer ")
	if token == "" {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Authorization header with Bearer token is required"})
		return
	}
	for _, authToken := range r.authTokens {
		if !strings.Contains(authToken, ".") {
			if token == authToken {
				//logging.Debugf("Token %s is valid", token)
				return
			}
		} else {
			hashedToken := strings.Split(authToken, ".")
			salt := hashedToken[0]
			hash := hashedToken[1]
			for _, secret := range r.tokenSecrets {
				//a := hashToken(token, salt, secret)
				//logging.Debugf("Hashed token: %s. Hash: %s ", a, hash)
				if hashToken(token, salt, secret) == hash {
					//logging.Debugf("Token %s is valid", token)
					return
				}
			}
		}
	}
	c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Invalid token: " + token})
	return
}

func (r *Router) ResponseError(c *gin.Context, code int, errorType string, maskError bool, err error) string {
	publicError := err
	if err != nil {
		if maskError {
			errorID := uuid.NewLettersNumbers()
			err = fmt.Errorf("error# %s: %s: %w", errorID, errorType, err)
			publicError = fmt.Errorf("error# %s: %s", errorID, errorType)
		} else {
			err = fmt.Errorf("%s: %w", errorType, err)
			publicError = err
		}
	} else {
		err = fmt.Errorf(errorType)
		publicError = err
	}
	r.Errorf("%v", err)
	c.JSON(code, gin.H{"error": publicError.Error()})
	return errorType
}

func parseDateQueryParam(param string) (time.Time, error) {
	if param != "" {
		if TimestampPattern.MatchString(param) {
			startTs, _ := strconv.Atoi(param)
			return time.UnixMilli(int64(startTs)), nil
		} else {
			return time.Parse(timestamp.DashDayLayout, param)
		}
	}
	return time.Time{}, nil
}

func hashToken(token string, salt string, secret string) string {
	//logging.Infof("Hashing token: %s. Salt: %s. Secret: %s", token, salt, secret)
	hash := sha512.New()
	hash.Write([]byte(token + salt + secret))
	return base64.RawStdEncoding.EncodeToString(hash.Sum(nil))
}

func hashApiKey(token string, salt string, secret string) string {
	hash := sha512.New()
	hash.Write([]byte(token + salt + secret))
	res := hash.Sum(nil)
	return fmt.Sprintf("%x", res)
}

type IngestMessageOrigin struct {
	BaseURL string `json:"baseUrl"`
	Slug    string `json:"slug"`
	Domain  string `json:"domain"`
}

type IngestMessage struct {
	ConnectionId   string              `json:"connectionId"`
	MessageCreated time.Time           `json:"messageCreated"`
	WriteKey       string              `json:"writeKey"`
	MessageId      string              `json:"messageId"`
	Type           string              `json:"type"`
	Origin         IngestMessageOrigin `json:"origin"`
	HttpHeaders    map[string]string   `json:"httpHeaders"`
	HttpPayload    any                 `json:"httpPayload"`
}
