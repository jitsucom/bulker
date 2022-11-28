package app

import (
	"crypto/sha512"
	"encoding/base64"
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
	authTokens       []string
	tokenSecrets     []string
	noAuthPaths      []string
}

func NewRouter(config *AppConfig, kafkaConfig *kafka.ConfigMap, repository *Repository, topicManager *TopicManager, producer *Producer, eventsLogService EventsLogService) *Router {
	base := objects.NewServiceBase("router")
	authTokens := strings.Split(config.AuthTokens, ",")
	if len(authTokens) == 1 && authTokens[0] == "" {
		authTokens = nil
		base.Warnf("⚠️ No auth tokens provided. All requests will be allowed")
	}
	tokenSecrets := strings.Split(config.TokenSecret, ",")

	router := &Router{
		ServiceBase:      base,
		authTokens:       authTokens,
		config:           config,
		kafkaConfig:      kafkaConfig,
		repository:       repository,
		topicManager:     topicManager,
		producer:         producer,
		eventsLogService: eventsLogService,
		tokenSecrets:     tokenSecrets,
		noAuthPaths:      []string{"/ready"},
	}
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
		errorType = "tableName query parameter is required"
		c.JSON(http.StatusBadRequest, gin.H{"error": errorType})
		return
	}
	destination := r.repository.GetDestination(destinationId)
	if destination == nil {
		errorType = "destination not found"
		c.JSON(http.StatusNotFound, gin.H{"error": errorType})
		return
	}
	topicId, err := destination.TopicId(tableName)
	if err != nil {
		errorType = "couldn't generate topicId"
		err = fmt.Errorf("%s: %w", errorType, err)
		r.Errorf(err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	err = r.topicManager.EnsureTopic(destination, topicId)
	if err != nil {
		//kafkaErr, ok := err.(kafka.Error)
		//if ok && kafkaErr.Code() == kafka.ErrTopicAlreadyExists {
		//	r.Warnf("Topic %s already exists", topicId)
		//} else {
		errorType = "couldn't create topic"
		err = fmt.Errorf("%s: %s : %w", errorType, topicId, err)
		r.Errorf(err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
		//}
	}

	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		errorType = "error reading HTTP body"
		r.Infof("%s: %v\n", errorType, err)
		err = fmt.Errorf("%s: %w", errorType, err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	err = r.producer.ProduceAsync(topicId, body)
	if err != nil {
		errorType = "producer error"
		errorID := uuid.NewLettersNumbers()
		err = fmt.Errorf("error# %s: couldn't produce message for kafka topic: %s : %w", errorID, topicId, err)
		r.Errorf(err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Errorf("error# %s: couldn't produce message", errorID).Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "ok"})
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
		errorID := uuid.NewLettersNumbers()
		err = fmt.Errorf("error# %s: couldn't start kafka consumer for topic: %s : %w", errorID, topicId, err)
		r.Errorf(err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Errorf("error# %s: couldn't start kafka consumer", errorID).Error()})
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
