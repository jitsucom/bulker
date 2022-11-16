package app

import (
	"crypto/sha512"
	"encoding/base64"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
	"github.com/hjson/hjson-go/v4"
	"github.com/jitsucom/bulker/base/objects"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/base/uuid"
	jsoniter "github.com/json-iterator/go"
	"io"
	"net/http"
	"strings"
	"time"
)

type Router struct {
	objects.ServiceBase
	engine       *gin.Engine
	config       *AppConfig
	kafkaConfig  *kafka.ConfigMap
	repository   *Repository
	topicManager *TopicManager
	producer     *Producer
	authTokens   []string
	tokenSecrets []string
	noAuthPaths  []string
}

func NewRouter(config *AppConfig, kafkaConfig *kafka.ConfigMap, repository *Repository, topicManager *TopicManager, producer *Producer) *Router {
	base := objects.NewServiceBase("router")
	authTokens := strings.Split(config.AuthTokens, ",")
	if len(authTokens) == 1 && authTokens[0] == "" {
		authTokens = nil
		base.Warnf("⚠️ No auth tokens provided. All requests will be allowed")
	}
	tokenSecrets := strings.Split(config.TokenSecrets, ",")

	router := &Router{
		ServiceBase:  base,
		authTokens:   authTokens,
		config:       config,
		kafkaConfig:  kafkaConfig,
		repository:   repository,
		topicManager: topicManager,
		producer:     producer,
		tokenSecrets: tokenSecrets,
		noAuthPaths:  []string{"/ready"},
	}
	engine := gin.New()
	engine.Use(router.AuthMiddleware)
	engine.POST("/post/:destinationId", router.EventsHandler)
	engine.GET("/failed/:destinationId", router.FailedHandler)
	engine.GET("/ready", func(c *gin.Context) {
		if router.topicManager.IsReady() {
			c.Status(http.StatusOK)
		} else {
			c.Status(http.StatusServiceUnavailable)
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
	if tableName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "tableName query parameter is required"})
		return
	}
	destination := r.repository.GetDestination(destinationId)
	if destination == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "destination not found"})
		return
	}
	topicId, err := destination.TopicId(tableName)
	if err != nil {
		err = fmt.Errorf("couldn't create topic: %w", err)
		r.Errorf(err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	err = r.topicManager.EnsureTopic(destination, topicId)
	if err != nil {
		err = fmt.Errorf("couldn't create topic: %s : %w", topicId, err)
		r.Errorf(err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		r.Infof("error reading HTTP body: %v\n", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("error reading HTTP body: %w", err).Error()})
		return
	}
	err = r.producer.ProduceAsync(topicId, body)
	if err != nil {
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

func hashToken(token string, salt string, secret string) string {
	//logging.Infof("Hashing token: %s. Salt: %s. Secret: %s", token, salt, secret)
	hash := sha512.New()
	hash.Write([]byte(token + salt + secret))
	return base64.RawStdEncoding.EncodeToString(hash.Sum(nil))
}
