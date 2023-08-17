package app

import (
	"bufio"
	"bytes"
	"crypto/sha512"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gin-gonic/gin"
	"github.com/hjson/hjson-go/v4"
	"github.com/jitsucom/bulker/bulkerapp/metrics"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/jitsucom/bulker/jitsubase/timestamp"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/jitsucom/bulker/jitsubase/uuid"
	jsoniter "github.com/json-iterator/go"
	timeout "github.com/vearne/gin-timeout"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var TimestampPattern = regexp.MustCompile(`^\d{13}$`)

type Router struct {
	*appbase.Router
	config           *Config
	kafkaConfig      *kafka.ConfigMap
	repository       *Repository
	topicManager     *TopicManager
	producer         *Producer
	eventsLogService EventsLogService
	fastStore        *FastStore
}

func NewRouter(appContext *Context) *Router {
	authTokens := strings.Split(appContext.config.AuthTokens, ",")
	tokenSecrets := strings.Split(appContext.config.TokenSecrets, ",")
	base := appbase.NewRouterBase(authTokens, tokenSecrets, []string{"/ready"})

	router := &Router{
		Router:           base,
		config:           appContext.config,
		kafkaConfig:      appContext.kafkaConfig,
		repository:       appContext.repository,
		topicManager:     appContext.topicManager,
		producer:         appContext.producer,
		eventsLogService: appContext.eventsLogService,
		fastStore:        appContext.fastStore,
	}
	engine := router.Engine()
	fast := engine.Group("")
	fast.Use(timeout.Timeout(timeout.WithTimeout(10 * time.Second)))
	fast.POST("/post/:destinationId", router.EventsHandler)
	fast.POST("/ingest", router.IngestHandler)
	fast.POST("/test", router.TestConnectionHandler)
	fast.GET("/log/:eventType/:actorId", router.EventsLogHandler)
	fast.GET("/ready", func(c *gin.Context) {
		if router.topicManager.IsReady() {
			c.Status(http.StatusOK)
		} else {
			logging.Errorf("Health check: FAILED")
			c.AbortWithStatus(http.StatusServiceUnavailable)
		}
	})

	engine.POST("/bulk/:destinationId", router.BulkHandler)
	engine.GET("/failed/:destinationId", router.FailedHandler)

	//engine.GET("/debug/pprof/profile", gin.WrapF(pprof.Profile))
	//engine.GET("/debug/pprof/heap", gin.WrapF(pprof.Handler("heap").ServeHTTP))
	//engine.GET("/debug/pprof/goroutine", gin.WrapF(pprof.Handler("goroutine").ServeHTTP))
	//engine.GET("/debug/pprof/block", gin.WrapF(pprof.Handler("block").ServeHTTP))
	//engine.GET("/debug/pprof/threadcreate", gin.WrapF(pprof.Handler("threadcreate").ServeHTTP))
	//engine.GET("/debug/pprof/cmdline", gin.WrapF(pprof.Handler("cmdline").ServeHTTP))
	//engine.GET("/debug/pprof/symbol", gin.WrapF(pprof.Handler("symbol").ServeHTTP))
	//engine.GET("/debug/pprof/trace", gin.WrapF(pprof.Handler("trace").ServeHTTP))
	//engine.GET("/debug/pprof/mutex", gin.WrapF(pprof.Handler("mutex").ServeHTTP))
	//engine.GET("/debug/pprof", gin.WrapF(pprof.Index))

	return router
}

func (r *Router) EventsHandler(c *gin.Context) {
	destinationId := c.Param("destinationId")
	tableName := c.Query("tableName")
	mode := ""
	bytesRead := 0
	var rError appbase.RouterError
	defer func() {
		if rError.Error != nil {
			metrics.EventsHandlerRequests(destinationId, mode, tableName, "error", rError.ErrorType).Inc()
		} else {
			metrics.EventsHandlerRequests(destinationId, mode, tableName, "success", "").Inc()
			metrics.EventsHandlerBytes(destinationId, mode, tableName, "success", "").Add(float64(bytesRead))
		}
	}()

	destination := r.repository.GetDestination(destinationId)
	if destination == nil {
		rError = r.ResponseError(c, http.StatusNotFound, "destination not found", false, fmt.Errorf("destination not found: %s", destinationId), "")
		return
	}
	mode = string(destination.Mode())
	if tableName == "" {
		rError = r.ResponseError(c, http.StatusBadRequest, "missing required parameter", false, fmt.Errorf("tableName query parameter is required"), "")
		return
	}
	topicId, err := destination.TopicId(tableName)
	if err != nil {
		rError = r.ResponseError(c, http.StatusInternalServerError, "couldn't generate topicId", false, err, "")
		return
	}
	err = r.topicManager.EnsureDestinationTopic(destination, topicId)
	if err != nil {
		kafkaErr, ok := err.(kafka.Error)
		if ok && kafkaErr.Code() == kafka.ErrTopicAlreadyExists {
			r.Warnf("Topic %s already exists", topicId)
		} else {
			rError = r.ResponseError(c, http.StatusInternalServerError, "couldn't create topic", false, fmt.Errorf("topicId %s: %v", topicId, err), "")
			return
		}
	}

	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		rError = r.ResponseError(c, http.StatusBadRequest, "error reading HTTP body", false, err, "")
		return
	}
	bytesRead = len(body)
	err = r.producer.ProduceAsync(topicId, uuid.New(), body)
	if err != nil {
		rError = r.ResponseError(c, http.StatusInternalServerError, "producer error", true, err, "")
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "ok"})
}

func (r *Router) BulkHandler(c *gin.Context) {
	destinationId := c.Param("destinationId")
	tableName := c.Query("tableName")
	taskId := c.DefaultQuery("taskId", uuid.New())
	jobId := c.DefaultQuery("jobId", fmt.Sprintf("%s_%s_%s", destinationId, tableName, taskId))
	bulkMode := bulker.BulkMode(c.DefaultQuery("mode", string(bulker.ReplaceTable)))
	pkeys := c.QueryArray("pk")

	mode := ""
	bytesRead := 0
	var rError appbase.RouterError
	defer func() {
		if rError.Error != nil {
			metrics.BulkHandlerRequests(destinationId, mode, tableName, "error", rError.ErrorType).Inc()
		} else {
			metrics.BulkHandlerRequests(destinationId, mode, tableName, "success", "").Inc()
			metrics.EventsHandlerBytes(destinationId, mode, tableName, "success", "").Add(float64(bytesRead))
		}
	}()

	destination := r.repository.GetDestination(destinationId)
	if destination == nil {
		rError = r.ResponseError(c, http.StatusNotFound, "destination not found", false, fmt.Errorf("destination not found: %s", destinationId), "")
		return
	}
	mode = string(destination.Mode())
	if tableName == "" {
		rError = r.ResponseError(c, http.StatusBadRequest, "missing required parameter", false, fmt.Errorf("tableName query parameter is required"), "")
		return
	}
	var streamOptions []bulker.StreamOption
	if len(pkeys) > 0 {
		streamOptions = append(streamOptions, bulker.WithPrimaryKey(pkeys...), bulker.WithMergeRows())
	}
	bulkerStream, err := destination.bulker.CreateStream(jobId, tableName, bulkMode, streamOptions...)
	if err != nil {
		rError = r.ResponseError(c, http.StatusInternalServerError, "create stream error", true, err, "")
		return
	}
	scanner := bufio.NewScanner(c.Request.Body)
	scanner.Buffer(make([]byte, 1024*100), 1024*1024*10)
	consumed := 0
	for scanner.Scan() {
		eventBytes := scanner.Bytes()
		if len(eventBytes) >= 5 && string(eventBytes[:5]) == "ABORT" {
			_, _ = bulkerStream.Abort(c)
			rError = r.ResponseError(c, http.StatusBadRequest, "aborted", false, fmt.Errorf(string(eventBytes)), "")
			return
		}
		bytesRead += len(eventBytes)
		obj := types.Object{}
		dec := jsoniter.NewDecoder(bytes.NewReader(eventBytes))
		dec.UseNumber()
		if err = dec.Decode(&obj); err != nil {
			_, _ = bulkerStream.Abort(c)
			rError = r.ResponseError(c, http.StatusBadRequest, "unmarhsal error", false, err, "")
			return
		}
		if _, _, err = bulkerStream.Consume(c, obj); err != nil {
			_, _ = bulkerStream.Abort(c)
			rError = r.ResponseError(c, http.StatusBadRequest, "stream consume error", false, err, "")
			return
		}
		consumed++
	}
	if err = scanner.Err(); err != nil {
		_, _ = bulkerStream.Abort(c)
		rError = r.ResponseError(c, http.StatusBadRequest, "scanner error", false, err, "")
		return
	}
	if consumed > 0 {
		state, err := bulkerStream.Complete(c)
		if err != nil {
			rError = r.ResponseError(c, http.StatusBadRequest, "stream complete error", false, err, "")
			return
		}
		c.JSON(http.StatusOK, gin.H{"message": "ok", "state": state})
	} else {
		_, _ = bulkerStream.Abort(c)
		c.JSON(http.StatusOK, gin.H{"message": "ok"})
	}
}

func maskWriteKey(wk string) string {
	arr := strings.Split(wk, ":")
	if len(arr) > 1 {
		return arr[0] + ":***"
	} else {
		return "***"
	}
}

func (r *Router) IngestHandler(c *gin.Context) {
	domain := ""
	// TODO: use workspaceId as default for all stream identification errors
	var eventsLogId string
	var rError appbase.RouterError
	var body []byte
	var asyncDestinations []string
	var tagsDestinations []string

	defer func() {
		bodyJsonObj := map[string]any{}
		err := json.Unmarshal(body, &bodyJsonObj)
		if err == nil {
			wk, ok := bodyJsonObj["writeKey"]
			if ok {
				bodyJsonObj["writeKey"] = maskWriteKey(fmt.Sprint(wk))
			}
			body, _ = json.Marshal(bodyJsonObj)
		}
		if rError.Error != nil {
			obj := map[string]any{"body": string(body), "error": rError.PublicError.Error(), "status": "FAILED"}
			r.eventsLogService.PostAsync(&ActorEvent{EventTypeIncomingError, eventsLogId, obj})
			r.eventsLogService.PostAsync(&ActorEvent{EventTypeIncomingAll, eventsLogId, obj})
			metrics.IngestHandlerRequests(domain, "error", rError.ErrorType).Inc()
		} else {
			obj := map[string]any{"body": string(body), "asyncDestinations": asyncDestinations, "tags": tagsDestinations}
			if len(asyncDestinations) > 0 || len(tagsDestinations) > 0 {
				obj["status"] = "SUCCESS"
			} else {
				obj["status"] = "SKIPPED"
				obj["error"] = "no destinations found for stream"
			}
			r.eventsLogService.PostAsync(&ActorEvent{EventTypeIncomingAll, eventsLogId, obj})
			metrics.IngestHandlerRequests(domain, "success", "").Inc()
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
	domain = utils.NvlString(ingestMessage.Origin.Slug, ingestMessage.Origin.Domain)
	r.Debugf("[ingest] Message ID: %s Domain: %s", messageId, domain)
	logFormat := "[ingest] Message ID: %s Domain: %s"

	var stream *StreamWithDestinations
	if ingestMessage.WriteKey != "" {
		parts := strings.Split(ingestMessage.WriteKey, ":")
		if len(parts) == 1 {
			stream, err = r.fastStore.GetStreamById(ingestMessage.WriteKey)
		} else {
			var binding *ApiKeyBinding
			binding, err = r.fastStore.getStreamByKeyId(parts[0])
			if err == nil && binding != nil {
				if binding.KeyType != ingestMessage.IngestType {
					err = fmt.Errorf("invalid key type: found %s, expected %s", binding.KeyType, ingestMessage.IngestType)
				} else if !r.checkHash(binding.Hash, parts[1]) {
					err = fmt.Errorf("invalid key secret")
				} else {
					stream, err = r.fastStore.GetStreamById(binding.StreamId)
				}
			}
		}
	} else if ingestMessage.Origin.Slug != "" {
		eventsLogId = ingestMessage.Origin.Slug
		stream, err = r.fastStore.GetStreamById(ingestMessage.Origin.Slug)
	} else if ingestMessage.Origin.Domain != "" {
		var streams []StreamWithDestinations
		streams, err = r.fastStore.GetStreamsByDomain(ingestMessage.Origin.Domain)
		if len(streams) > 1 {
			rError = r.ResponseError(c, http.StatusBadRequest, "error getting stream", false, fmt.Errorf("multiple streams found for domain %s. Please use 'writeKey' message property to select a concrete stream", ingestMessage.Origin.Domain), logFormat, messageId, domain)
			return
		} else if len(streams) == 1 {
			stream = &streams[0]
		}
	}
	if err != nil {
		rError = r.ResponseError(c, http.StatusInternalServerError, "error getting stream", false, err, logFormat, messageId, domain)
		return
	}
	if stream == nil {
		rError = r.ResponseError(c, http.StatusNotFound, "stream not found", false, nil, logFormat, messageId, domain)
		return
	}
	eventsLogId = stream.Stream.Id
	if len(stream.AsynchronousDestinations) == 0 && len(stream.SynchronousDestinations) == 0 {
		c.JSON(http.StatusOK, gin.H{"message": "no destinations found for stream"})
		return
	}
	asyncDestinations = utils.ArrayMap(stream.AsynchronousDestinations, func(d ShortDestinationConfig) string { return d.ConnectionId })
	tagsDestinations = utils.ArrayMap(stream.SynchronousDestinations, func(d ShortDestinationConfig) string { return d.ConnectionId })

	r.Debugf("[ingest] Message ID: %s Domain: %s to Connections: [%s] Tags: [%s]", messageId, domain,
		strings.Join(asyncDestinations, ", "), strings.Join(tagsDestinations, ", "))
	for _, destination := range stream.AsynchronousDestinations {
		messageCopy := ingestMessage
		messageCopy.ConnectionId = destination.ConnectionId
		multithreading, ok := destination.Options["multithreading"].(bool)
		topic := r.config.KafkaDestinationsTopicName
		messageKey := messageCopy.ConnectionId
		if ok && multithreading {
			topic = r.config.KafkaDestinationsTopicMultiThreadedName
			messageKey = ""
		}
		payload, err := json.Marshal(messageCopy)
		r.Debugf("[ingest] Message ID: %s Producing for: %s topic: %s key: %s", messageId, destination.ConnectionId, topic, messageKey)
		if err != nil {
			metrics.IngestedMessages(destination.ConnectionId, "error", "message marshal error").Inc()
			rError = r.ResponseError(c, http.StatusInternalServerError, "message marshal error", false, err, logFormat, messageId, domain)
			continue
		}
		err = r.producer.ProduceAsync(topic, messageKey, payload)
		if err != nil {
			metrics.IngestedMessages(destination.ConnectionId, "error", "producer error").Inc()
			rError = r.ResponseError(c, http.StatusInternalServerError, "producer error", true, err, logFormat, messageId, domain)
			continue
		}
		metrics.IngestedMessages(destination.ConnectionId, "success", "").Inc()
	}
	if len(stream.SynchronousDestinations) == 0 {
		c.JSON(http.StatusOK, gin.H{"ok": true})
		return
	}

	tags := make(map[string]TagDestinationConfig, len(stream.SynchronousDestinations))
	for _, destination := range stream.SynchronousDestinations {
		tags[destination.Id] = destination.TagDestinationConfig
		metrics.IngestedMessages(destination.ConnectionId, "success", "").Inc()
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "tags": tags})
}

func (r *Router) FailedHandler(c *gin.Context) {
	destinationId := c.Param("destinationId")
	status := utils.NvlString(c.Query("status"), "dead")
	if status != retryTopicMode && status != deadTopicMode {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unknown status: " + status + " (should be '" + retryTopicMode + "' or '" + deadTopicMode + "')"})
		return
	}
	topicId, _ := MakeTopicId(destinationId, status, allTablesToken, false)
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
		r.ResponseError(c, http.StatusInternalServerError, "consumer error", true, err, "")
		return
	}
	start := time.Now()
	c.Header("Content-Type", "application/x-ndjson")
	for {
		msg, err := consumer.ReadMessage(time.Second * 2)
		jsn := make(map[string]any)
		if err != nil {
			kafkaErr := err.(kafka.Error)
			if kafkaErr.Code() == kafka.ErrTimedOut {
				break
			}
			errorID := uuid.NewLettersNumbers()
			err = fmt.Errorf("error# %s: couldn't read kafka message from topic: %s : %v", errorID, topicId, kafkaErr)
			r.Errorf(err.Error())
			jsn["ERROR"] = fmt.Errorf("error# %s: couldn't read kafka message", errorID).Error()
		} else {
			err = hjson.Unmarshal(msg.Value, &jsn)
			if err != nil {
				jsn["UNPARSABLE_MESSAGE"] = string(msg.Value)
			}
		}

		bytes, _ := jsoniter.Marshal(jsn)
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
		_ = r.ResponseError(c, http.StatusBadRequest, "error reading HTTP body", false, err, "")
		return
	}
	bulkerCfg := bulker.Config{}
	destinationConfig := map[string]any{}
	err = utils.ParseObject(body, &destinationConfig)
	if err != nil {
		_ = r.ResponseError(c, http.StatusUnprocessableEntity, "parse failed", false, err, "")
		return
	} else {
		r.Debugf("[test] parsed config for destination %s: %+v", utils.MapNVL(destinationConfig, "id", ""), destinationConfig)
	}
	bulkerCfg.DestinationConfig = destinationConfig
	bulkerCfg.Id = utils.MapNVL(destinationConfig, "id", "").(string)
	bulkerCfg.BulkerType = utils.MapNVL(destinationConfig, "destinationType", "").(string)

	b, err := bulker.CreateBulker(bulkerCfg)
	if err != nil {
		_ = r.ResponseError(c, http.StatusUnprocessableEntity, "error creating bulker", false, err, "")
		return
	}
	_ = b.Close()
	// test with stream settings
	//
	//if bulkerCfg.StreamConfig.BulkMode != "" || len(bulkerCfg.StreamConfig.Options) > 0 {
	//	options := bulker.StreamOptions{}
	//	for name, serializedOption := range bulkerCfg.StreamConfig.Options {
	//		opt, err := bulker.ParseOption(name, serializedOption)
	//		if err != nil {
	//			_ = r.ResponseError(c, http.StatusUnprocessableEntity, "option parse error", false, err)
	//			return
	//		}
	//		options.Add(opt)
	//	}
	//	str, err := b.CreateStream(bulkerCfg.Id(), bulkerCfg.TableName, bulkerCfg.BulkMode, options.Options...)
	//	if err != nil {
	//		_ = r.ResponseError(c, http.StatusUnprocessableEntity, "error creating bulker stream", false, err)
	//		return
	//	}
	//	_, _ = str.Abort(context.Background())
	//}
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
	ndjson := c.Query("ndjson")

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
	if ok, _ := strconv.ParseBool(ndjson); ok {
		c.Header("Content-Type", "application/x-ndjson")
		for _, record := range records {
			bytes, err := jsoniter.Marshal(record)
			if err != nil {
				bytes = []byte(fmt.Sprintf(`{"EVENTS_LOG_ERROR": "Failed to marshal event log record: %s", "OBJECT": "%+v"}`, err.Error(), record))
			}
			_, _ = c.Writer.Write(bytes)
			_, _ = c.Writer.Write([]byte("\n"))
		}
	} else {
		c.JSON(http.StatusOK, records)
	}
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
}
