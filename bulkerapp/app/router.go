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
	"github.com/jitsucom/bulker/eventslog"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/jitsucom/bulker/jitsubase/timestamp"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/jitsucom/bulker/jitsubase/uuid"
	jsoniter "github.com/json-iterator/go"
	timeout "github.com/vearne/gin-timeout"
	"io"
	"net/http"
	"net/http/pprof"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var TimestampPattern = regexp.MustCompile(`^\d{13}$`)
var WriteKeyPattern = regexp.MustCompile(`"writeKey":\s*"([^:"]+)?(:)?([^"]+)?"`)

type Router struct {
	*appbase.Router
	config           *Config
	kafkaConfig      *kafka.ConfigMap
	repository       *Repository
	topicManager     *TopicManager
	producer         *Producer
	eventsLogService eventslog.EventsLogService
	fastStore        *FastStore
}

func NewRouter(appContext *Context) *Router {
	base := appbase.NewRouterBase(appContext.config.Config, []string{"/ready", "/health"})

	router := &Router{
		Router:           base,
		config:           appContext.config,
		kafkaConfig:      appContext.kafkaConfig,
		repository:       appContext.repository,
		topicManager:     appContext.topicManager,
		producer:         appContext.batchProducer,
		eventsLogService: appContext.eventsLogService,
		fastStore:        appContext.fastStore,
	}
	engine := router.Engine()
	fast := engine.Group("")
	fast.Use(timeout.Timeout(timeout.WithTimeout(10 * time.Second)))
	fast.POST("/post/:destinationId", router.EventsHandler)
	fast.POST("/test", router.TestConnectionHandler)
	fast.GET("/log/:eventType/:actorId", router.EventsLogHandler)
	fast.GET("/ready", router.Health)
	fast.GET("/health", router.Health)

	engine.POST("/bulk/:destinationId", router.BulkHandler)
	engine.GET("/failed/:destinationId", router.FailedHandler)

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

func (r *Router) Health(c *gin.Context) {
	if r.kafkaConfig == nil {
		c.JSON(http.StatusOK, gin.H{"status": "pass"})
		return
	}
	if r.topicManager.IsReady() {
		c.JSON(http.StatusOK, gin.H{"status": "pass"})
	} else {
		logging.Errorf("Health check: FAILED")
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "fail", "output": "topic manager is not ready"})
	}
}

func (r *Router) EventsHandler(c *gin.Context) {
	destinationId := c.Param("destinationId")
	tableName := c.Query("tableName")
	metricsMeta := utils.NvlString(c.GetHeader("metricsMeta"), c.Query("metricsMeta"))
	mode := ""
	bytesRead := 0
	var rError *appbase.RouterError
	defer func() {
		if rError != nil {
			metrics.EventsHandlerRequests(destinationId, mode, tableName, "error", rError.ErrorType).Inc()
		} else {
			metrics.EventsHandlerRequests(destinationId, mode, tableName, "success", "").Inc()
			metrics.EventsHandlerBytes(destinationId, mode, tableName, "success", "").Add(float64(bytesRead))
		}
	}()

	destination := r.repository.GetDestination(destinationId)
	if destination == nil {
		rError = r.ResponseError(c, http.StatusNotFound, "destination not found", false, fmt.Errorf("destination not found: %s", destinationId), true)
		return
	}
	mode = string(destination.Mode())
	if tableName == "" {
		rError = r.ResponseError(c, http.StatusBadRequest, "missing required parameter", false, fmt.Errorf("tableName query parameter is required"), true)
		return
	}
	topicId, err := destination.TopicId(tableName)
	if err != nil {
		rError = r.ResponseError(c, http.StatusInternalServerError, "couldn't generate topicId", false, err, true)
		return
	}
	err = r.topicManager.EnsureDestinationTopic(destination, topicId)
	if err != nil {
		kafkaErr, ok := err.(kafka.Error)
		if ok && kafkaErr.Code() == kafka.ErrTopicAlreadyExists {
			r.Warnf("Topic %s already exists", topicId)
		} else {
			rError = r.ResponseError(c, http.StatusInternalServerError, "couldn't create topic", false, fmt.Errorf("topicId %s: %v", topicId, err), true)
			return
		}
	}

	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		rError = r.ResponseError(c, http.StatusBadRequest, "error reading HTTP body", false, err, true)
		return
	}
	bytesRead = len(body)
	err = r.producer.ProduceAsync(topicId, uuid.New(), body, map[string]string{MetricsMetaHeader: metricsMeta}, kafka.PartitionAny)
	if err != nil {
		rError = r.ResponseError(c, http.StatusInternalServerError, "producer error", true, err, true)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "ok"})
}

func (r *Router) BulkHandler(c *gin.Context) {
	start := time.Now()
	destinationId := c.Param("destinationId")
	tableName := c.Query("tableName")
	taskId := c.DefaultQuery("taskId", uuid.New())
	jobId := c.DefaultQuery("jobId", fmt.Sprintf("%s_%s_%s", destinationId, tableName, taskId))
	bulkMode := bulker.BulkMode(c.DefaultQuery("mode", string(bulker.ReplaceTable)))
	pkeys := c.QueryArray("pk")
	schemaHeader := c.GetHeader("X-Jitsu-Schema")
	mode := ""
	bytesRead := 0
	var err error
	var rError *appbase.RouterError
	var processedObjectSample types.Object
	var state bulker.State
	defer func() {
		state.ProcessingTimeSec = time.Since(start).Seconds()
		if rError != nil {
			r.postEventsLog(destinationId, state, processedObjectSample, rError.PublicError)
			metrics.BulkHandlerRequests(destinationId, mode, tableName, "error", rError.ErrorType).Inc()
		} else {
			r.postEventsLog(destinationId, state, processedObjectSample, nil)
			metrics.BulkHandlerRequests(destinationId, mode, tableName, "success", "").Inc()
			metrics.EventsHandlerBytes(destinationId, mode, tableName, "success", "").Add(float64(bytesRead))
		}
	}()

	destination := r.repository.GetDestination(destinationId)
	if destination == nil {
		rError = r.ResponseError(c, http.StatusNotFound, "destination not found", false, fmt.Errorf("destination not found: %s", destinationId), true)
		return
	}
	mode = string(destination.Mode())
	if tableName == "" {
		rError = r.ResponseError(c, http.StatusBadRequest, "missing required parameter", false, fmt.Errorf("tableName query parameter is required"), true)
		return
	}
	var streamOptions []bulker.StreamOption
	if len(pkeys) > 0 {
		streamOptions = append(streamOptions, bulker.WithPrimaryKey(pkeys...), bulker.WithDeduplicate())
	}
	if schemaHeader != "" {
		schema := types.Schema{}
		err = json.Unmarshal([]byte(schemaHeader), &schema)
		if err != nil {
			rError = r.ResponseError(c, http.StatusBadRequest, "schema unmarshal error", false, err, true)
			return
		}
		if !schema.IsEmpty() {
			streamOptions = append(streamOptions, bulker.WithSchema(schema))
		}
		r.Infof("Schema for %s: %v", jobId, schema)
	}
	//streamOptions = append(streamOptions, sql.WithoutOmitNils())
	destination.InitBulkerInstance()
	bulkerStream, err := destination.bulker.CreateStream(jobId, tableName, bulkMode, streamOptions...)
	if err != nil {
		rError = r.ResponseError(c, http.StatusInternalServerError, "create stream error", true, err, true)
		return
	}
	scanner := bufio.NewScanner(c.Request.Body)
	scanner.Buffer(make([]byte, 1024*100), 1024*1024*10)
	consumed := 0
	for scanner.Scan() {
		eventBytes := scanner.Bytes()
		if len(eventBytes) >= 5 && string(eventBytes[:5]) == "ABORT" {
			state, _ = bulkerStream.Abort(c)
			rError = r.ResponseError(c, http.StatusBadRequest, "aborted", false, fmt.Errorf(string(eventBytes)), true)
			return
		}
		bytesRead += len(eventBytes)
		obj := types.Object{}
		dec := jsoniter.NewDecoder(bytes.NewReader(eventBytes))
		dec.UseNumber()
		if err = dec.Decode(&obj); err != nil {
			state, _ = bulkerStream.Abort(c)
			rError = r.ResponseError(c, http.StatusBadRequest, "unmarhsal error", false, err, true)
			return
		}
		if _, processedObjectSample, err = bulkerStream.Consume(c, obj); err != nil {
			state, _ = bulkerStream.Abort(c)
			rError = r.ResponseError(c, http.StatusBadRequest, "stream consume error", false, err, true)
			return
		}
		consumed++
	}
	if err = scanner.Err(); err != nil {
		state, _ = bulkerStream.Abort(c)
		rError = r.ResponseError(c, http.StatusBadRequest, "scanner error", false, err, true)
		return
	}
	if consumed > 0 {
		state, err = bulkerStream.Complete(c)
		if err != nil {
			rError = r.ResponseError(c, http.StatusBadRequest, "stream complete error", false, err, true)
			return
		}
		r.Infof("Bulk stream for %s mode: %s Completed. Processed: %d in %dms.", jobId, mode, state.SuccessfulRows, time.Since(start).Milliseconds())
		c.JSON(http.StatusOK, gin.H{"message": "ok", "state": state})
	} else {
		state, _ = bulkerStream.Abort(c)
		c.JSON(http.StatusOK, gin.H{"message": "ok"})
	}
}

func (r *Router) postEventsLog(destinationId string, state bulker.State, processedObjectSample types.Object, batchErr error) {
	if batchErr != nil && state.LastError == nil {
		state.SetError(batchErr)
	}
	batchState := BatchState{State: state, LastMappedRow: processedObjectSample}
	level := eventslog.LevelInfo
	if batchErr != nil {
		level = eventslog.LevelError
	}
	r.eventsLogService.PostAsync(&eventslog.ActorEvent{EventType: eventslog.EventTypeBatch, Level: level, ActorId: destinationId, Event: batchState})
}

func maskWriteKey(wk string) string {
	arr := strings.Split(wk, ":")
	if len(arr) > 1 {
		return arr[0] + ":***"
	} else {
		return "***"
	}
}

func (r *Router) FailedHandler(c *gin.Context) {
	destinationId := c.Param("destinationId")
	status := utils.DefaultString(c.Query("status"), "dead")
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
		r.ResponseError(c, http.StatusInternalServerError, "consumer error", true, err, true)
		return
	}
	start := time.Now()
	c.Header("Content-Type", "application/x-ndjson")
	for {
		msg, err := consumer.ReadMessage(time.Second * 5)
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
		_ = r.ResponseError(c, http.StatusBadRequest, "error reading HTTP body", false, err, true)
		return
	}
	bulkerCfg := bulker.Config{}
	destinationConfig := map[string]any{}
	err = utils.ParseObject(body, &destinationConfig)
	if err != nil {
		_ = r.ResponseError(c, http.StatusUnprocessableEntity, "parse failed", false, err, true)
		return
	} else {
		r.Debugf("[test] parsed config for destination %s: %+v", utils.MapNVL(destinationConfig, "id", ""), destinationConfig)
	}
	bulkerCfg.DestinationConfig = destinationConfig
	bulkerCfg.Id = utils.MapNVL(destinationConfig, "id", "").(string)
	bulkerCfg.BulkerType = utils.MapNVL(destinationConfig, "destinationType", "").(string)

	b, err := bulker.CreateBulker(bulkerCfg)
	if err != nil {
		if b != nil {
			_ = b.Close()
		}
		_ = r.ResponseError(c, http.StatusUnprocessableEntity, "error creating bulker", false, err, true)
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
	eventKey := c.Param("eventType")
	actorId := c.Param("actorId")
	beforeId := c.Query("beforeId")
	start := c.Query("start")
	end := c.Query("end")
	limit := c.Query("limit")
	ndjson := c.Query("ndjson")
	maxBytesStr := c.Query("maxBytes")
	maxBytes := 0
	var err error
	if maxBytesStr != "" {
		maxBytes, err = strconv.Atoi(maxBytesStr)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "'maxBytes' parameter must be an integer number"})
			return
		}
	}

	eventsLogFilter := &eventslog.EventsLogFilter{}
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
	parts := strings.Split(eventKey, ".")
	eventType := parts[0]
	level := parts[1]
	eventsLogFilter.BeforeId = eventslog.EventsLogRecordId(beforeId)
	records, err := r.eventsLogService.GetEvents(eventslog.EventType(eventType), actorId, level, eventsLogFilter, iLimit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get events log: " + err.Error()})
		return
	}
	written := 0
	if ok, _ := strconv.ParseBool(ndjson); ok {
		c.Header("Content-Type", "application/x-ndjson")
		for _, record := range records {
			maskWriteKeyInObj(eventType, record)
			bytes, err := jsoniter.Marshal(record)
			if err != nil {
				bytes = []byte(fmt.Sprintf(`{"EVENTS_LOG_ERROR": "Failed to marshal event log record: %s", "OBJECT": "%+v"}`, err.Error(), record))
			}
			if maxBytes > 0 && written+len(bytes) > maxBytes {
				break
			}
			_, _ = c.Writer.Write(bytes)
			_, _ = c.Writer.Write([]byte("\n"))
			written += len(bytes) + 1
		}
	} else {
		c.Header("Content-Type", "application/json")
		_, _ = c.Writer.Write([]byte("["))
		for _, record := range records {
			maskWriteKeyInObj(eventType, record)
			bytes, err := jsoniter.Marshal(record)
			if err != nil {
				bytes = []byte(fmt.Sprintf(`{"EVENTS_LOG_ERROR": "Failed to marshal event log record: %s", "OBJECT": "%+v"}`, err.Error(), record))
			}
			if maxBytes > 0 && written+len(bytes) > maxBytes {
				break
			}
			if written > 0 {
				_, _ = c.Writer.Write([]byte(","))
			}
			_, _ = c.Writer.Write(bytes)
			written += len(bytes) + 1
		}
		_, _ = c.Writer.Write([]byte("]"))
	}
}

func maskWriteKeyInObj(eventType string, record eventslog.EventsLogRecord) {
	if eventType == "incoming" {
		o, ok := record.Content.(map[string]any)
		if ok {
			b, ok := o["body"].(string)
			if ok {
				o["body"] = WriteKeyPattern.ReplaceAllString(b, `"writeKey": "$1$2***"`)
			}
		}
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
			stream, err := r.fastStore.GetStreamById(ingestMessage.WriteKey)
			if err != nil {
				r.Errorf("error getting stream: %v", err)
			} else {
				return stream
			}
		} else {
			var binding *ApiKeyBinding
			binding, err := r.fastStore.getStreamByKeyId(parts[0])
			if err == nil && binding != nil {
				if binding.KeyType != ingestMessage.IngestType {
					r.Errorf("invalid key type: found %s, expected %s", binding.KeyType, ingestMessage.IngestType)
				} else if !r.checkHash(binding.Hash, parts[1]) {
					r.Errorf("invalid key secret")
				} else {
					stream, err := r.fastStore.GetStreamById(binding.StreamId)
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
		stream, err := r.fastStore.GetStreamById(ingestMessage.Origin.Slug)
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
		streams, err := r.fastStore.GetStreamsByDomain(ingestMessage.Origin.Domain)
		if err != nil {
			r.Errorf("error getting stream: %v", err)
		} else if len(streams) == 1 {
			return &streams[0]
		} else if ingestMessage.WriteKey == "" && len(streams) > 1 {
			return &streams[0]
		}
	}
	return nil
}

func (r *Router) AmbiguousDomainStreamLocator(ingestMessage IngestMessage) *StreamWithDestinations {
	if ingestMessage.Origin.Domain != "" {
		streams, err := r.fastStore.GetStreamsByDomain(ingestMessage.Origin.Domain)
		if err != nil {
			r.Errorf("error getting stream: %v", err)
		} else if len(streams) > 0 {
			return &streams[0]
		}
	}
	return nil
}
