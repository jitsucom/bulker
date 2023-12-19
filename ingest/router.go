package main

import (
	"bytes"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gin-gonic/gin"
	"github.com/jitsucom/bulker/eventslog"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/timestamp"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/jitsucom/bulker/jitsubase/uuid"
	"github.com/jitsucom/bulker/kafkabase"
	jsoniter "github.com/json-iterator/go"
	"github.com/penglongli/gin-metrics/ginmetrics"
	timeout "github.com/vearne/gin-timeout"
	"io"
	"maps"
	"net/http"
	"net/http/pprof"
	"strings"
	"time"
)

var eventTypesDict = map[string]string{
	"p": "page",
	"i": "identify",
	"t": "track",
	"g": "group",
	"a": "alias",
	"s": "screen",
	"e": "event"}

var eventTypesSet = utils.NewSet("page", "identify", "track", "group", "alias", "screen")

type Router struct {
	*appbase.Router
	config           *Config
	kafkaConfig      *kafka.ConfigMap
	repository       *Repository
	script           *Script
	producer         *kafkabase.Producer
	eventsLogService eventslog.EventsLogService
	backupsLogger    *BackupLogger
	httpClient       *http.Client
}

type IngestType string

const (
	IngestTypeS2S     IngestType = "s2s"
	IngestTypeBrowser IngestType = "browser"
	// type of writeKey defines the type of ingest
	IngestTypeWriteKeyDefined IngestType = "writeKey"

	ConnectionIdsHeader = "connection_ids"
)

type StreamCredentials struct {
	Slug       string     `json:"slug"`
	Domain     string     `json:"domain"`
	WriteKey   string     `json:"writeKey"`
	IngestType IngestType `json:"ingestType"`
}

func NewRouter(appContext *Context) *Router {
	authTokens := strings.Split(appContext.config.AuthTokens, ",")
	tokenSecrets := strings.Split(appContext.config.TokenSecrets, ",")
	base := appbase.NewRouterBase(authTokens, tokenSecrets, []string{
		"/health",
		"/p.js",
		"/v1/projects/:writeKey/settings",
		"/v1/b",
		"/v1/batch",
		"/projects/:writeKey/settings",
		"/b",
		"/batch",
		"/api/s/:tp",
		"/api/s/s2s/:tp",
	})

	httpClient := &http.Client{
		Timeout: time.Duration(appContext.config.DeviceFunctionsTimeoutMs) * time.Millisecond,
	}

	router := &Router{
		Router:           base,
		config:           appContext.config,
		kafkaConfig:      appContext.kafkaConfig,
		producer:         appContext.producer,
		eventsLogService: appContext.eventsLogService,
		backupsLogger:    appContext.backupsLogger,
		repository:       appContext.repository,
		script:           appContext.script,
		httpClient:       httpClient,
	}
	engine := router.Engine()
	// get global Monitor object
	m := ginmetrics.GetMonitor()
	m.SetSlowTime(1)
	// set request duration, default {0.1, 0.3, 1.2, 5, 10}
	// used to p95, p99
	m.SetDuration([]float64{0.02, 0.05, 0.1, 0.2, 0.5})
	m.UseWithoutExposingEndpoint(engine)
	fast := engine.Group("")
	fast.Use(timeout.Timeout(timeout.WithTimeout(5 * time.Second)))
	fast.Use(router.CorsMiddleware)
	fast.Match([]string{"GET", "OPTIONS", "POST"}, "/v1/projects/:writeKey/settings", router.SettingsHandler)
	fast.Match([]string{"GET", "OPTIONS", "POST"}, "/projects/:writeKey/settings", router.SettingsHandler)
	fast.Match([]string{"OPTIONS", "POST"}, "/v1/batch", router.BatchHandler)
	fast.Match([]string{"OPTIONS", "POST"}, "/v1/b", router.BatchHandler)
	fast.Match([]string{"OPTIONS", "POST"}, "/batch", router.BatchHandler)
	fast.Match([]string{"OPTIONS", "POST"}, "/b", router.BatchHandler)
	fast.Match([]string{"OPTIONS", "POST"}, "/api/s/:tp", router.IngestHandler)
	fast.Match([]string{"OPTIONS", "POST"}, "/api/s/s2s/:tp", router.IngestHandler)
	fast.Match([]string{"GET", "HEAD", "OPTIONS"}, "/p.js", router.ScriptHandler)

	engine.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "pass"})
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

func (r *Router) CorsMiddleware(c *gin.Context) {
	c.Header("Access-Control-Allow-Origin", utils.NvlString(c.GetHeader("Origin"), "*"))
	c.Header("Access-Control-Allow-Methods", "GET,POST,HEAD,OPTIONS")
	c.Header("Access-Control-Allow-Headers", "x-enable-debug, x-write-key, authorization, content-type")
	c.Header("Access-Control-Allow-Credentials", "true")
	if c.Request.Method == "OPTIONS" {
		c.AbortWithStatus(http.StatusOK)
		return
	}
	c.Next()
}

type BatchPayload struct {
	Batch   []AnalyticsServerEvent `json:"batch"`
	Context map[string]any         `json:"context"`
}

func (r *Router) SettingsHandler(c *gin.Context) {
	writeKey := c.Param("writeKey")
	c.Data(http.StatusOK, "application/json", []byte(fmt.Sprintf(`{
  "integrations": {
    "Actions Google Analytic 4": {
      "versionSettings": {
        "componentTypes": []
      }
    },
    "Segment.io": {
      "apiKey": "%s",
      "unbundledIntegrations": [],
      "addBundledMetadata": true,
      "maybeBundledConfigIds": {},
      "versionSettings": {
        "version": "4.4.7",
        "componentTypes": [
          "browser"
        ]
      }
    }
  },
  "plan": {
    "track": {
      "__default": {
        "enabled": true,
        "integrations": {}
      }
    },
    "identify": {
      "__default": {
        "enabled": true
      }
    },
    "group": {
      "__default": {
        "enabled": true
      }
    }
  },
  "edgeFunction": {},
  "analyticsNextEnabled": true,
  "middlewareSettings": {},
  "enabledMiddleware": {},
  "metrics": {
    "sampleRate": 0.1
  },
  "legacyVideoPluginsEnabled": false,
  "remotePlugins": []
}`, writeKey)))
}

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
			rError = r.ResponseError(c, http.StatusInternalServerError, "panic", true, fmt.Errorf("%v", rerr), "", true)
		}
	}()
	if !strings.HasSuffix(c.ContentType(), "application/json") && !strings.HasSuffix(c.ContentType(), "text/plain") {
		rError = r.ResponseError(c, http.StatusBadRequest, "invalid content type", false, fmt.Errorf("%s. Expected: application/json", c.ContentType()), "", true)
		return
	}
	loc, err := r.getDataLocator(c, nil, IngestTypeWriteKeyDefined)
	if err != nil {
		rError = r.ResponseError(c, http.StatusOK, "error processing message", false, err, "", true)
		return
	}
	domain = utils.DefaultString(loc.Slug, loc.Domain)
	logPrefix := "[ingest][batch] Domain: " + domain
	stream := r.getStream(&loc)
	if stream == nil {
		rError = r.ResponseError(c, http.StatusOK, "stream not found", false, fmt.Errorf("for: %+v", loc), logPrefix, true)
		return
	}
	if len(stream.AsynchronousDestinations) == 0 {
		rError = r.ResponseError(c, http.StatusOK, "no destinations found for stream", false, fmt.Errorf(stream.Stream.Id), logPrefix, true)
		return
	}
	err = jsoniter.NewDecoder(c.Request.Body).Decode(&payload)
	if err != nil {
		rError = r.ResponseError(c, http.StatusOK, "error parsing message", false, err, logPrefix, true)
		return
	}
	eventsLogId := stream.Stream.Id
	okEvents := 0
	errors := make([]string, 0)
	for _, event := range payload.Batch {
		messageId, _, ingestMessageBytes, asyncDestinations, tagsDestinations, err1 := r.sendToBulker(c, &event, payload.Context, "event", loc, stream, logPrefix, false)
		if len(ingestMessageBytes) >= 0 {
			_ = r.backupsLogger.Log(utils.DefaultString(eventsLogId, "UNKNOWN"), ingestMessageBytes)
		}
		if err1 != nil {
			rError = err1
			obj := map[string]any{"body": string(ingestMessageBytes), "error": rError.PublicError.Error(), "status": "FAILED"}
			r.eventsLogService.PostAsync(&eventslog.ActorEvent{EventType: eventslog.EventTypeIncomingError, ActorId: eventsLogId, Event: obj})
			r.eventsLogService.PostAsync(&eventslog.ActorEvent{EventType: eventslog.EventTypeIncomingAll, ActorId: eventsLogId, Event: obj})
			IngestHandlerRequests(domain, "error", rError.ErrorType).Inc()
			_ = r.producer.ProduceAsync(r.config.KafkaDestinationsDeadLetterTopicName, uuid.New(), ingestMessageBytes, map[string]string{"error": rError.Error.Error()})
			errors = append(errors, fmt.Sprintf("Message ID: %s: %v", messageId, err1.PublicError))
		} else {
			okEvents++
			obj := map[string]any{"body": string(ingestMessageBytes), "asyncDestinations": asyncDestinations, "tags": tagsDestinations}
			if len(asyncDestinations) > 0 || len(tagsDestinations) > 0 {
				obj["status"] = "SUCCESS"
			} else {
				obj["status"] = "SKIPPED"
				obj["error"] = "no destinations found for stream"
			}
			r.eventsLogService.PostAsync(&eventslog.ActorEvent{EventType: eventslog.EventTypeIncomingAll, ActorId: eventsLogId, Event: obj})
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

func (r *Router) ScriptHandler(c *gin.Context) {
	if r.script == nil {
		c.AbortWithStatus(http.StatusNotFound)
		return
	}
	if c.Request.Method != "GET" && c.Request.Method != "HEAD" {
		c.AbortWithStatus(http.StatusMethodNotAllowed)
		return
	}
	ifNoneMatch := c.GetHeader("If-None-Match")
	etag := r.script.GetEtag()
	if etag != nil && ifNoneMatch != "" && *etag == ifNoneMatch {
		c.Header("ETag", *etag)
		c.AbortWithStatus(http.StatusNotModified)
		return
	}
	r.script.WriteScript(c, c.Request.Method == "HEAD", r.ShouldCompress(c.Request))
}

func (r *Router) IngestHandler(c *gin.Context) {
	domain := ""
	// TODO: use workspaceId as default for all stream identification errors
	var eventsLogId string
	var ingestType IngestType
	var rError *appbase.RouterError
	var body []byte
	var ingestMessageBytes []byte
	var asyncDestinations []string
	var tagsDestinations []string

	defer func() {
		if len(ingestMessageBytes) == 0 {
			ingestMessageBytes = body
		}
		if len(ingestMessageBytes) > 0 {
			_ = r.backupsLogger.Log(utils.DefaultString(eventsLogId, "UNKNOWN"), ingestMessageBytes)
		}
		if rError != nil {
			obj := map[string]any{"body": string(ingestMessageBytes), "error": rError.PublicError.Error(), "status": "FAILED"}
			r.eventsLogService.PostAsync(&eventslog.ActorEvent{EventType: eventslog.EventTypeIncomingError, ActorId: eventsLogId, Event: obj})
			r.eventsLogService.PostAsync(&eventslog.ActorEvent{EventType: eventslog.EventTypeIncomingAll, ActorId: eventsLogId, Event: obj})
			IngestHandlerRequests(domain, "error", rError.ErrorType).Inc()
			_ = r.producer.ProduceAsync(r.config.KafkaDestinationsDeadLetterTopicName, uuid.New(), ingestMessageBytes, map[string]string{"error": rError.Error.Error()})
		} else {
			obj := map[string]any{"body": string(ingestMessageBytes), "asyncDestinations": asyncDestinations, "tags": tagsDestinations}
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
	defer func() {
		if rerr := recover(); rerr != nil {
			rError = r.ResponseError(c, http.StatusInternalServerError, "panic", true, fmt.Errorf("%v", rerr), "", true)
		}
	}()
	if !strings.HasSuffix(c.ContentType(), "application/json") && !strings.HasSuffix(c.ContentType(), "text/plain") {
		rError = r.ResponseError(c, http.StatusBadRequest, "invalid content type", false, fmt.Errorf("%s. Expected: application/json", c.ContentType()), "", true)
		return
	}
	if c.FullPath() == "/api/s/s2s/:tp" {
		ingestType = IngestTypeS2S
	} else {
		ingestType = IngestTypeBrowser
	}
	tp := c.Param("tp")
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		rError = r.ResponseError(c, http.StatusOK, "error reading HTTP body", false, err, "", true)
		return
	}
	message := AnalyticsServerEvent{}
	err = jsoniter.Unmarshal(body, &message)
	if err != nil {
		rError = r.ResponseError(c, http.StatusOK, "error parsing message", false, fmt.Errorf("%v: %s", err, string(body)), "", true)
		return
	}
	messageId, _ := message["messageId"].(string)
	loc, err := r.getDataLocator(c, &message, ingestType)
	if err != nil {
		rError = r.ResponseError(c, http.StatusOK, "error processing message", false, fmt.Errorf("%v: %s", err, string(body)), "", true)
		return
	}
	domain = utils.DefaultString(loc.Slug, loc.Domain)
	logPrefix := fmt.Sprintf("[ingest] Message ID: %s Domain: %s", messageId, domain)
	stream := r.getStream(&loc)
	if stream == nil {
		rError = r.ResponseError(c, http.StatusOK, "stream not found", false, fmt.Errorf("for: %+v", loc), logPrefix, true)
		return
	}
	eventsLogId = stream.Stream.Id
	if len(stream.AsynchronousDestinations) == 0 && len(stream.SynchronousDestinations) == 0 {
		rError = r.ResponseError(c, http.StatusOK, "no destinations found for stream", false, fmt.Errorf(stream.Stream.Id), logPrefix, true)
		return
	}
	var ingestMessage *IngestMessage
	messageId, ingestMessage, ingestMessageBytes, asyncDestinations, tagsDestinations, rError = r.sendToBulker(c, &message, nil, tp, loc, stream, logPrefix, true)
	if len(tagsDestinations) == 0 {
		c.JSON(http.StatusOK, gin.H{"ok": true})
		return
	}
	resp := r.processSyncDestination(ingestMessage, stream, ingestMessageBytes)
	if resp != nil {
		//if r.ShouldCompress(c.Request) {
		//	c.Header("Content-Encoding", "gzip")
		//	c.Header("Content-Type", "application/json")
		//	c.Header("Vary", "Accept-Encoding")
		//	gz := gzip.NewWriter(c.Writer)
		//	_ = json.NewEncoder(gz).Encode(resp)
		//	_ = gz.Close()
		//} else {
		c.JSON(http.StatusOK, resp)
		//}
	} else {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	}
}

func (r *Router) sendToBulker(c *gin.Context, message *AnalyticsServerEvent, analyticContext map[string]any, tp string, loc StreamCredentials, stream *StreamWithDestinations, logPrefix string, sendResponse bool) (messageId string, ingestMessage *IngestMessage, ingestMessageBytes []byte, asyncDestinations []string, tagsDestinations []string, rError *appbase.RouterError) {
	err := patchEvent(c, message, tp, loc.IngestType, analyticContext)
	if err != nil {
		rError = r.ResponseError(c, http.StatusOK, "error processing message", false, fmt.Errorf("%v: %+v", err, *message), logPrefix, sendResponse)
		return
	}
	messageId = (*message)["messageId"].(string)
	logPrefix = fmt.Sprintf("[ingest] Message ID: %s Domain: %s", messageId, loc.Domain)

	ingestMessage, err = r.buildIngestMessage(c, message, loc)
	if err != nil {
		rError = r.ResponseError(c, http.StatusOK, "error building ingest message", false, err, logPrefix, sendResponse)
		return
	}
	ingestMessageBytes, err = jsoniter.Marshal(ingestMessage)
	if err != nil {
		rError = r.ResponseError(c, http.StatusOK, "error marshaling ingest message", false, err, logPrefix, sendResponse)
		return
	}

	asyncDestinations = utils.ArrayMap(stream.AsynchronousDestinations, func(d *ShortDestinationConfig) string { return d.ConnectionId })
	tagsDestinations = utils.ArrayMap(stream.SynchronousDestinations, func(d *ShortDestinationConfig) string { return d.ConnectionId })

	if stream.BackupEnabled {
		backupTopic := fmt.Sprintf("in.id.%s_backup.m.batch.t.backup", stream.Stream.WorkspaceId)
		err2 := r.producer.ProduceAsync(backupTopic, uuid.New(), ingestMessageBytes, nil)
		if err2 != nil {
			r.Errorf("Error producing to backup topic %s: %v", backupTopic, err2)
		}
	}

	r.Debugf("[ingest] Message ID: %s Domain: %s to Connections: [%s] Tags: [%s]", messageId, loc.Domain,
		strings.Join(asyncDestinations, ", "), strings.Join(tagsDestinations, ", "))
	if len(asyncDestinations) > 0 {
		topic := r.config.KafkaDestinationsTopicName
		messageKey := uuid.New()
		err = r.producer.ProduceAsync(topic, messageKey, ingestMessageBytes, map[string]string{ConnectionIdsHeader: strings.Join(asyncDestinations, ",")})
		if err != nil {
			for _, id := range asyncDestinations {
				IngestedMessages(id, "error", "producer error").Inc()
			}
			rError = r.ResponseError(c, http.StatusInternalServerError, "producer error", true, err, logPrefix, sendResponse)
		}
		for _, id := range asyncDestinations {
			IngestedMessages(id, "success", "").Inc()
		}
	}
	return
}

func patchEvent(c *gin.Context, event *AnalyticsServerEvent, tp string, ingestType IngestType, analyticContext map[string]any) error {
	typeFixed := utils.MapNVL(eventTypesDict, tp, tp)
	ev := *event
	if typeFixed == "event" {
		var ok bool
		typeFixed, ok = ev["type"].(string)
		if !ok {
			return fmt.Errorf("type property of event is required")
		}
	}
	if !eventTypesSet.Contains(typeFixed) {
		return fmt.Errorf("Unknown event type: %s", tp)
	}
	ip := utils.NvlString(c.GetHeader("X-Real-Ip"), c.GetHeader("X-Forwarded-For"), c.ClientIP())
	ev["request_ip"] = ip

	ctx, ok := ev["context"].(map[string]any)
	if !ok {
		ctx = map[string]any{}
	}

	if analyticContext != nil {
		mergedCtx := map[string]any{}
		maps.Copy(mergedCtx, analyticContext)
		maps.Copy(mergedCtx, ctx)
		ctx = mergedCtx
	}
	if ingestType == IngestTypeBrowser {
		//if ip comes from browser, don't trust it!
		ctx["ip"] = ip
	}
	if _, ok = ctx["userAgent"]; !ok {
		ctx["userAgent"] = c.GetHeader("User-Agent")
	}
	if _, ok = ctx["locale"]; !ok {
		ctx["locale"] = strings.TrimSpace(strings.Split(c.GetHeader("Accept-Language"), ",")[0])
	}
	ev["context"] = ctx
	nowIsoDate := time.Now().UTC().Format(timestamp.JsonISO)
	ev["receivedAt"] = nowIsoDate
	ev["type"] = typeFixed
	if _, ok = ev["timestamp"]; !ok {
		ev["timestamp"] = nowIsoDate
	}
	if _, ok = ev["messageId"]; !ok {
		ev["messageId"] = uuid.New()
	}
	return nil
}

func (r *Router) getDataLocator(c *gin.Context, event *AnalyticsServerEvent, ingestType IngestType) (cred StreamCredentials, err error) {
	dataHosts := strings.Split(r.config.DataDomain, ",")
	cred.IngestType = ingestType
	if c.GetHeader("Authorization") != "" {
		wk := strings.Replace(c.GetHeader("Authorization"), "Basic ", "", 1)
		fmt.Println(wk)
		//decode base64
		wkDecoded, err := base64.StdEncoding.DecodeString(wk)
		if err != nil {
			return cred, fmt.Errorf("failed to decode writeKey from Authorization header as base64: %v", err)
		}
		cred.WriteKey = string(wkDecoded)
	} else if c.GetHeader("X-Write-Key") != "" {
		cred.WriteKey = c.GetHeader("X-Write-Key")
	}
	host := strings.Split(c.Request.Host, ":")[0]
	for _, dataHost := range dataHosts {
		if dataHost != "" && strings.HasSuffix(host, "."+dataHost) {
			cred.Slug = strings.TrimSuffix(host, "."+dataHost)
			return
		}
	}
	cred.Domain = host

	return
}
func isInternalHeader(headerName string) bool {
	l := strings.ToLower(headerName)
	return strings.HasPrefix(l, "x-jitsu-") || strings.HasPrefix(l, "x-vercel")
}

type SyncDestinationsResponse struct {
	Destinations []*SyncDestinationsData `json:"destinations,omitempty"`
	OK           bool                    `json:"ok"`
}
type SyncDestinationsData struct {
	*ShortDestinationConfig `json:",inline,omitempty"`
	NewEvents               any `json:"newEvents,omitempty"`
	DeviceOptions           any `json:"deviceOptions,omitempty"`
}

func (r *Router) processSyncDestination(message *IngestMessage, stream *StreamWithDestinations, messageBytes []byte) *SyncDestinationsResponse {
	if len(stream.SynchronousDestinations) == 0 {
		return nil
	}
	filteredDestinations := utils.ArrayFilter(stream.SynchronousDestinations, func(d *ShortDestinationConfig) bool {
		return ApplyFilters(message.HttpPayload, d.Options)
	})
	if len(filteredDestinations) == 0 {
		return nil
	}
	var functionsResults map[string]any
	functionDestinations := utils.ArrayFilter(filteredDestinations, func(d *ShortDestinationConfig) bool {
		funcs, ok := d.Options["functions"].([]any)
		if !ok || len(funcs) == 0 {
			return false
		}
		return true
	})
	if len(functionDestinations) > 0 {
		var err error
		ids := utils.ArrayMap(functionDestinations, func(d *ShortDestinationConfig) string { return d.ConnectionId })
		defer func() {
			for _, id := range ids {
				if err != nil {
					IngestedMessages(id, "error", "device functions error").Inc()
				}
			}
		}()
		req, err := http.NewRequest("POST", r.config.RotorURL+"/func/multi?ids="+strings.Join(ids, ","), bytes.NewReader(messageBytes))
		if err != nil {
			r.Errorf("failed to create rotor request for connections: %s: %v", ids, err)
		}
		req.Header.Set("Content-Type", "application/json")
		res, err := r.httpClient.Do(req)
		if err != nil {
			r.Errorf("failed to send rotor request for device functions for connections: %s: %v", ids, err)
		} else {
			defer res.Body.Close()
			//get body
			body, err := io.ReadAll(res.Body)
			if res.StatusCode != 200 || err != nil {
				r.Errorf("Failed to send rotor request for device functions for connections: %s: status: %v body: %s", ids, res.StatusCode, string(body))
			} else {
				err = json.Unmarshal(body, &functionsResults)
				if err != nil {
					r.Errorf("Failed to unmarshal rotor response for connections: %s: %v", ids, err)
				}
			}
		}
	}
	data := make([]*SyncDestinationsData, 0, len(filteredDestinations))
	for _, d := range filteredDestinations {
		IngestedMessages(d.ConnectionId, "success", "").Inc()
		dOptions := DeviceOptions[d.DestinationType]
		newEvents, ok := functionsResults[d.ConnectionId]
		if ok {
			data = append(data, &SyncDestinationsData{ShortDestinationConfig: d, NewEvents: newEvents, DeviceOptions: dOptions})
		} else {
			data = append(data, &SyncDestinationsData{ShortDestinationConfig: d, DeviceOptions: dOptions})
		}
	}
	return &SyncDestinationsResponse{Destinations: data, OK: true}
}

func (r *Router) buildIngestMessage(c *gin.Context, event *AnalyticsServerEvent, loc StreamCredentials) (*IngestMessage, error) {
	headers := utils.MapMap(utils.MapFilter(c.Request.Header, func(k string, v []string) bool {
		return len(v) > 0 && !isInternalHeader(k)
	}), func(k string, v []string) string {
		if strings.ToLower(k) == "x-write-key" {
			return maskWriteKey(v[0])
		}
		return strings.Join(v, ",")
	})
	ingestMessage := IngestMessage{
		IngestType:     loc.IngestType,
		MessageCreated: time.Now(),
		MessageId:      (*event)["messageId"].(string),
		WriteKey:       maskWriteKey(loc.WriteKey),
		Type:           (*event)["type"].(string),
		Origin: IngestMessageOrigin{
			BaseURL: fmt.Sprintf("%s://%s", c.Request.URL.Scheme, c.Request.URL.Host),
			Slug:    loc.Slug,
			Domain:  loc.Domain,
		},
		HttpHeaders: headers,
		HttpPayload: event,
	}
	return &ingestMessage, nil
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
	BaseURL string `json:"baseUrl,omitempty"`
	Slug    string `json:"slug,omitempty"`
	Domain  string `json:"domain,omitempty"`
}

type IngestMessage struct {
	IngestType     IngestType            `json:"ingestType"`
	MessageCreated time.Time             `json:"messageCreated"`
	WriteKey       string                `json:"writeKey,omitempty"`
	MessageId      string                `json:"messageId"`
	Type           string                `json:"type"`
	Origin         IngestMessageOrigin   `json:"origin"`
	HttpHeaders    map[string]string     `json:"httpHeaders"`
	HttpPayload    *AnalyticsServerEvent `json:"httpPayload"`
}

type StreamLocator func(loc *StreamCredentials) *StreamWithDestinations

func (r *Router) getStream(loc *StreamCredentials) *StreamWithDestinations {
	var locators []StreamLocator
	if loc.IngestType == IngestTypeWriteKeyDefined {
		locators = []StreamLocator{r.WriteKeyStreamLocator}
	} else if loc.IngestType == IngestTypeS2S {
		locators = []StreamLocator{r.WriteKeyStreamLocator, r.SlugStreamLocator, r.AmbiguousDomainStreamLocator}
	} else {
		locators = []StreamLocator{r.SlugStreamLocator, r.DomainStreamLocator, r.WriteKeyStreamLocator}
	}
	for _, locator := range locators {
		stream := locator(loc)
		if stream != nil {
			return stream
		}
	}
	return nil
}

func (r *Router) WriteKeyStreamLocator(loc *StreamCredentials) *StreamWithDestinations {
	if loc.WriteKey != "" {
		parts := strings.Split(loc.WriteKey, ":")
		if len(parts) == 1 {
			stream, err := r.repository.GetStreamById(loc.WriteKey)
			if err != nil {
				r.Errorf("error getting stream: %v", err)
			} else {
				return stream
			}
		} else {
			var binding *ApiKeyBinding
			binding, err := r.repository.getStreamByKeyId(parts[0])
			if err == nil && binding != nil {
				if loc.IngestType != IngestTypeWriteKeyDefined && binding.KeyType != string(loc.IngestType) {
					r.Errorf("invalid key type: found %s, expected %s", binding.KeyType, loc.IngestType)
				} else if !r.checkHash(binding.Hash, parts[1]) {
					r.Errorf("invalid key secret")
				} else {
					stream, err := r.repository.GetStreamById(binding.StreamId)
					if err != nil {
						r.Errorf("error getting stream: %v", err)
					} else {
						loc.IngestType = IngestType(binding.KeyType)
						return stream
					}
				}
			}
		}
	}
	return nil
}

func (r *Router) SlugStreamLocator(loc *StreamCredentials) *StreamWithDestinations {
	if loc.Slug != "" {
		stream, err := r.repository.GetStreamById(loc.Slug)
		if err != nil {
			r.Errorf("error getting stream: %v", err)
		} else {
			return stream
		}
	}
	return nil
}

func (r *Router) DomainStreamLocator(loc *StreamCredentials) *StreamWithDestinations {
	if loc.Domain != "" {
		streams, err := r.repository.GetStreamsByDomain(loc.Domain)
		if err != nil {
			r.Errorf("error getting stream: %v", err)
		} else if len(streams) == 1 {
			return streams[0]
		} else if loc.WriteKey == "" && len(streams) > 1 {
			return streams[0]
		}
	}
	return nil
}

func (r *Router) AmbiguousDomainStreamLocator(loc *StreamCredentials) *StreamWithDestinations {
	if loc.Domain != "" {
		streams, err := r.repository.GetStreamsByDomain(loc.Domain)
		if err != nil {
			r.Errorf("error getting stream: %v", err)
		} else if len(streams) > 0 {
			return streams[0]
		}
	}
	return nil
}

func maskWriteKey(writeKey string) string {
	if writeKey != "" {
		parts := strings.Split(writeKey, ":")
		if len(parts) > 1 {
			return parts[0] + ":***"
		} else {
			return "***"
		}
	}
	return writeKey
}
