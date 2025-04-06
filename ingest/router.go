package main

import (
	"bytes"
	"crypto/sha512"
	"encoding/base64"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gin-gonic/gin"
	"github.com/jitsucom/bulker/eventslog"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/jsoniter"
	"github.com/jitsucom/bulker/jitsubase/jsonorder"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/jitsucom/bulker/jitsubase/timestamp"
	"github.com/jitsucom/bulker/jitsubase/types"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/jitsucom/bulker/jitsubase/uuid"
	"github.com/jitsucom/bulker/kafkabase"
	"github.com/penglongli/gin-metrics/ginmetrics"
	"io"
	"math/rand"
	"net/http"
	"net/http/pprof"
	"net/url"
	"regexp"
	"strconv"
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

var eventTypesSet = types.NewSet("page", "identify", "track", "group", "alias", "screen")

var messageIdUnsupportedChars = regexp.MustCompile(`[^a-zA-Z0-9._-]`)

type eventPatchFunc func(c *gin.Context, messageId string, event types.Json, tp string, ingestType IngestType, analyticContext types.Json, defaultEventName string) error

type Router struct {
	*appbase.Router
	config            *Config
	kafkaConfig       *kafka.ConfigMap
	repository        appbase.Repository[Streams]
	scriptRepository  appbase.Repository[Script]
	producer          *kafkabase.Producer
	eventsLogService  eventslog.EventsLogService
	backupsLogger     *BackupLogger
	httpClient        *http.Client
	dataHosts         []string
	partitionSelector kafkabase.PartitionSelector
}

type IngestType string

const (
	IngestTypeS2S     IngestType = "s2s"
	IngestTypeBrowser IngestType = "browser"
	// type of writeKey defines the type of ingest
	IngestTypeWriteKeyDefined IngestType = "writeKey"

	ConnectionIdsHeader = "connection_ids"

	ErrNoDst                = "no destinations found for stream"
	ErrThrottledType        = "quota exceeded"
	ErrThrottledDescription = "billing quota exceeded, event throttled"
)

type StreamCredentials struct {
	Slug       string     `json:"slug"`
	Domain     string     `json:"domain"`
	WriteKey   string     `json:"writeKey"`
	IngestType IngestType `json:"ingestType"`
}

func (sc *StreamCredentials) String() string {
	return fmt.Sprintf("[slug: %s][domain: %s][writeKey: %s][ingestType: %s]", sc.Slug, sc.Domain, maskWriteKey(sc.WriteKey), sc.IngestType)
}

func NewRouter(appContext *Context, partitionSelector kafkabase.PartitionSelector) *Router {
	base := appbase.NewRouterBase(appContext.config.Config, []string{
		"/health",
		"/robots.txt",
		"/p.js",
		"/v1/projects/:writeKey/settings",
		"/v1/b",
		"/v1/batch",
		"/projects/:writeKey/settings",
		"/b",
		"/batch",
		"/api/s/s2s/batch",
		"/api/s/:tp",
		"/api/px/:tp",
		"/api/s/s2s/:tp",
		// classic compat
		"/s/lib.js",
		"/api/v1/s2s/event",
		"/api/v1/s2s/event/",
		"/api/v1/s2s/events",
		"/api/v1/event",
		"/api/v1/events",
		"/api.:ignored",
	})

	httpClient := &http.Client{
		Timeout: time.Duration(appContext.config.DeviceFunctionsTimeoutMs) * time.Millisecond,
	}

	var dataHosts []string
	if appContext.config.DataDomain != "" {
		dataHosts = strings.Split(appContext.config.DataDomain, ",")
	} else if appContext.config.PublicURL != "" {
		u, err := url.ParseRequestURI(appContext.config.PublicURL)
		if err != nil {
			base.Errorf("Failed to parse %sPUBLIC_URL: %v", appContext.config.AppSetting.EnvPrefixWithUnderscore(), err)
		} else {
			dataHosts = []string{u.Hostname()}
		}
	}
	base.Infof("Data hosts: %s", dataHosts)

	router := &Router{
		Router:            base,
		config:            appContext.config,
		kafkaConfig:       appContext.kafkaConfig,
		producer:          appContext.producer,
		eventsLogService:  appContext.eventsLogService,
		backupsLogger:     appContext.backupsLogger,
		repository:        appContext.repository,
		scriptRepository:  appContext.scriptRepository,
		httpClient:        httpClient,
		dataHosts:         dataHosts,
		partitionSelector: partitionSelector,
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
	//fast.Use(timeout.Timeout(timeout.WithTimeout(5 * time.Second)))
	fast.Use(router.CorsMiddleware)
	fast.Match([]string{"GET", "OPTIONS", "POST"}, "/v1/projects/:writeKey/settings", router.SettingsHandler)
	fast.Match([]string{"GET", "OPTIONS", "POST"}, "/projects/:writeKey/settings", router.SettingsHandler)
	fast.Match([]string{"OPTIONS", "POST"}, "/v1/batch", router.BatchHandler)
	fast.Match([]string{"OPTIONS", "POST"}, "/v1/b", router.BatchHandler)
	fast.Match([]string{"OPTIONS", "POST"}, "/batch", router.BatchHandler)
	fast.Match([]string{"OPTIONS", "POST"}, "/b", router.BatchHandler)
	fast.Match([]string{"OPTIONS", "POST"}, "/api/s/s2s/batch", router.BatchHandler)

	fast.Match([]string{"OPTIONS", "POST"}, "/api/s/:tp", router.IngestHandler)
	fast.Match([]string{"OPTIONS", "GET"}, "/api/px/:tp", router.PixelHandler)
	fast.Match([]string{"OPTIONS", "POST"}, "/api/s/s2s/:tp", router.IngestHandler)

	// classic compat
	fast.Match([]string{"GET", "HEAD", "OPTIONS"}, "/s/lib.js", router.ClassicScriptHandler)
	fast.Match([]string{"OPTIONS", "POST"}, "/api/v1/s2s/event", router.ClassicHandler)
	fast.Match([]string{"OPTIONS", "POST"}, "/api/v1/s2s/event/", router.ClassicHandler)
	fast.Match([]string{"OPTIONS", "POST"}, "/api/v1/s2s/events", router.ClassicHandler)
	fast.Match([]string{"OPTIONS", "POST"}, "/api/v1/event", router.ClassicHandler)
	fast.Match([]string{"OPTIONS", "POST"}, "/api/v1/events", router.ClassicHandler)
	fast.Match([]string{"OPTIONS", "POST"}, "/api.:ignored", router.ClassicHandler)

	fast.Match([]string{"GET", "HEAD", "OPTIONS"}, "/p.js", router.ScriptHandler)

	engine.GET("/health", router.Health)
	engine.GET("/robots.txt", func(c *gin.Context) {
		c.Data(http.StatusOK, "text/plain", []byte("User-agent: *\nDisallow: /\n"))
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
	origin := c.GetHeader("Origin")
	if c.Request.Method == "OPTIONS" {
		c.Header("Access-Control-Allow-Origin", utils.NvlString(origin, "*"))
		c.Header("Access-Control-Allow-Methods", "GET,POST,HEAD,OPTIONS")
		// x-jitsu-custom - in case client want to add some custom payload via header
		c.Header("Access-Control-Allow-Headers", "x-enable-debug, x-write-key, authorization, content-type, x-ip-policy, cache-control, x-jitsu-custom")
		c.Header("Access-Control-Allow-Credentials", "true")
		c.Header("Access-Control-Max-Age", "86400")
		c.AbortWithStatus(http.StatusOK)
		return
	} else if origin != "" {
		c.Header("Access-Control-Allow-Origin", origin)
		c.Header("Access-Control-Allow-Methods", "GET,POST,HEAD,OPTIONS")
		// x-jitsu-custom - in case client want to add some custom payload via header
		c.Header("Access-Control-Allow-Headers", "x-enable-debug, x-write-key, authorization, content-type, x-ip-policy, cache-control, x-jitsu-custom")
		c.Header("Access-Control-Allow-Credentials", "true")
		c.Header("Access-Control-Max-Age", "86400")
	}
	c.Next()
}

func (r *Router) Health(c *gin.Context) {
	if r.kafkaConfig == nil {
		logging.Errorf("Health check: FAILED: kafka config is missing")
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "fail", "output": "kafka config is missing"})
		return
	}
	size, err := r.producer.QueueSize()
	if err != nil {
		logging.Errorf("Health check: FAILED: producer queue size error: %v", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "fail", "output": "producer queue size error: " + err.Error()})
		return
	}
	if float64(size) > r.config.ProducerQueueSizeThreshold*float64(r.config.ProducerQueueSize) {
		// we need to start worrying about the queue size before it reaches the limit
		logging.Errorf("Health check: FAILED: producer queue size: %d", size)
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "fail", "output": fmt.Sprintf("producer queue size: %d", size)})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "pass", "producerQueueSize": size})
	return
}

type BatchPayload struct {
	Batch      []types.Json `json:"batch"`
	EventsName string       `json:"eventsName"`
	Context    types.Json   `json:"context"`
	WriteKey   string       `json:"writeKey"`
}

func (r *Router) sendToRotor(c *gin.Context, ingestMessageBytes []byte, stream *StreamWithDestinations, sendResponse bool) (asyncDestinations []string, tagsDestinations []string, rError *appbase.RouterError) {
	var err error
	if stream.BackupEnabled {
		backupTopic := fmt.Sprintf("%sin.id.%s_backup.m.batch.t.backup", r.config.KafkaTopicPrefix, stream.Stream.WorkspaceId)
		err2 := r.producer.ProduceAsync(backupTopic, uuid.New(), ingestMessageBytes, nil, kafka.PartitionAny)
		if err2 != nil {
			r.Errorf("Error producing to backup topic %s: %v", backupTopic, err2)
		}
	}

	if stream.Throttle > 0 {
		if stream.Throttle >= 100 || rand.Int31n(100) < int32(stream.Throttle) {
			rError = r.ResponseError(c, http.StatusPaymentRequired, ErrThrottledType, false, fmt.Errorf(ErrThrottledDescription), sendResponse, false)
			return
		}
	}

	asyncDestinations = utils.ArrayMap(stream.AsynchronousDestinations, func(d *ShortDestinationConfig) string { return d.ConnectionId })
	tagsDestinations = utils.ArrayMap(stream.SynchronousDestinations, func(d *ShortDestinationConfig) string { return d.ConnectionId })

	if len(asyncDestinations) > 0 {
		topic := r.config.KafkaDestinationsTopicName
		partition := kafka.PartitionAny
		if stream.Shard > 0 {
			topic = fmt.Sprintf("%s-shard%d", topic, stream.Shard)
		} else {
			partition = r.partitionSelector.SelectPartition()
		}
		messageKey := uuid.New()
		err = r.producer.ProduceAsync(topic, messageKey, ingestMessageBytes, map[string]string{ConnectionIdsHeader: strings.Join(asyncDestinations, ",")}, partition)
		if err != nil {
			for _, id := range asyncDestinations {
				IngestedMessages(id, "error", "producer error").Inc()
			}
			rError = r.ResponseError(c, http.StatusInternalServerError, "producer error", true, err, sendResponse, true)
		}
		for _, id := range asyncDestinations {
			IngestedMessages(id, "success", "").Inc()
		}
	}
	return
}

func patchEvent(c *gin.Context, messageId string, ev types.Json, tp string, ingestType IngestType, analyticContext types.Json, defaultEventName string) error {
	typeFixed := utils.MapNVL(eventTypesDict, tp, tp)
	if typeFixed == "event" {
		if defaultEventName != "" {
			typeFixed = "track"
		} else {
			typeFixed = ev.GetS("type")
			if typeFixed == "" {
				return fmt.Errorf("type property of event is required")
			}
		}
	}
	if !eventTypesSet.Contains(typeFixed) {
		return fmt.Errorf("Unknown event type: %s", typeFixed)
	}
	if typeFixed == "track" {
		//check event name
		eventName := utils.DefaultString(ev.GetS("event"), defaultEventName)
		if eventName == "" {
			return fmt.Errorf("'event' property is required for 'track' event")
		}
		if strings.Contains(eventName, "--") || strings.Contains(eventName, ";") || strings.Contains(eventName, "=") || strings.Contains(eventName, "/*") {
			return fmt.Errorf("Invalid track event name '%s'. Only alpha-numeric characters, underscores and spaces are allowed in track event name.", eventName)
		}
		if len(eventName) > 64 {
			return fmt.Errorf("Invalid track event name '%s'. Max length is 64 characters.", eventName)
		}
		if defaultEventName != "" {
			ev.SetIfAbsent("event", eventName)
		}
	}
	ip := strings.TrimSpace(strings.Split(utils.NvlString(c.GetHeader("X-Real-Ip"), c.GetHeader("X-Forwarded-For"), c.ClientIP()), ",")[0])
	ipPolicy := c.GetHeader("X-IP-Policy")
	switch ipPolicy {
	case "stripLastOctet":
		ip = ipStripLastOctet(ip)
	case "remove":
		ip = ""
	}
	if ip != "" {
		ev.Set("requestIp", ip)
	}

	ctx, ok := ev.GetN("context").(types.Json)
	if !ok || ctx == nil {
		ctx = types.NewOrderedMap[string, any](4)
		ev.Set("context", ctx)
	}

	if analyticContext != nil && analyticContext.Len() > 0 {
		mergedCtx := analyticContext.Copy()
		mergedCtx.SetAll(ctx)
		ctx = mergedCtx
		ev.Set("context", ctx)
	}
	if ingestType == IngestTypeBrowser {
		//if ip comes from browser, don't trust it!
		if ip != "" {
			ctx.Set("ip", ip)
		}
		ctx.SetIfAbsentFunc("userAgent", func() any {
			return c.GetHeader("User-Agent")
		})
		ctx.SetIfAbsentFunc("locale", func() any {
			return strings.TrimSpace(strings.Split(c.GetHeader("Accept-Language"), ",")[0])
		})
		// remove any jitsu special properties from ingested events
		// it is only allowed to be set via functions
		types.FilterEvent(ev)
	}
	nowIsoDate := time.Now().UTC().Format(timestamp.JsonISO)
	ev.Set("receivedAt", nowIsoDate)
	ev.Set("type", typeFixed)
	ev.SetIfAbsent("timestamp", nowIsoDate)
	ev.SetIfAbsent("messageId", messageId)
	return nil
}

func (r *Router) getDataLocator(c *gin.Context, ingestType IngestType, writeKeyExtractor func() string) (cred StreamCredentials, err error) {
	cred.IngestType = ingestType
	if w := c.GetHeader("Authorization"); w != "" {
		wk := strings.TrimPrefix(w, "Basic ")
		//decode base64
		wkDecoded, err := base64.StdEncoding.DecodeString(wk)
		if err != nil {
			return cred, fmt.Errorf("failed to decode writeKey from Authorization header as base64: %v", err)
		}
		//remove trailing :
		wkDecoded = bytes.TrimSuffix(wkDecoded, []byte(":"))
		cred.WriteKey = string(wkDecoded)
	} else if w := c.GetHeader("X-Write-Key"); w != "" {
		cred.WriteKey = w
	} else if w := c.Query("writekey"); w != "" {
		cred.WriteKey = w
	} else if writeKeyExtractor != nil {
		cred.WriteKey = writeKeyExtractor()
	}
	host := strings.Split(c.Request.Host, ":")[0]
	for _, dataHost := range r.dataHosts {
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

func ipStripLastOctet(ip string) string {
	parts := strings.Split(ip, ".")
	if len(parts) == 4 {
		return strings.Join(parts[:3], ".") + ".0"
	}
	return ip
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
		defer func(ids []string) {
			for _, id := range ids {
				if err != nil {
					DeviceFunctions(id, "error").Inc()
				} else {
					DeviceFunctions(id, "success").Inc()
				}
			}
		}(ids)
		req, err := http.NewRequest("POST", r.config.RotorURL+"/func/multi?ids="+strings.Join(ids, ","), bytes.NewReader(messageBytes))
		if err != nil {
			r.Errorf("failed to create rotor request for connections: %s: %v", ids, err)
		} else {
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Request-Timeout-Ms", strconv.Itoa(r.config.DeviceFunctionsTimeoutMs))
			if r.config.RotorAuthKey != "" {
				req.Header.Set("Authorization", "Bearer "+r.config.RotorAuthKey)
			}
			var res *http.Response
			res, err = r.httpClient.Do(req)
			if err != nil {
				r.Errorf("failed to send rotor request for device functions for connections: %s: %v", ids, err)
			} else {
				defer res.Body.Close()
				//get body
				var body []byte
				body, err = io.ReadAll(res.Body)
				if res.StatusCode != 200 || err != nil {
					r.Errorf("Failed to send rotor request for device functions for connections: %s: status: %v body: %s", ids, res.StatusCode, string(body))
				} else {
					err = jsoniter.Unmarshal(body, &functionsResults)
					if err != nil {
						r.Errorf("Failed to unmarshal rotor response for connections: %s: %v", ids, err)
					}
				}
			}
		}
	}
	data := make([]*SyncDestinationsData, 0, len(filteredDestinations))
	for _, d := range filteredDestinations {
		IngestedMessages(d.ConnectionId, "success", "").Inc()
		dOptions := DeviceOptions[d.DestinationType]
		newEvents, ok := functionsResults[d.ConnectionId].([]any)
		if ok {
			if len(newEvents) > 0 {
				data = append(data, &SyncDestinationsData{ShortDestinationConfig: d.CloneForJsLib(), NewEvents: newEvents, DeviceOptions: dOptions})
			}
		} else {
			data = append(data, &SyncDestinationsData{ShortDestinationConfig: d.CloneForJsLib(), DeviceOptions: dOptions})
		}
	}
	return &SyncDestinationsResponse{Destinations: data, OK: true}
}

func (r *Router) buildIngestMessage(c *gin.Context, messageId string, event types.Json, analyticContext types.Json, tp string, loc StreamCredentials, stream *StreamWithDestinations, patchFunc eventPatchFunc, defaultEventName string) (ingestMessage *IngestMessage, ingestMessageBytes []byte, err error) {
	err = patchFunc(c, messageId, event, tp, loc.IngestType, analyticContext, defaultEventName)
	headers := utils.MapMap(utils.MapFilter(c.Request.Header, func(k string, v []string) bool {
		return len(v) > 0 && !isInternalHeader(k)
	}), func(k string, v []string) string {
		if strings.ToLower(k) == "x-write-key" {
			return maskWriteKey(v[0])
		}
		return strings.Join(v, ",")
	})
	bodyType := utils.Ternary(tp != "classic", event.GetS("type"), event.GetS("event_type"))
	ingestMessage = &IngestMessage{
		IngestType:     loc.IngestType,
		MessageCreated: utils.Ternary(tp != "classic", event.GetS("receivedAt"), event.GetS("_timestamp")),
		MessageId:      messageId,
		WriteKey:       maskWriteKey(loc.WriteKey),
		Type:           utils.NvlString(bodyType, tp),
		Origin: IngestMessageOrigin{
			BaseURL:    fmt.Sprintf("%s://%s", c.Request.URL.Scheme, c.Request.URL.Host),
			Slug:       loc.Slug,
			SourceId:   stream.Stream.Id,
			SourceName: stream.Stream.Name,
			Domain:     loc.Domain,
		},
		HttpHeaders: headers,
		HttpPayload: event,
	}
	if tp == "classic" {
		ingestMessage.Origin.Classic = true
	}
	ingestMessageBytes, err1 := jsonorder.Marshal(ingestMessage)
	if err1 != nil {
		err = utils.Nvl(err, err1)
	} else {
		if len(ingestMessageBytes) > r.config.MaxIngestPayloadSize {
			err = fmt.Errorf("message size %d is too big. max allowed: %d", len(ingestMessageBytes), r.config.MaxIngestPayloadSize)
		}
	}
	return
}

func hashApiKey(token string, salt string, secret string) string {
	hash := sha512.New()
	hash.Write([]byte(token + salt + secret))
	res := hash.Sum(nil)
	return fmt.Sprintf("%x", res)
}

func (r *Router) checkHash(hash string, secret string) bool {
	pk := strings.SplitN(hash, ".", 2)
	if len(pk) < 2 {
		r.Errorf("invalid hash format: %s", hash)
		return false
	}
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
	BaseURL    string `json:"baseUrl,omitempty"`
	Slug       string `json:"slug,omitempty"`
	SourceId   string `json:"sourceId,omitempty"`
	SourceName string `json:"sourceName,omitempty"`
	Domain     string `json:"domain,omitempty"`
	Classic    bool   `json:"classic,omitempty"`
}

type IngestMessage struct {
	IngestType     IngestType          `json:"ingestType"`
	MessageCreated string              `json:"messageCreated"`
	WriteKey       string              `json:"writeKey,omitempty"`
	MessageId      string              `json:"messageId"`
	Type           string              `json:"type"`
	Origin         IngestMessageOrigin `json:"origin"`
	HttpHeaders    map[string]string   `json:"httpHeaders"`
	HttpPayload    types.Json          `json:"httpPayload"`
}

type StreamLocator func(loc *StreamCredentials, s2sEndpoint bool) *StreamWithDestinations

func (r *Router) getStream(loc *StreamCredentials, strict, s2sEndpoint bool) *StreamWithDestinations {
	var locators []StreamLocator
	if strict {
		locators = []StreamLocator{r.WriteKeyStreamLocator}
	} else if loc.IngestType == IngestTypeWriteKeyDefined {
		locators = []StreamLocator{r.WriteKeyStreamLocator, r.SlugStreamLocator, r.AmbiguousDomainStreamLocator, r.SoleStreamLocator}
	} else if loc.IngestType == IngestTypeS2S {
		locators = []StreamLocator{r.WriteKeyStreamLocator, r.SlugStreamLocator, r.AmbiguousDomainStreamLocator}
	} else {
		locators = []StreamLocator{r.SlugStreamLocator, r.DomainStreamLocator, r.WriteKeyStreamLocator, r.SoleStreamLocator}
	}
	for _, locator := range locators {
		stream := locator(loc, s2sEndpoint)
		if stream != nil {
			return stream
		}
	}
	return nil
}

func (r *Router) checkOrigin(c *gin.Context, loc *StreamCredentials, stream *StreamWithDestinations) error {
	if loc.IngestType == IngestTypeBrowser {
		domainsString := strings.TrimSpace(stream.Stream.AuthorizedJavaScriptDomains)
		if domainsString != "" && domainsString != "*" {
			originString := c.GetHeader("Origin")
			if originString == "" {
				return nil
			}
			origin, trimmed := strings.CutPrefix(originString, "https://")
			if !trimmed {
				origin = strings.TrimPrefix(originString, "http://")
			}
			origin = strings.Split(origin, ":")[0]
			if !ApplyAuthorizedJavaScriptDomainsFilter(domainsString, origin) {
				return fmt.Errorf("Stream: %s Workspace: %s Origin %s is not authorized by: %s", stream.Stream.Id, stream.Stream.WorkspaceId, origin, domainsString)
			}
		}
	}
	return nil
}

func (r *Router) WriteKeyStreamLocator(loc *StreamCredentials, s2sEndpoint bool) *StreamWithDestinations {
	if loc.WriteKey != "" {
		parts := strings.Split(loc.WriteKey, ":")
		if len(parts) == 1 {
			loc.IngestType = utils.Ternary(s2sEndpoint, IngestTypeS2S, IngestTypeBrowser)
			if s2sEndpoint {
				return r.repository.GetData().GetS2SStreamByPlainKeyOrId(loc.WriteKey)
			} else {
				return r.repository.GetData().GetStreamByPlainKeyOrId(loc.WriteKey)
			}
		} else {
			binding := r.repository.GetData().getStreamByKeyId(parts[0])
			if binding != nil {
				if loc.IngestType != IngestTypeWriteKeyDefined && binding.KeyType != string(loc.IngestType) {
					r.Errorf("[stream: %s]%s invalid key type found %s", binding.StreamId, loc.String(), binding.KeyType)
				} else if !r.checkHash(binding.Hash, parts[1]) {
					r.Errorf("[stream: %s]%s invalid key secret", binding.StreamId, loc.String())
				} else {
					stream := r.repository.GetData().GetStreamByPlainKeyOrId(binding.StreamId)
					if stream != nil {
						loc.IngestType = IngestType(binding.KeyType)
						return stream
					}
				}
			}
		}
	}
	return nil
}

func (r *Router) SlugStreamLocator(loc *StreamCredentials, s2sEndpoint bool) *StreamWithDestinations {
	if loc.Slug != "" {
		stream := r.repository.GetData().GetStreamByPlainKeyOrId(loc.Slug)
		if stream != nil && !stream.Stream.Strict {
			loc.IngestType = utils.Ternary(s2sEndpoint, IngestTypeS2S, IngestTypeBrowser)
			return stream
		}
	}
	return nil
}

func (r *Router) DomainStreamLocator(loc *StreamCredentials, s2sEndpoint bool) *StreamWithDestinations {
	if loc.Domain != "" {
		streams := r.repository.GetData().GetStreamsByDomain(loc.Domain)
		if len(streams) == 1 {
			stream := streams[0]
			if !stream.Stream.Strict {
				loc.IngestType = utils.Ternary(s2sEndpoint, IngestTypeS2S, IngestTypeBrowser)
				return stream
			}
		} else if loc.WriteKey == "" && len(streams) > 1 {
			for _, stream := range streams {
				if !stream.Stream.Strict {
					loc.IngestType = utils.Ternary(s2sEndpoint, IngestTypeS2S, IngestTypeBrowser)
					return stream
				}
			}
		}
	}
	return nil
}

func (r *Router) AmbiguousDomainStreamLocator(loc *StreamCredentials, s2sEndpoint bool) *StreamWithDestinations {
	if loc.Domain != "" {
		streams := r.repository.GetData().GetStreamsByDomain(loc.Domain)
		if len(streams) > 0 {
			for _, stream := range streams {
				if !stream.Stream.Strict {
					loc.IngestType = utils.Ternary(s2sEndpoint, IngestTypeS2S, IngestTypeBrowser)
					return stream
				}
			}
		}
	}
	return nil
}

func (r *Router) SoleStreamLocator(loc *StreamCredentials, s2sEndpoint bool) *StreamWithDestinations {
	streams := r.repository.GetData().GetStreams()
	if len(streams) == 1 {
		stream := streams[0]
		if !stream.Stream.Strict {
			loc.IngestType = utils.Ternary(s2sEndpoint, IngestTypeS2S, IngestTypeBrowser)
			return stream
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
			return writeKey[0:1] + "***" + writeKey[len(writeKey)-1:]
		}
	}
	return writeKey
}
