package app

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jitsucom/bulker/bulkerapp/metrics"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/jitsucom/bulker/jitsubase/safego"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"net/http"
	"runtime/debug"
	"time"
)

type Context struct {
	config              *Config
	kafkaConfig         *kafka.ConfigMap
	configurationSource ConfigurationSource
	repository          *Repository
	cron                *Cron
	batchProducer       *Producer
	streamProducer      *Producer
	eventsLogService    EventsLogService
	topicManager        *TopicManager
	fastStore           *FastStore
	server              *http.Server
	metricsServer       *MetricsServer
	metricsRelay        *MetricsRelay
}

func (a *Context) InitContext(settings *appbase.AppSettings) error {
	var err error
	a.config = &Config{}
	err = appbase.InitAppConfig(a.config, settings)
	if err != nil {
		return err
	}
	safego.GlobalRecoverHandler = func(value interface{}) {
		logging.Error("panic")
		logging.Error(value)
		logging.Error(string(debug.Stack()))
		metrics.Panics().Inc()
	}
	a.kafkaConfig = a.config.GetKafkaConfig()

	if err != nil {
		return err
	}
	a.configurationSource, err = InitConfigurationSource(a.config)
	if err != nil {
		return err
	}
	a.repository, err = NewRepository(a.config, a.configurationSource)
	if err != nil {
		return err
	}
	a.cron = NewCron(a.config)
	//batch producer uses higher linger.ms and doesn't suit for sync delivery used by stream consumer when retrying messages
	batchProducerConfig := kafka.ConfigMap(utils.MapPutAll(kafka.ConfigMap{
		"queue.buffering.max.messages": a.config.ProducerQueueSize,
		"batch.size":                   a.config.ProducerBatchSize,
		"linger.ms":                    a.config.ProducerLingerMs,
	}, *a.kafkaConfig))
	a.batchProducer, err = NewProducer(a.config, &batchProducerConfig, true)
	if err != nil {
		return err
	}
	a.batchProducer.Start()

	a.streamProducer, err = NewProducer(a.config, a.kafkaConfig, false)
	if err != nil {
		return err
	}
	a.streamProducer.Start()

	a.eventsLogService = &DummyEventsLogService{}
	eventsLogRedisUrl := utils.NvlString(a.config.EventsLogRedisURL, a.config.RedisURL)
	if eventsLogRedisUrl != "" {
		a.eventsLogService, err = NewRedisEventsLog(a.config, eventsLogRedisUrl)
		if err != nil {
			return err
		}
	}

	a.topicManager, err = NewTopicManager(a)
	if err != nil {
		return err
	}
	a.topicManager.Start()

	a.fastStore, err = NewFastStore(a.config)
	if err != nil {
		return err
	}

	router := NewRouter(a)
	a.server = &http.Server{
		Addr:        fmt.Sprintf(":%d", a.config.HTTPPort),
		Handler:     router.Engine(),
		ReadTimeout: time.Minute * 30,
		IdleTimeout: time.Minute * 5,
	}
	metricsServer := NewMetricsServer(a.config)
	a.metricsServer = metricsServer

	metricsRelay, err := NewMetricsRelay(a.config)
	if err != nil {
		logging.Errorf("Error initializing metrics relay: %v", err)
	}
	a.metricsRelay = metricsRelay
	return nil
}

// TODO: graceful shutdown and cleanups. Flush producer
func (a *Context) Shutdown() error {
	_ = a.batchProducer.Close()
	_ = a.topicManager.Close()
	a.cron.Close()
	_ = a.repository.Close()
	_ = a.configurationSource.Close()
	if a.config.ShutdownExtraDelay > 0 {
		logging.Infof("Waiting %d seconds before http server shutdown...", a.config.ShutdownExtraDelay)
		time.Sleep(time.Duration(a.config.ShutdownExtraDelay) * time.Second)
	}
	logging.Infof("Shutting down http server...")
	_ = a.metricsServer.Stop()
	_ = a.metricsRelay.Stop()
	_ = a.server.Shutdown(context.Background())
	_ = a.eventsLogService.Close()
	_ = a.fastStore.Close()
	return nil
}

func (a *Context) Config() *Config {
	return a.config
}

func (a *Context) Server() *http.Server {
	return a.server
}
