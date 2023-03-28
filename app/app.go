package app

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/utils"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var exitChannel = make(chan os.Signal, 1)

type AppContext struct {
	config              *AppConfig
	kafkaConfig         *kafka.ConfigMap
	configurationSource ConfigurationSource
	repository          *Repository
	cron                *Cron
	producer            *Producer
	eventsLogService    EventsLogService
	topicManager        *TopicManager
	fastStore           *FastStore
	server              *http.Server
	metricsServer       *MetricsServer
	metricsRelay        *MetricsRelay
}

// TODO: graceful shutdown and cleanups. Flush producer
func Run() {
	logging.SetTextFormatter()

	signal.Notify(exitChannel, os.Interrupt, os.Kill, syscall.SIGTERM)

	appContext := InitAppContext()

	go func() {
		signal := <-exitChannel
		logging.Infof("Received signal: %s. Shutting down...", signal)
		appContext.Shutdown()
		os.Exit(0)
	}()
	logging.Infof("Starting http server on %s", appContext.server.Addr)
	logging.Info(appContext.server.ListenAndServe())
}

func Exit(signal os.Signal) {
	logging.Infof("App Triggered Exit...")
	exitChannel <- signal
}

func InitAppContext() *AppContext {
	appContext := AppContext{}
	var err error
	appContext.config, err = InitAppConfig()
	if err != nil {
		panic(err)
	}
	appContext.kafkaConfig = appContext.config.GetKafkaConfig()

	if err != nil {
		panic(err)
	}
	appContext.configurationSource, err = InitConfigurationSource(appContext.config)
	if err != nil {
		panic(err)
	}
	appContext.repository, err = NewRepository(appContext.config, appContext.configurationSource)
	if err != nil {
		panic(err)
	}
	appContext.cron = NewCron(appContext.config)
	appContext.producer, err = NewProducer(appContext.config, appContext.kafkaConfig)
	if err != nil {
		panic(err)
	}
	appContext.producer.Start()

	appContext.eventsLogService = &DummyEventsLogService{}
	eventsLogRedisUrl := utils.NvlString(appContext.config.EventsLogRedisURL, appContext.config.RedisURL)
	if eventsLogRedisUrl != "" {
		appContext.eventsLogService, err = NewRedisEventsLog(appContext.config, eventsLogRedisUrl)
		if err != nil {
			panic(err)
		}
	}

	appContext.topicManager, err = NewTopicManager(&appContext)
	if err != nil {
		panic(err)
	}
	appContext.topicManager.Start()

	appContext.fastStore, err = NewFastStore(appContext.config)
	if err != nil {
		panic(err)
	}

	router := NewRouter(&appContext)
	appContext.server = &http.Server{
		Addr:              fmt.Sprintf("0.0.0.0:%d", appContext.config.HTTPPort),
		Handler:           router.GetEngine(),
		ReadTimeout:       time.Second * 60,
		ReadHeaderTimeout: time.Second * 60,
		IdleTimeout:       time.Second * 65,
	}
	metricsServer := NewMetricsServer(appContext.config)
	appContext.metricsServer = metricsServer

	metricsRelay, err := NewMetricsRelay(appContext.config)
	if err != nil {
		logging.Errorf("Error initializing metrics relay: %v", err)
	}
	appContext.metricsRelay = metricsRelay
	return &appContext
}

func (a *AppContext) Shutdown() {
	_ = a.producer.Close()
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
}
