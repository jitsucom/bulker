package main

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jitsucom/bulker/eventslog"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/jitsucom/bulker/kafkabase"
	"net/http"
	"time"
)

type Context struct {
	config           *Config
	kafkaConfig      *kafka.ConfigMap
	dbpool           *pgxpool.Pool
	repository       *Repository
	script           *Script
	producer         *kafkabase.Producer
	eventsLogService eventslog.EventsLogService
	server           *http.Server
	metricsServer    *MetricsServer
	backupsLogger    *BackupLogger
}

func (a *Context) InitContext(settings *appbase.AppSettings) error {
	var err error
	a.config = &Config{}
	err = appbase.InitAppConfig(a.config, settings)
	if err != nil {
		return err
	}
	a.dbpool, err = pgxpool.New(context.Background(), a.config.DatabaseURL)
	if err != nil {
		return fmt.Errorf("Unable to create postgres connection pool: %v\n", err)
	}
	a.repository = NewRepository(a.dbpool, a.config.RepositoryRefreshPeriodSec, a.config.CacheDir)
	a.script = NewScript(a.config.ScriptOrigin, a.config.CacheDir)
	a.eventsLogService = &eventslog.DummyEventsLogService{}
	eventsLogRedisUrl := a.config.RedisURL
	if eventsLogRedisUrl != "" {
		a.eventsLogService, err = eventslog.NewRedisEventsLog(a.config.RedisURL, a.config.RedisTLSCA, a.config.EventsLogMaxSize)
		if err != nil {
			return err
		}
	}
	a.kafkaConfig = a.config.GetKafkaConfig()
	//batch producer uses higher linger.ms and doesn't suit for sync delivery used by stream consumer when retrying messages
	producerConfig := kafka.ConfigMap(utils.MapPutAll(kafka.ConfigMap{
		"queue.buffering.max.messages": a.config.ProducerQueueSize,
		"batch.size":                   a.config.ProducerBatchSize,
		"linger.ms":                    a.config.ProducerLingerMs,
		"compression.type":             a.config.KafkaTopicCompression,
	}, *a.kafkaConfig))
	a.producer, err = kafkabase.NewProducer(&a.config.KafkaConfig, &producerConfig, true, nil)
	if err != nil {
		return err
	}
	a.producer.Start()
	a.backupsLogger = NewBackupLogger(a.config)
	router := NewRouter(a)
	a.server = &http.Server{
		Addr:              fmt.Sprintf("0.0.0.0:%d", a.config.HTTPPort),
		Handler:           router.Engine(),
		ReadTimeout:       time.Second * 5,
		ReadHeaderTimeout: time.Second * 5,
		IdleTimeout:       time.Second * 65,
	}
	a.metricsServer = NewMetricsServer(a.config)
	return nil
}

func (a *Context) Cleanup() error {
	_ = a.producer.Close()
	_ = a.backupsLogger.Close()
	if a.config.ShutdownExtraDelay > 0 {
		logging.Infof("Waiting %d seconds before http server shutdown...", a.config.ShutdownExtraDelay)
		time.Sleep(time.Duration(a.config.ShutdownExtraDelay) * time.Second)
	}
	_ = a.metricsServer.Stop()
	_ = a.eventsLogService.Close()
	a.script.Close()
	a.repository.Close()
	a.dbpool.Close()
	return nil
}

func (a *Context) ShutdownSignal() error {
	_ = a.server.Shutdown(context.Background())
	return nil
}

func (a *Context) Server() *http.Server {
	return a.server
}

func (a *Context) Config() *Config {
	return a.config
}
