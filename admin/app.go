package main

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/jitsucom/bulker/kafkabase"
	"net/http"
	"time"
)

type Context struct {
	config              *Config
	kafkaConfig         *kafka.ConfigMap
	repository          appbase.Repository[Streams]
	producer            *kafkabase.Producer
	server              *http.Server
	reprocessingManager *ReprocessingJobManager
}

func (a *Context) InitContext(settings *appbase.AppSettings) error {
	var err error
	a.config = &Config{}
	err = appbase.InitAppConfig(a.config, settings)
	if err != nil {
		return err
	}
	
	// Initialize repository
	a.repository = NewStreamsRepository(a.config.RepositoryURL, a.config.RepositoryAuthToken, a.config.RepositoryRefreshPeriodSec, a.config.CacheDir)
	
	// Initialize Kafka config
	a.kafkaConfig = a.config.GetKafkaConfig()
	
	// Initialize producer
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
	
	// Initialize reprocessing manager
	a.reprocessingManager, err = NewReprocessingJobManager(a.producer, a.repository, a.config)
	if err != nil {
		return fmt.Errorf("failed to initialize reprocessing manager: %w", err)
	}
	
	// Initialize HTTP server with router
	router := NewRouter(a)
	a.server = &http.Server{
		Addr:              fmt.Sprintf("0.0.0.0:%d", a.config.HTTPPort),
		Handler:           router.Engine(),
		ReadTimeout:       time.Second * 5,
		ReadHeaderTimeout: time.Second * 5,
		IdleTimeout:       time.Second * 65,
	}
	
	return nil
}

func (a *Context) Cleanup() error {
	// Close reprocessing manager first to cancel running jobs and log final status
	if a.reprocessingManager != nil {
		_ = a.reprocessingManager.Close()
	}
	
	// Close producer
	if a.producer != nil {
		_ = a.producer.Close()
	}
	
	// Shutdown HTTP server
	if a.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = a.server.Shutdown(ctx)
	}
	
	return nil
}

func (a *Context) ShutdownSignal() error {
	a.server.SetKeepAlivesEnabled(false)
	_ = a.server.Shutdown(context.Background())
	return nil
}

func (a *Context) Server() *http.Server {
	return a.server
}

func (a *Context) Config() *Config {
	return a.config
}