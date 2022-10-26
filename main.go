package main

import (
	"context"
	"fmt"
	"github.com/jitsucom/bulker/app"
	"github.com/jitsucom/bulker/base/logging"
	_ "github.com/jitsucom/bulker/implementations/sql"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// var setup app.ConfigurationSource
//var repository *app.Repository
//var topicManager *app.TopicManager
//var batchRunner *app.BatchRunner

// TODO: graceful shutdown and cleanups. Flush producer
func main() {
	exitChannel := make(chan os.Signal, 1) // we need to reserve to buffer size 1, so the notifier are not blocked
	signal.Notify(exitChannel, os.Interrupt, os.Kill, syscall.SIGTERM)

	appConfig, err := app.InitAppConfig()
	kafkaConfig := appConfig.GetKafkaConfig()

	if err != nil {
		panic(err)
	}
	configurationSource, err := app.InitConfigurationSource(appConfig)
	if err != nil {
		panic(err)
	}
	repository, err := app.NewRepository(appConfig, configurationSource)
	if err != nil {
		panic(err)
	}
	topicManager, err := app.NewTopicManager(appConfig, kafkaConfig)
	if err != nil {
		panic(err)
	}
	err = topicManager.Start()
	if err != nil {
		panic(err)
	}
	batchRunner := app.NewBatchRunner(appConfig, kafkaConfig, repository, topicManager)
	batchRunner.Start()

	producer, err := app.NewProducer(appConfig, kafkaConfig)
	if err != nil {
		panic(err)
	}
	producer.Start()

	router := app.NewRouter(appConfig, repository, topicManager, producer)
	server := &http.Server{
		Addr:              fmt.Sprintf("0.0.0.0:%d", appConfig.HTTPPort),
		Handler:           router.GetEngine(),
		ReadTimeout:       time.Second * 60,
		ReadHeaderTimeout: time.Second * 60,
		IdleTimeout:       time.Second * 65,
	}
	go func() {
		signal := <-exitChannel
		logging.Infof("Received signal: %s. Shutting down...", signal)
		_ = producer.Close()
		_ = batchRunner.Close()
		_ = topicManager.Close()
		_ = repository.Close()
		_ = server.Shutdown(context.Background())
		os.Exit(0)
	}()
	logging.Info(server.ListenAndServe())
}
