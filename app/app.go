package app

import (
	"context"
	"fmt"
	"github.com/jitsucom/bulker/base/logging"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// TODO: graceful shutdown and cleanups. Flush producer
func Run() {
	logging.LogLevel = logging.INFO

	exitChannel := make(chan os.Signal, 1) // we need to reserve to buffer size 1, so the notifier are not blocked
	signal.Notify(exitChannel, os.Interrupt, os.Kill, syscall.SIGTERM)

	appConfig, err := InitAppConfig()
	kafkaConfig := appConfig.GetKafkaConfig()

	if err != nil {
		panic(err)
	}
	configurationSource, err := InitConfigurationSource(appConfig)
	if err != nil {
		panic(err)
	}
	repository, err := NewRepository(appConfig, configurationSource)
	if err != nil {
		panic(err)
	}
	cron := NewCron(appConfig)
	producer, err := NewProducer(appConfig, kafkaConfig)
	if err != nil {
		panic(err)
	}
	producer.Start()

	topicManager, err := NewTopicManager(appConfig, kafkaConfig, repository, cron, producer)
	if err != nil {
		panic(err)
	}
	topicManager.Start()

	//batchRunner := NewBatchRunner(appConfig, kafkaConfig, repository, topicManager)
	//batchRunner.Start()

	router := NewRouter(appConfig, kafkaConfig, repository, topicManager, producer)
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
		//TODO: proper consumer shutdown
		//_ = batchRunner.Close()
		_ = topicManager.Close()
		cron.Close()
		_ = repository.Close()
		_ = configurationSource.Close()
		_ = server.Shutdown(context.Background())
		os.Exit(0)
	}()
	logging.Info(server.ListenAndServe())
}
