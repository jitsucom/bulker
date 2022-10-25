package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jitsucom/bulker/app"
	"github.com/jitsucom/bulker/base/logging"
	_ "github.com/jitsucom/bulker/implementations/sql"
	"net/http"
	"time"
)

// var setup app.ConfigurationSource
//var repository *app.Repository
//var topicManager *app.TopicManager
//var batchRunner *app.BatchRunner

// TODO: graceful shutdown and cleanups. Flush producer
func main() {
	appConfig, err := app.InitAppConfig()
	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers": appConfig.KafkaBootstrapServers,
		"security.protocol": appConfig.KafkaSecurityProtocol,
		"sasl.mechanism":    appConfig.KafkaSaslMechanism,
		"sasl.username":     appConfig.KafkaSaslUsername,
		"sasl.password":     appConfig.KafkaSaslPassword,
	}
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
	router := app.NewRouter(appConfig, repository, topicManager, producer)

	server := &http.Server{
		Addr:              fmt.Sprintf("0.0.0.0:%d", appConfig.HTTPPort),
		Handler:           router.GetEngine(),
		ReadTimeout:       time.Second * 60,
		ReadHeaderTimeout: time.Second * 60,
		IdleTimeout:       time.Second * 65,
	}
	logging.Fatal(server.ListenAndServe())
}
