package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
	"github.com/jitsucom/bulker/app"
	"github.com/jitsucom/bulker/base/logging"
	_ "github.com/jitsucom/bulker/implementations/sql"
	"github.com/spf13/viper"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const workspaceId = "ildar"

// var setup app.SetupProvider
//var repository *app.Repository
//var topicManager *app.TopicManager
//var batchRunner *app.BatchRunner

// TODO: graceful shutdown and cleanups
func main() {
	config, err := initConfig()
	if err != nil {
		panic(err)
	}
	setupProvider, err := initSetupProvider(config)
	if err != nil {
		panic(err)
	}
	repository, err := app.NewRepository(config, setupProvider)
	if err != nil {
		panic(err)
	}
	topicManager, err := app.NewTopicManager(config)
	if err != nil {
		panic(err)
	}
	err = topicManager.Start()
	if err != nil {
		panic(err)
	}
	batchRunner := app.NewBatchRunner(config, repository, topicManager)
	batchRunner.Start()

	//TODO: producers config vars
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": config.KafkaBootstrapServers,
	})
	if err != nil {
		panic(err)
	}
	// Delivery report handler for produced messages
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	router := gin.New()
	router.POST("/queue/:destinationId", func(c *gin.Context) {
		destinationId := c.Param("destinationId")
		tableName := c.Query("tableName")
		destinationId = fmt.Sprintf("%s_%s", workspaceId, destinationId)

		destination := repository.GetDestination(destinationId)
		if destination == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "destination not found"})
			return
		}
		topicId := destination.TopicId(tableName)
		topics := topicManager.GetTopics(destinationId)
		if !topics.Contains(topicId) {
			logging.Infof("Topic %s not found for destination %s. Creating one", topicId, destinationId)
			err := topicManager.CreateTopic(destination, tableName)
			if err != nil {
				err = fmt.Errorf("couldn't create topic: %s : %w", topicId, err)
				logging.Error(err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
		}
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			fmt.Printf("error reading HTTP body: %v\n", err)
			return
		}
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topicId, Partition: kafka.PartitionAny},
			Value:          body,
		}, nil)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Errorf("couldn't produce message for kafka topic: %s : %w", topicId, err)})
			return
		}
		producer.Flush(100)

	})
	server := &http.Server{
		Addr:              "0.0.0.0:8182",
		Handler:           router,
		ReadTimeout:       time.Second * 60,
		ReadHeaderTimeout: time.Second * 60,
		IdleTimeout:       time.Second * 65,
	}
	logging.Fatal(server.ListenAndServe())
}

func initConfig() (*app.Config, error) {
	appConfig := app.Config{}
	configPath := os.Getenv("BULKER_CONFIG_PATH")
	if configPath == "" {
		configPath = "."
	}
	viper.AddConfigPath(configPath)
	viper.SetConfigName("bulker")
	viper.SetConfigType("env")
	viper.SetEnvPrefix("BULKER")
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		//it is ok to not have config file
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("❗error reading config file: %s", err)
		}
	}
	err := viper.Unmarshal(&appConfig)
	if err != nil {
		return nil, fmt.Errorf("❗error unmarshalling config: %s", err)
	}
	return &appConfig, nil
}

func initSetupProvider(config *app.Config) (app.SetupProvider, error) {
	if config.SetupProvider == "" {
		return nil, fmt.Errorf("❗️it is required to set Bulker Setup Provider using BULKER_SETUP_PROVIDER environement variable")
	}

	if strings.HasPrefix(config.SetupProvider, "file://") || !strings.Contains(config.SetupProvider, "://") {
		filePath := strings.TrimPrefix(config.SetupProvider, "file://")
		yamlConfig, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("❗️error reading yaml config file: %s: %w", filePath, err)
		}
		stpProvider, err := app.NewYamlSetupProvider(yamlConfig)
		if err != nil {
			return nil, fmt.Errorf("❗error creating yaml setup provider from config file: %s: %v", filePath, err)
		}
		return stpProvider, nil
	} else {
		return nil, fmt.Errorf("❗unsupported setup provider: %s", config.SetupProvider)
	}
}
