package app

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/hjson/hjson-go/v4"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/spf13/viper"
	"os"
	"strings"
)

// Config is a struct for bulker app configuration
// It is loaded from `bulker.env` config file or environment variables.
//
// Environment variables requires prefix `BULKER_`
type Config struct {
	appbase.Config `mapstructure:",squash"`

	// For ingest endpoint only
	GlobalHashSecret string `mapstructure:"GLOBAL_HASH_SECRET" default:"dea42a58-acf4-45af-85bb-e77e94bd5025"`
	// For ingest endpoint only
	GlobalHashSecrets []string

	// # DESTINATIONS CONFIGS

	// ConfigSource source of destinations configs. Can be:
	//  - `file://...`  for destinations config in yaml format
	//  - `redis` or `redis://redis_url` to load configs from redis `enrichedConnections` key
	//  - `env://PREFIX` to load each destination environment variables with like `PREFIX_ID` where ID is destination id
	//
	// Default: `env://BULKER_DESTINATION`
	ConfigSource string `mapstructure:"CONFIG_SOURCE"`
	// RedisURL that will be used by default by all services that need Redis
	RedisURL   string `mapstructure:"REDIS_URL"`
	RedisTLSCA string `mapstructure:"REDIS_TLS_CA"`

	// # KAFKA

	// KafkaBootstrapServers List of Kafka brokers separated by comma. Each broker should be in format host:port.
	KafkaBootstrapServers string `mapstructure:"KAFKA_BOOTSTRAP_SERVERS"`
	KafkaSSL              bool   `mapstructure:"KAFKA_SSL" default:"false"`
	KafkaSSLSkipVerify    bool   `mapstructure:"KAFKA_SSL_SKIP_VERIFY" default:"false"`
	//Kafka authorization as JSON object {"mechanism": "SCRAM-SHA-256|PLAIN", "username": "user", "password": "password"}
	KafkaSASL string `mapstructure:"KAFKA_SASL"`

	KafkaTopicCompression    string `mapstructure:"KAFKA_TOPIC_COMPRESSION" default:"snappy"`
	KafkaTopicRetentionHours int    `mapstructure:"KAFKA_TOPIC_RETENTION_HOURS" default:"168"`

	KafkaRetryTopicRetentionHours            int    `mapstructure:"KAFKA_RETRY_TOPIC_RETENTION_HOURS" default:"48"`
	KafkaRetryTopicSegmentBytes              int    `mapstructure:"KAFKA_RETRY_TOPIC_SEGMENT_BYTES" default:"104857600"`
	KafkaDeadTopicRetentionHours             int    `mapstructure:"KAFKA_DEAD_TOPIC_RETENTION_HOURS" default:"168"`
	KafkaTopicReplicationFactor              int    `mapstructure:"KAFKA_TOPIC_REPLICATION_FACTOR"`
	KafkaAdminMetadataTimeoutMs              int    `mapstructure:"KAFKA_ADMIN_METADATA_TIMEOUT_MS" default:"1000"`
	KafkaConsumerPartitionsAssigmentStrategy string `mapstructure:"KAFKA_CONSUMER_PARTITIONS_ASSIGMENT_STRATEGY" default:"cooperative-sticky"`
	//TODO: max.poll.interval.ms

	// KafkaDestinationsTopicName destination topic for /ingest endpoint
	KafkaDestinationsTopicName              string `mapstructure:"KAFKA_DESTINATIONS_TOPIC_NAME" default:"destination-messages"`
	KafkaDestinationsTopicMultiThreadedName string `mapstructure:"KAFKA_DESTINATIONS_MT_TOPIC_NAME" default:"destination-messages-mt"`
	KafkaDestinationsTopicRetentionHours    int    `mapstructure:"KAFKA_DESTINATIONS_TOPIC_RETENTION_HOURS" default:"48"`

	KafkaDestinationsTopicPartitions              int `mapstructure:"KAFKA_DESTINATIONS_TOPIC_PARTITIONS" default:"4"`
	KafkaDestinationsTopicMultiThreadedPartitions int `mapstructure:"KAFKA_DESTINATIONS_MT_TOPIC_PARTITIONS" default:"64"`

	KafkaDestinationsRetryTopicName           string `mapstructure:"KAFKA_DESTINATIONS_RETRY_TOPIC_NAME" default:"destination-messages-retry"`
	KafkaDestinationsRetryRetentionHours      int    `mapstructure:"KAFKA_DESTINATIONS_RETRY_RETENTION_HOURS" default:"24"`
	KafkaDestinationsDeadLetterTopicName      string `mapstructure:"KAFKA_DESTINATIONS_DEAD_LETTER_TOPIC_NAME" default:"destination-messages-dead-letter"`
	KafkaDestinationsDeadLetterRetentionHours int    `mapstructure:"KAFKA_DESTINATIONS_DEAD_LETTER_RETENTION_HOURS" default:"168"`

	// TopicManagerRefreshPeriodSec how often topic manager will check for new topics
	TopicManagerRefreshPeriodSec int `mapstructure:"TOPIC_MANAGER_REFRESH_PERIOD_SEC" default:"5"`

	// ProducerWaitForDeliveryMs For ProduceSync only is a timeout for producer to wait for delivery report.
	ProducerQueueSize         int `mapstructure:"PRODUCER_QUEUE_SIZE" default:"100000"`
	ProducerBatchSize         int `mapstructure:"PRODUCER_BATCH_SIZE" default:"65535"`
	ProducerLingerMs          int `mapstructure:"PRODUCER_LINGER_MS" default:"1000"`
	ProducerWaitForDeliveryMs int `mapstructure:"PRODUCER_WAIT_FOR_DELIVERY_MS" default:"1000"`

	// # BATCHING

	BatchRunnerPeriodSec        int `mapstructure:"BATCH_RUNNER_DEFAULT_PERIOD_SEC" default:"300"`
	BatchRunnerDefaultBatchSize int `mapstructure:"BATCH_RUNNER_DEFAULT_BATCH_SIZE" default:"10000"`
	// BatchRunnerWaitForMessagesSec when there are no more messages in the topic BatchRunner will wait for BatchRunnerWaitForMessagesSec seconds before sending a batch
	BatchRunnerWaitForMessagesSec int `mapstructure:"BATCH_RUNNER_WAIT_FOR_MESSAGES_SEC" default:"5"`

	// # ERROR RETRYING

	BatchRunnerRetryPeriodSec            int     `mapstructure:"BATCH_RUNNER_DEFAULT_RETRY_PERIOD_SEC" default:"300"`
	BatchRunnerDefaultRetryBatchFraction float64 `mapstructure:"BATCH_RUNNER_DEFAULT_RETRY_BATCH_FRACTION" default:"0.1"`
	MessagesRetryCount                   int     `mapstructure:"MESSAGES_RETRY_COUNT" default:"5"`
	// MessagesRetryBackoffBase defines base for exponential backoff in minutes.
	// For example, if retry count is 3 and base is 5, then retry delays will be 5, 25, 125 minutes.
	// Default: 5
	MessagesRetryBackoffBase float64 `mapstructure:"MESSAGES_RETRY_BACKOFF_BASE" default:"5"`
	// MessagesRetryBackoffMaxDelay defines maximum possible retry delay in minutes. Default: 1440 minutes = 24 hours
	MessagesRetryBackoffMaxDelay float64 `mapstructure:"MESSAGES_RETRY_BACKOFF_MAX_DELAY" default:"1440"`

	// # EVENTS LOGGING

	EventsLogRedisURL string `mapstructure:"EVENTS_LOG_REDIS_URL"`
	EventsLogMaxSize  int    `mapstructure:"EVENTS_LOG_MAX_SIZE" default:"1000"`

	// # METRICS

	MetricsPort             int    `mapstructure:"METRICS_PORT" default:"9091"`
	MetricsRelayDestination string `mapstructure:"METRICS_RELAY_DESTINATION"`
	MetricsRelayPeriodSec   int    `mapstructure:"METRICS_RELAY_PERIOD_SEC" default:"60"`

	// # GRACEFUL SHUTDOWN

	//Timeout that give running batch tasks time to finish during shutdown.
	ShutdownTimeoutSec int `mapstructure:"SHUTDOWN_TIMEOUT_SEC" default:"10"`
	//Extra delay may be needed. E.g. for metric scrapper to scrape final metrics. So http server will stay active for an extra period.
	ShutdownExtraDelay int `mapstructure:"SHUTDOWN_EXTRA_DELAY_SEC"`
}

func init() {
	viper.SetDefault("HTTP_PORT", utils.NvlString(os.Getenv("PORT"), "3042"))
	viper.SetDefault("REDIS_URL", os.Getenv("REDIS_URL"))
	viper.SetDefault("EVENTS_LOG_REDIS_URL", utils.NvlString(os.Getenv("BULKER_REDIS_URL"), os.Getenv("REDIS_URL")))
}

func (ac *Config) PostInit(settings *appbase.AppSettings) error {
	err := ac.Config.PostInit(settings)
	if err != nil {
		return err
	}
	ac.GlobalHashSecrets = strings.Split(ac.GlobalHashSecret, ",")
	return nil
}

// GetKafkaConfig returns kafka config
func (ac *Config) GetKafkaConfig() *kafka.ConfigMap {
	if ac.KafkaBootstrapServers == "" {
		panic("❗️Kafka bootstrap servers are not set. Please set BULKER_KAFKA_BOOTSTRAP_SERVERS env variable")
	}
	kafkaConfig := &kafka.ConfigMap{
		"client.id":                "bulkerapp",
		"bootstrap.servers":        ac.KafkaBootstrapServers,
		"reconnect.backoff.ms":     1000,
		"reconnect.backoff.max.ms": 10000,
	}
	if ac.KafkaSSL {
		if ac.KafkaSASL != "" {
			_ = kafkaConfig.SetKey("security.protocol", "SASL_SSL")
		} else {
			_ = kafkaConfig.SetKey("security.protocol", "SSL")
		}
		if ac.KafkaSSLSkipVerify {
			_ = kafkaConfig.SetKey("enable.ssl.certificate.verification", false)
		}
	}
	if ac.KafkaSASL != "" {
		sasl := map[string]interface{}{}
		err := hjson.Unmarshal([]byte(ac.KafkaSASL), &sasl)
		if err != nil {
			panic(fmt.Errorf("error parsing Kafka SASL config: %v", err))
		}
		for k, v := range sasl {
			_ = kafkaConfig.SetKey("sasl."+k, v)
		}
	}

	return kafkaConfig
}
