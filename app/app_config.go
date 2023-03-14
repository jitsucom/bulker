package app

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hjson/hjson-go/v4"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/base/uuid"
	"github.com/spf13/viper"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

const instanceIdFilePath = "~/.bulkerapp/instance_id"

// AppConfig is a struct for bulker app configuration
// It is loaded from `bulker.env` config file or environment variables.
//
// Environment variables requires prefix `BULKER_`
type AppConfig struct {
	// InstanceId ID of bulker instance. It is used for identifying Kafka consumers.
	// If is not set, instance id will be generated and persisted to disk (~/.bulkerapp/instance_id) and reused on next restart.
	// Default: random uuid
	InstanceId string `mapstructure:"INSTANCE_ID"`

	// HTTPPort port for bulker http server. Default: 3042
	HTTPPort int `mapstructure:"HTTP_PORT"`

	// # AUTH

	// AuthTokens A list of auth tokens that authorizes user in HTTP interface separated by comma. Each token can be either:
	// - `${token}` un-encrypted token value
	// - `${salt}.${hash}` hashed token.` ${salt}` should be random string. Hash is `base64(sha512($token + $salt + TokenSecrets)`.
	// - Token is `[0-9a-zA-Z_\-]` (only letters, digits, underscore and dash)
	AuthTokens string `mapstructure:"AUTH_TOKENS"`
	// See AuthTokens
	TokenSecrets string `mapstructure:"TOKEN_SECRET"`
	// For ingest endpoint only
	GlobalHashSecret string `mapstructure:"GLOBAL_HASH_SECRET" default:"dea42a58-acf4-45af-85bb-e77e94bd5025"`
	// For ingest endpoint only
	GlobalHashSecrets []string

	// # LOGGING

	// LogFormat log format. Can be `text` or `json`. Default: `text`
	LogFormat string `mapstructure:"LOG_FORMAT"`

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

	KafkaTopicCompression                    string `mapstructure:"KAFKA_TOPIC_COMPRESSION" default:"snappy"`
	KafkaTopicRetentionHours                 int    `mapstructure:"KAFKA_TOPIC_RETENTION_HOURS" default:"168"`
	KafkaRetryTopicRetentionHours            int    `mapstructure:"KAFKA_RETRY_TOPIC_RETENTION_HOURS" default:"168"`
	KafkaRetryTopicSegmentBytes              int    `mapstructure:"KAFKA_RETRY_TOPIC_SEGMENT_BYTES" default:"104857600"`
	KafkaDeadTopicRetentionHours             int    `mapstructure:"KAFKA_DEAD_TOPIC_RETENTION_HOURS" default:"168"`
	KafkaTopicReplicationFactor              int    `mapstructure:"KAFKA_TOPIC_REPLICATION_FACTOR"`
	KafkaAdminMetadataTimeoutMs              int    `mapstructure:"KAFKA_ADMIN_METADATA_TIMEOUT_MS" default:"1000"`
	KafkaConsumerPartitionsAssigmentStrategy string `mapstructure:"KAFKA_CONSUMER_PARTITIONS_ASSIGMENT_STRATEGY" default:"cooperative-sticky"`
	//TODO: max.poll.interval.ms

	// KafkaDestinationsTopicName destination topic for /ingest endpoint
	KafkaDestinationsTopicName       string `mapstructure:"KAFKA_DESTINATIONS_TOPIC_NAME" default:"destination-messages"`
	KafkaDestinationsTopicPartitions int    `mapstructure:"KAFKA_DESTINATIONS_TOPIC_PARTITIONS" default:"4"`

	KafkaDestinationsDeadLetterTopicName string `mapstructure:"KAFKA_DESTINATIONS_DEAD_LETTER_TOPIC_NAME" default:"destination-messages-dead-letter"`

	// TopicManagerRefreshPeriodSec how often topic manager will check for new topics
	TopicManagerRefreshPeriodSec int `mapstructure:"TOPIC_MANAGER_REFRESH_PERIOD_SEC" default:"5"`

	// ProducerWaitForDeliveryMs For ProduceSync only is a timeout for producer to wait for delivery report.
	ProducerWaitForDeliveryMs int `mapstructure:"PRODUCER_WAIT_FOR_DELIVERY_MS" default:"1000"`

	// # BATCHING

	BatchRunnerPeriodSec        int `mapstructure:"BATCH_RUNNER_DEFAULT_PERIOD_SEC" default:"300"`
	BatchRunnerDefaultBatchSize int `mapstructure:"BATCH_RUNNER_DEFAULT_BATCH_SIZE" default:"10000"`
	// BatchRunnerWaitForMessagesSec when there are no more messages in the topic BatchRunner will wait for BatchRunnerWaitForMessagesSec seconds before sending a batch
	BatchRunnerWaitForMessagesSec int `mapstructure:"BATCH_RUNNER_WAIT_FOR_MESSAGES_SEC" default:"1"`

	// # ERROR RETRYING

	BatchRunnerRetryPeriodSec        int `mapstructure:"BATCH_RUNNER_DEFAULT_RETRY_PERIOD_SEC" default:"300"`
	BatchRunnerDefaultRetryBatchSize int `mapstructure:"BATCH_RUNNER_DEFAULT_RETRY_BATCH_SIZE" default:"100"`
	MessagesRetryCount               int `mapstructure:"MESSAGES_RETRY_COUNT" default:"5"`
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
	initViperVariables()
	viper.SetDefault("HTTP_PORT", utils.NvlString(os.Getenv("PORT"), "3042"))
	viper.SetDefault("REDIS_URL", os.Getenv("REDIS_URL"))
	viper.SetDefault("EVENTS_LOG_REDIS_URL", utils.NvlString(os.Getenv("BULKER_REDIS_URL"), os.Getenv("REDIS_URL")))
}

func initViperVariables() {
	elem := reflect.TypeOf(AppConfig{})
	fieldsCount := elem.NumField()
	for i := 0; i < fieldsCount; i++ {
		field := elem.Field(i)
		variable := field.Tag.Get("mapstructure")
		if variable != "" {
			defaultValue := field.Tag.Get("default")
			if defaultValue != "" {
				viper.SetDefault(variable, defaultValue)
			} else {
				_ = viper.BindEnv(variable)
			}
		}
	}
}

func InitAppConfig() (*AppConfig, error) {
	appConfig := AppConfig{}
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
	if appConfig.LogFormat == "json" {
		logging.SetJsonFormatter()
	}
	if strings.HasPrefix(appConfig.InstanceId, "env://") {
		env := appConfig.InstanceId[len("env://"):]
		appConfig.InstanceId = os.Getenv(env)
		if appConfig.InstanceId != "" {
			logging.Infof("Loaded instance id from env %s: %s", env, appConfig.InstanceId)
		}
	}
	if appConfig.InstanceId == "" {
		instId, _ := os.ReadFile(instanceIdFilePath)
		if len(instId) > 0 {
			appConfig.InstanceId = string(instId)
			logging.Infof("Loaded instance id from file: %s", appConfig.InstanceId)
		} else {
			appConfig.InstanceId = uuid.New()
			logging.Infof("Generated instance id: %s", appConfig.InstanceId)
			_ = os.MkdirAll(filepath.Dir(instanceIdFilePath), 0755)
			err = os.WriteFile(instanceIdFilePath, []byte(appConfig.InstanceId), 0644)
			if err != nil {
				logging.Errorf("error persisting instance id file: %s", err)
			}
		}
	}
	appConfig.GlobalHashSecrets = strings.Split(appConfig.GlobalHashSecret, ",")
	return &appConfig, nil
}

// GetKafkaConfig returns kafka config
func (ac *AppConfig) GetKafkaConfig() *kafka.ConfigMap {
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
			panic(fmt.Errorf("error parsing Kafka SASL config: %w", err))
		}
		for k, v := range sasl {
			_ = kafkaConfig.SetKey("sasl."+k, v)
		}
	}

	return kafkaConfig
}
