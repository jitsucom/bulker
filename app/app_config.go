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

type AppConfig struct {
	// TODO: persist on disk
	InstanceId string `mapstructure:"INSTANCE_ID"`

	HTTPPort int `mapstructure:"HTTP_PORT"`

	AuthTokens   string `mapstructure:"AUTH_TOKENS"`
	TokenSecrets string `mapstructure:"TOKEN_SECRET"`

	LogFormat string `mapstructure:"LOG_FORMAT"`

	GlobalHashSecret  string `mapstructure:"GLOBAL_HASH_SECRET" default:"dea42a58-acf4-45af-85bb-e77e94bd5025"`
	GlobalHashSecrets []string

	ConfigSource string `mapstructure:"CONFIG_SOURCE"`

	RedisTLSCA string `mapstructure:"REDIS_TLS_CA"`

	KafkaBootstrapServers string `mapstructure:"KAFKA_BOOTSTRAP_SERVERS"`
	KafkaSSL              bool   `mapstructure:"KAFKA_SSL" default:"false"`
	KafkaSSLSkipVerify    bool   `mapstructure:"KAFKA_SSL_SKIP_VERIFY" default:"false"`

	KafkaSASL string `mapstructure:"KAFKA_SASL"`

	KafkaTopicRetentionHours       int `mapstructure:"KAFKA_TOPIC_RETENTION_HOURS" default:"168"`
	KafkaFailedTopicRetentionHours int `mapstructure:"KAFKA_FAILED_TOPIC_RETENTION_HOURS" default:"168"`

	KafkaTopicReplicationFactor              int    `mapstructure:"KAFKA_TOPIC_REPLICATION_FACTOR"`
	KafkaAdminMetadataTimeoutMs              int    `mapstructure:"KAFKA_ADMIN_METADATA_TIMEOUT_MS" default:"1000"`
	KafkaConsumerPartitionsAssigmentStrategy string `mapstructure:"KAFKA_CONSUMER_PARTITIONS_ASSIGMENT_STRATEGY" default:"cooperative-sticky"`
	//TODO: max.poll.interval.ms

	KafkaDestinationsTopicName string `mapstructure:"KAFKA_DESTINATIONS_TOPIC_NAME" default:"destination-messages"`

	TopicManagerRefreshPeriodSec int `mapstructure:"TOPIC_MANAGER_REFRESH_PERIOD_SEC" default:"5"`

	ProducerWaitForDeliveryMs int `mapstructure:"PRODUCER_WAIT_FOR_DELIVERY_MS" default:"1000"`

	BatchRunnerPeriodSec          int `mapstructure:"BATCH_RUNNER_DEFAULT_PERIOD_SEC" default:"300"`
	BatchRunnerDefaultBatchSize   int `mapstructure:"BATCH_RUNNER_DEFAULT_BATCH_SIZE" default:"10000"`
	BatchRunnerWaitForMessagesSec int `mapstructure:"BATCH_RUNNER_WAIT_FOR_MESSAGES_SEC" default:"1"`

	// Redis URL that will be used by default by services that need Redis
	RedisURL          string `mapstructure:"REDIS_URL"`
	EventsLogRedisURL string `mapstructure:"EVENTS_LOG_REDIS_URL"`
	EventsLogMaxSize  int    `mapstructure:"EVENTS_LOG_MAX_SIZE" default:"1000"`

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
	if appConfig.InstanceId == "" {
		instId, _ := os.ReadFile(instanceIdFilePath)
		if len(instId) > 0 {
			appConfig.InstanceId = string(instId)
			logging.Infof("Loaded instance id from file: %s", appConfig.InstanceId)
		} else {
			appConfig.InstanceId = uuid.New()
			_ = os.MkdirAll(filepath.Dir(instanceIdFilePath), 0755)
			err = os.WriteFile(instanceIdFilePath, []byte(appConfig.InstanceId), 0644)
			if err != nil {
				logging.Errorf("error persisting instance id file: %s", err)
			}
		}
	} else if strings.HasPrefix(appConfig.InstanceId, "env://") {
		env := appConfig.InstanceId[len("env://"):]
		appConfig.InstanceId = os.Getenv(env)
		logging.Infof("Loading instance id from env %s: %s", env, appConfig.InstanceId)
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
