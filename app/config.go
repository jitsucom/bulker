package app

import (
	"github.com/spf13/viper"
	"os"
)

const DummyClusterID = "bulker_cluster"
const DummyInstanceID = "bulker_instance"

type Config struct {
	ClusterId  string `mapstructure:"CLUSTER_ID"`
	InstanceId string `mapstructure:"INSTANCE_ID"`

	SetupProvider string `mapstructure:"SETUP_PROVIDER"`

	KafkaBootstrapServers       string `mapstructure:"KAFKA_BOOTSTRAP_SERVERS"`
	KafkaTopicRetentionMs       int    `mapstructure:"KAFKA_TOPIC_RETENTION_MS"`
	KafkaTopicPartitionsCount   int    `mapstructure:"KAFKA_TOPIC_PARTITIONS_COUNT"`
	KafkaTopicReplicationFactor int    `mapstructure:"KAFKA_TOPIC_REPLICATION_FACTOR"`
	KafkaAdminMetadataTimeoutMs int    `mapstructure:"KAFKA_ADMIN_METADATA_TIMEOUT_MS"`

	TopicManagerRefreshPeriodSec int `mapstructure:"TOPIC_MANAGER_REFRESH_PERIOD_SEC"`

	BatchRunnerPeriodSec          int `mapstructure:"BATCH_RUNNER_PERIOD_SEC"`
	BatchRunnerDefaultBatchSize   int `mapstructure:"BATCH_RUNNER_DEFAULT_BATCH_SIZE"`
	BatchRunnerWaitForMessagesSec int `mapstructure:"BATCH_RUNNER_WAIT_FOR_MESSAGES_SEC"`
}

func init() {
	viper.SetDefault("CLUSTER_ID", DummyClusterID)
	instanceId, err := os.Hostname()
	if err != nil {
		instanceId = DummyInstanceID
	}
	viper.SetDefault("INSTANCE_ID", instanceId)

	_ = viper.BindEnv("SETUP_PROVIDER")

	viper.SetDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
	viper.SetDefault("KAFKA_TOPIC_PARTITIONS_COUNT", 16)
	viper.SetDefault("KAFKA_TOPIC_REPLICATION_FACTOR", 1)
	viper.SetDefault("KAFKA_TOPIC_RETENTION_MS", 604800000) // 7 days. Default Kafka value
	viper.SetDefault("KAFKA_ADMIN_METADATA_TIMEOUT_MS", 1000)

	viper.SetDefault("TOPIC_MANAGER_REFRESH_PERIOD_SEC", 5)

	viper.SetDefault("BATCH_RUNNER_PERIOD_SEC", 300)
	viper.SetDefault("BATCH_RUNNER_DEFAULT_BATCH_SIZE", 1000)
	viper.SetDefault("BATCH_RUNNER_WAIT_FOR_MESSAGES_SEC", 1)
}
