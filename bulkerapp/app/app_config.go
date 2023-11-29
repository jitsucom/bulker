package app

import (
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/jitsucom/bulker/kafkabase"
	"github.com/spf13/viper"
	"os"
	"strings"
)

// Config is a struct for bulker app configuration
// It is loaded from `bulker.env` config file or environment variables.
//
// Environment variables requires prefix `BULKER_`
type Config struct {
	appbase.Config        `mapstructure:",squash"`
	kafkabase.KafkaConfig `mapstructure:",squash"`

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

	// TopicManagerRefreshPeriodSec how often topic manager will check for new topics
	TopicManagerRefreshPeriodSec int `mapstructure:"TOPIC_MANAGER_REFRESH_PERIOD_SEC" default:"5"`

	// # BATCHING

	BatchRunnerPeriodSec          int `mapstructure:"BATCH_RUNNER_DEFAULT_PERIOD_SEC" default:"300"`
	BatchRunnerDefaultBatchSize   int `mapstructure:"BATCH_RUNNER_DEFAULT_BATCH_SIZE" default:"10000"`
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

	// # EVENTS REDIS LOGGING

	EventsLogRedisURL string `mapstructure:"EVENTS_LOG_REDIS_URL"`
	EventsLogMaxSize  int    `mapstructure:"EVENTS_LOG_MAX_SIZE" default:"1000"`

	// # METRICS

	MetricsPort             int    `mapstructure:"METRICS_PORT" default:"9091"`
	MetricsRelayDestination string `mapstructure:"METRICS_RELAY_DESTINATION"`
	MetricsRelayPeriodSec   int    `mapstructure:"METRICS_RELAY_PERIOD_SEC" default:"60"`

	BackupLogDir         string `mapstructure:"BACKUP_LOG_DIR"`
	BackupLogTTL         int    `mapstructure:"BACKUP_LOG_TTL_DAYS" default:"7"`
	BackupLogRotateHours int    `mapstructure:"BACKUP_LOG_ROTATE_HOURS" default:"24"`
	BackupLogMaxSizeMb   int    `mapstructure:"BACKUP_LOG_MAX_SIZE_MB" default:"100"`

	// # GRACEFUL SHUTDOWN
	//Timeout that give running batch tasks time to finish during shutdown.
	ShutdownTimeoutSec int `mapstructure:"SHUTDOWN_TIMEOUT_SEC" default:"10"`
	//Extra delay may be needed. E.g. for metric scrapper to scrape final metrics. So http server will stay active for an extra period.
	ShutdownExtraDelay int `mapstructure:"SHUTDOWN_EXTRA_DELAY_SEC" default:"5"`
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
