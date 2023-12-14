package main

import (
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/jitsucom/bulker/kafkabase"
	"github.com/spf13/viper"
	"os"
	"strings"
)

type Config struct {
	appbase.Config        `mapstructure:",squash"`
	kafkabase.KafkaConfig `mapstructure:",squash"`

	DatabaseURL string `mapstructure:"DATABASE_URL"`

	DataDomain string `mapstructure:"DATA_DOMAIN"`

	// For ingest endpoint only
	GlobalHashSecret string `mapstructure:"GLOBAL_HASH_SECRET" default:"dea42a58-acf4-45af-85bb-e77e94bd5025"`
	// For ingest endpoint only
	GlobalHashSecrets []string

	BackupLogDir         string `mapstructure:"BACKUP_LOG_DIR"`
	BackupLogTTL         int    `mapstructure:"BACKUP_LOG_TTL_DAYS" default:"7"`
	BackupLogRotateHours int    `mapstructure:"BACKUP_LOG_ROTATE_HOURS" default:"24"`
	BackupLogMaxSizeMb   int    `mapstructure:"BACKUP_LOG_MAX_SIZE_MB" default:"100"`

	// # EVENTS REDIS LOGGING

	RedisURL         string `mapstructure:"REDIS_URL"`
	RedisTLSCA       string `mapstructure:"REDIS_TLS_CA"`
	EventsLogMaxSize int    `mapstructure:"EVENTS_LOG_MAX_SIZE" default:"1000"`

	RepositoryRefreshPeriodSec int `mapstructure:"REPOSITORY_REFRESH_PERIOD_SEC" default:"5"`

	RotorURL                 string `mapstructure:"ROTOR_URL"`
	DeviceFunctionsTimeoutMs int    `mapstructure:"DEVICE_FUNCTIONS_TIMEOUT_MS" default:"200"`

	MetricsPort int `mapstructure:"METRICS_PORT" default:"9091"`

	// # GRACEFUL SHUTDOWN
	//Timeout that give running batch tasks time to finish during shutdown.
	ShutdownTimeoutSec int `mapstructure:"SHUTDOWN_TIMEOUT_SEC" default:"10"`
	//Extra delay may be needed. E.g. for metric scrapper to scrape final metrics. So http server will stay active for an extra period.
	ShutdownExtraDelay int `mapstructure:"SHUTDOWN_EXTRA_DELAY_SEC" default:"5"`
}

func init() {
	viper.SetDefault("HTTP_PORT", utils.NvlString(os.Getenv("PORT"), "3049"))
}

func (ac *Config) PostInit(settings *appbase.AppSettings) error {
	err := ac.Config.PostInit(settings)
	if err != nil {
		return err
	}
	ac.GlobalHashSecrets = strings.Split(ac.GlobalHashSecret, ",")
	return nil
}
