package main

import (
	"os"

	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/jitsucom/bulker/kafkabase"
	"github.com/spf13/viper"
)

type Config struct {
	// # BASE CONFIG - base setting for jitsu apps
	appbase.Config `mapstructure:",squash"`
	// # KAFKA CONFIG - base kafka setting
	kafkabase.KafkaConfig `mapstructure:",squash"`

	// # REPOSITORY CONFIG - settings for loading streams from repository
	RepositoryConfig `mapstructure:",squash"`

	// Cache dir for repository data
	CacheDir string `mapstructure:"CACHE_DIR"`
}

func init() {
	viper.SetDefault("HTTP_PORT", utils.NvlString(os.Getenv("PORT"), "3049"))
}

func (c *Config) PostInit(settings *appbase.AppSettings) error {
	if err := c.Config.PostInit(settings); err != nil {
		return err
	}
	return nil
}
