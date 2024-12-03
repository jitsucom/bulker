package main

import (
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/spf13/viper"
	"os"
)

type Config struct {
	appbase.Config `mapstructure:",squash"`

	//Cache dir for repositories data
	CacheDir string `mapstructure:"CACHE_DIR"`

	ScriptOrigin string `mapstructure:"SCRIPT_ORIGIN" default:"https://cdn.jsdelivr.net/npm/@jitsu/js@latest/dist/web/p.js.txt"`

	RepositoryBaseURL          string `mapstructure:"REPOSITORY_BASE_URL"`
	RepositoryAuthToken        string `mapstructure:"REPOSITORY_AUTH_TOKEN"`
	RepositoryRefreshPeriodSec int    `mapstructure:"REPOSITORY_REFRESH_PERIOD_SEC" default:"5"`
	Repositories               string `mapstructure:"REPOSITORIES" default:"streams-with-destinations,workspaces-with-profiles,functions,rotor-connections,bulker-connections"`
}

func init() {
	viper.SetDefault("HTTP_PORT", utils.NvlString(os.Getenv("PORT"), "3059"))
}

func (c *Config) PostInit(settings *appbase.AppSettings) error {
	return c.Config.PostInit(settings)
}
