package appbase

import (
	"fmt"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/jitsucom/bulker/jitsubase/uuid"
	"github.com/spf13/viper"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
)

type Context[C any] interface {
	InitContext(settings *AppSettings) error
	Shutdown() error
	Config() *C
	Server() *http.Server
}

type Config struct {
	// InstanceId ID of bulker instance. It is used for identifying Kafka consumers.
	// If is not set, instance id will be generated and persisted to disk (~/.{appName}/instance_id) and reused on next restart.
	// Default: random uuid
	InstanceId string `mapstructure:"INSTANCE_ID"`

	// HTTPPort port for bulker http server.
	HTTPPort int `mapstructure:"HTTP_PORT"`

	// # AUTH

	// AuthTokens A list of auth tokens that authorizes user in HTTP interface separated by comma. Each token can be either:
	// - `${token}` un-encrypted token value
	// - `${salt}.${hash}` hashed token.` ${salt}` should be random string. Hash is `base64(sha512($token + $salt + TokenSecrets)`.
	// - Token is `[0-9a-zA-Z_\-]` (only letters, digits, underscore and dash)
	AuthTokens string `mapstructure:"AUTH_TOKENS"`
	// See AuthTokens
	TokenSecrets string `mapstructure:"TOKEN_SECRET"`

	// # LOGGING

	// LogFormat log format. Can be `text` or `json`. Default: `text`
	LogFormat string `mapstructure:"LOG_FORMAT"`
}

func (c *Config) PostInit(settings *AppSettings) error {
	if c.LogFormat == "json" {
		logging.SetJsonFormatter()
	}
	if strings.HasPrefix(c.InstanceId, "env://") {
		env := c.InstanceId[len("env://"):]
		c.InstanceId = os.Getenv(env)
		if c.InstanceId != "" {
			logging.Infof("Loaded instance id from env %s: %s", env, c.InstanceId)
		}
	}
	if c.InstanceId == "" {
		instanceIdFilePath := fmt.Sprintf("~/.%s/instance_id", settings.ConfigName)
		instId, _ := os.ReadFile(instanceIdFilePath)
		if len(instId) > 0 {
			c.InstanceId = string(instId)
			logging.Infof("Loaded instance id from file: %s", c.InstanceId)
		} else {
			c.InstanceId = uuid.New()
			logging.Infof("Generated instance id: %s", c.InstanceId)
			_ = os.MkdirAll(filepath.Dir(instanceIdFilePath), 0755)
			err := os.WriteFile(instanceIdFilePath, []byte(c.InstanceId), 0644)
			if err != nil {
				logging.Errorf("error persisting instance id file: %s", err)
			}
		}
	}

	return nil
}

type InstanceConfig interface {
	PostInit(settings *AppSettings) error
}

func initViperVariables[C InstanceConfig](appConfig C) {
	elem := reflect.ValueOf(appConfig).Elem()
	tp := elem.Type()
	fieldsCount := tp.NumField()
	for i := 0; i < fieldsCount; i++ {
		field := tp.Field(i)
		if field.Type == reflect.TypeOf(Config{}) {
			// init nested config variables
			initViperVariables(elem.Field(i).Addr().Interface().(*Config))
		}
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

func InitAppConfig[C InstanceConfig](appConfig C, settings *AppSettings) error {
	fmt.Println("InitAppConfig")
	configPath := settings.ConfigPath
	if configPath == "" {
		configPath = "."
	}
	initViperVariables(appConfig)
	viper.AddConfigPath(configPath)
	viper.SetConfigName(settings.ConfigName)
	viper.SetConfigType(settings.ConfigType)
	viper.SetEnvPrefix(settings.EnvPrefix)
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		//it is ok to not have config file
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return fmt.Errorf("❗error reading config file: %s", err)
		}
	}
	err := viper.Unmarshal(&appConfig)
	if err != nil {
		return fmt.Errorf("❗error unmarshalling config: %s", err)
	}
	if err = appConfig.PostInit(settings); err != nil {
		return fmt.Errorf("❗error initializing config: %s", err)
	}
	return nil
}

type AppSettings struct {
	Name, ConfigPath, ConfigName, ConfigType, EnvPrefix string
}

func (a *AppSettings) EnvPrefixWithUnderscore() string {
	if a.EnvPrefix == "" {
		return ""
	}
	return a.EnvPrefix + "_"
}

const SIG_SHUTDOWN_FOR_TESTS = syscall.Signal(0x42)

type App[C any] struct {
	appContext  Context[C]
	settings    *AppSettings
	exitChannel chan os.Signal
}

func NewApp[C any](appContext Context[C], appSettings *AppSettings) *App[C] {
	logging.SetTextFormatter()
	err := appContext.InitContext(appSettings)
	if err != nil {
		panic(fmt.Errorf("failed to start app: %w", err))
	}
	return &App[C]{
		appContext:  appContext,
		settings:    appSettings,
		exitChannel: make(chan os.Signal, 1),
	}
}

func (a *App[C]) Run() {

	signal.Notify(a.exitChannel, os.Interrupt, os.Kill, syscall.SIGTERM)

	go func() {
		sig := <-a.exitChannel
		logging.Infof("Received signal: %s. Shutting down...", sig)
		err := a.appContext.Shutdown()
		if err != nil {
			logging.Errorf("error during shutdown: %s", err)
		}
		if sig != SIG_SHUTDOWN_FOR_TESTS {
			// we don't want to exit when running tests
			os.Exit(0)
		}
	}()
	server := a.appContext.Server()
	if server != nil {
		logging.Infof("Starting http server on %s", server.Addr)
		logging.Info(server.ListenAndServe())
	}

}

func (a *App[C]) Exit(signal os.Signal) {
	logging.Infof("App Triggered Exit...")
	a.exitChannel <- signal
}
