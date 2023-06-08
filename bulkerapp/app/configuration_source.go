package app

import (
	"fmt"
	"github.com/hjson/hjson-go/v4"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/jitsucom/bulker/jitsubase/objects"
	"github.com/mitchellh/mapstructure"
	"gopkg.in/yaml.v3"
	"io"
	"os"
	"strings"
	"time"
)

const destinationsKey = "destinations"
const defaultEnvDestinationPrefix = "BULKER_DESTINATION"

type DestinationConfig struct {
	UpdatedAt           time.Time `mapstructure:"updatedAt" json:"updatedAt"`
	UsesBulker          bool      `mapstructure:"usesBulker" json:"usesBulker"`
	bulker.Config       `mapstructure:",squash"`
	bulker.StreamConfig `mapstructure:",squash"`
}

func (dc *DestinationConfig) Id() string {
	return dc.Config.Id
}

type ConfigurationSource interface {
	io.Closer
	GetDestinationConfigs() []*DestinationConfig
	GetDestinationConfig(id string) *DestinationConfig
	ChangesChannel() <-chan bool
	//Equals(other ConfigurationSource) bool
}

func InitConfigurationSource(config *AppConfig) (ConfigurationSource, error) {
	cfgSource := config.ConfigSource
	if cfgSource == "" {
		logging.Infof("BULKER_CONFIG_SOURCE is not set. Using environment variables configuration source with prefix: %s", defaultEnvDestinationPrefix)
		return NewEnvConfigurationSource(defaultEnvDestinationPrefix), nil
	} else if cfgSource == "redis" {
		config.ConfigSource = config.RedisURL
		cfgSource = config.RedisURL
	}

	var configurationSource ConfigurationSource
	if strings.HasPrefix(cfgSource, "file://") || !strings.Contains(cfgSource, "://") {
		filePath := strings.TrimPrefix(cfgSource, "file://")
		yamlConfig, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("❗️error reading yaml config file: %s: %w", filePath, err)
		}
		configurationSource, err = NewYamlConfigurationSource(yamlConfig)
		if err != nil {
			return nil, fmt.Errorf("❗error creating yaml configuration source from config file: %s: %v", filePath, err)
		}
	} else if strings.HasPrefix(cfgSource, "redis://") || strings.HasPrefix(cfgSource, "rediss://") {
		var err error
		configurationSource, err = NewRedisConfigurationSource(config)
		if err != nil {
			return nil, fmt.Errorf("❗️error while init redis configuration source: %s: %w", cfgSource, err)
		}
	} else if strings.HasPrefix(cfgSource, "env://BULKER_DESTINATION") {
		envPrefix := strings.TrimPrefix(cfgSource, "env://")
		configurationSource = NewEnvConfigurationSource(envPrefix)
	} else {
		return nil, fmt.Errorf("❗unsupported configuration source: %s", cfgSource)
	}

	internalDestinationsSource := NewEnvConfigurationSource("BULKER_INTERNAL")
	return NewMultiConfigurationSource([]ConfigurationSource{internalDestinationsSource, configurationSource}), nil
}

func initConfigurationSource(config *AppConfig) (ConfigurationSource, error) {
	cfgSource := config.ConfigSource
	if cfgSource == "" {
		logging.Infof("BULKER_CONFIG_SOURCE is not set. Using environment variables configuration source with prefix: %s", defaultEnvDestinationPrefix)
		return NewEnvConfigurationSource(defaultEnvDestinationPrefix), nil
	} else if cfgSource == "redis" {
		config.ConfigSource = config.RedisURL
		cfgSource = config.RedisURL
	}

	if strings.HasPrefix(cfgSource, "file://") || !strings.Contains(cfgSource, "://") {
		filePath := strings.TrimPrefix(cfgSource, "file://")
		yamlConfig, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("❗️error reading yaml config file: %s: %w", filePath, err)
		}
		cfgSrc, err := NewYamlConfigurationSource(yamlConfig)
		if err != nil {
			return nil, fmt.Errorf("❗error creating yaml configuration source from config file: %s: %v", filePath, err)
		}
		return cfgSrc, nil
	} else if strings.HasPrefix(cfgSource, "redis://") || strings.HasPrefix(cfgSource, "rediss://") {
		redisConfigSource, err := NewRedisConfigurationSource(config)
		if err != nil {
			return nil, fmt.Errorf("❗️error while init redis configuration source: %s: %w", cfgSource, err)
		}
		return redisConfigSource, nil
	} else if strings.HasPrefix(cfgSource, "env://BULKER_DESTINATION") {
		envPrefix := strings.TrimPrefix(cfgSource, "env://")
		return NewEnvConfigurationSource(envPrefix), nil
	} else {
		return nil, fmt.Errorf("❗unsupported configuration source: %s", cfgSource)
	}
}

type YamlConfigurationSource struct {
	objects.ServiceBase

	changesChan chan bool

	config       map[string]any
	destinations map[string]*DestinationConfig
}

func NewYamlConfigurationSource(data []byte) (*YamlConfigurationSource, error) {
	base := objects.NewServiceBase("yaml_configuration")
	cfg := make(map[string]any)
	err := yaml.Unmarshal(data, cfg)
	if err != nil {
		return nil, err
	}
	y := &YamlConfigurationSource{
		ServiceBase: base,
		changesChan: make(chan bool),
		config:      cfg,
	}
	err = y.init()
	if err != nil {
		return nil, err
	}
	return y, nil
}

func (ycp *YamlConfigurationSource) init() error {
	destinationsRaw, ok := ycp.config[destinationsKey]
	if !ok {
		return nil
	}
	destinations, ok := destinationsRaw.(map[string]any)
	if !ok {
		return ycp.NewError("failed to parse destinations. Expected map[string]any got: %T", destinationsRaw)
	}
	results := make(map[string]*DestinationConfig, len(destinations))
	for id, destination := range destinations {
		cfg := &DestinationConfig{}
		err := mapstructure.Decode(destination, cfg)
		if err != nil {
			ycp.Errorf("Failed to parse destination config %s: %v:\n%s", id, err, destination)
			continue
		}
		cfg.Config.Id = id
		//logging.Infof("Parsed destination config: %+v", cfg)
		results[id] = cfg
	}
	ycp.destinations = results
	return nil
}

func (ycp *YamlConfigurationSource) GetDestinationConfigs() []*DestinationConfig {
	results := make([]*DestinationConfig, len(ycp.destinations))
	i := 0
	for _, destination := range ycp.destinations {
		results[i] = destination
		i++
	}
	return results
}

func (ycp *YamlConfigurationSource) GetDestinationConfig(id string) *DestinationConfig {
	return ycp.destinations[id]
}

func (ycp *YamlConfigurationSource) GetValue(key string) any {
	return ycp.config[key]
}

//func (ycp *YamlConfigurationSource) Equals(other ConfigurationSource) bool {
//	o, ok := other.(*YamlConfigurationSource)
//	if !ok {
//		return false
//	}
//	return reflect.DeepEqual(ycp.config, o.config)
//}

func (ycp *YamlConfigurationSource) ChangesChannel() <-chan bool {
	return ycp.changesChan
}

func (ycp *YamlConfigurationSource) Close() error {
	close(ycp.changesChan)
	return nil
}

type EnvConfigurationSource struct {
	objects.ServiceBase

	changesChan chan bool

	destinations map[string]*DestinationConfig
}

func NewEnvConfigurationSource(prefix string) *EnvConfigurationSource {
	base := objects.NewServiceBase("env_configuration")
	results := make(map[string]*DestinationConfig)
	envs := os.Environ()
	for _, env := range envs {
		parts := strings.SplitN(env, "=", 2)
		if len(parts) != 2 {
			continue
		}
		id := parts[0]
		if !strings.HasPrefix(id, prefix) {
			continue
		}
		id = strings.TrimPrefix(id, prefix+"_")
		value := parts[1]
		cfg := &DestinationConfig{}
		err := hjson.Unmarshal([]byte(value), &cfg)
		if err != nil {
			base.Errorf("Failed to parse destination config %s: %v:\n%s", id, err, value)
			continue
		}
		cfg.Config.Id = id
		base.Debugf("parsed config for destination %s: %+v", id, cfg)
		results[id] = cfg
	}
	y := &EnvConfigurationSource{
		ServiceBase:  base,
		changesChan:  make(chan bool),
		destinations: results,
	}
	return y
}

func (ecs *EnvConfigurationSource) GetDestinationConfigs() []*DestinationConfig {
	results := make([]*DestinationConfig, len(ecs.destinations))
	i := 0
	for _, destination := range ecs.destinations {
		results[i] = destination
		i++
	}
	return results
}

func (ecs *EnvConfigurationSource) GetDestinationConfig(id string) *DestinationConfig {
	return ecs.destinations[id]
}

func (ecs *EnvConfigurationSource) GetValue(key string) any {
	return nil
}

func (ecs *EnvConfigurationSource) ChangesChannel() <-chan bool {
	return ecs.changesChan
}

func (ecs *EnvConfigurationSource) Close() error {
	close(ecs.changesChan)
	return nil
}
