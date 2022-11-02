package app

import (
	"fmt"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/bulker"
	"github.com/mitchellh/mapstructure"
	"gopkg.in/yaml.v3"
	"io"
	"os"
	"reflect"
	"strings"
)

const destinationsKey = "destinations"

type DestinationConfig struct {
	BatchSize      int `mapstructure:"batch_size" json:"batch_size"`
	BatchPeriodSec int `mapstructure:"batch_period_sec" json:"batch_period_sec"`

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
	GetValue(key string) any
	ChangesChannel() <-chan bool
	//Equals(other ConfigurationSource) bool
}

func InitConfigurationSource(config *AppConfig) (ConfigurationSource, error) {
	if config.ConfigSource == "" {
		return nil, fmt.Errorf("❗️it is required to set Configuration Source using BULKER_CONFIG_SOURCE environement variable")
	}

	if strings.HasPrefix(config.ConfigSource, "file://") || !strings.Contains(config.ConfigSource, "://") {
		filePath := strings.TrimPrefix(config.ConfigSource, "file://")
		yamlConfig, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("❗️error reading yaml config file: %s: %w", filePath, err)
		}
		cfgSrc, err := NewYamlConfigurationSource(yamlConfig)
		if err != nil {
			return nil, fmt.Errorf("❗error creating yaml configuration source from config file: %s: %v", filePath, err)
		}
		return cfgSrc, nil
	} else if strings.HasPrefix(config.ConfigSource, "redis://") {
		redisConfigSource, err := NewRedisConfigurationSource(config.ConfigSource)
		if err != nil {
			return nil, fmt.Errorf("❗️error while init redis configuration source: %s: %w", config.ConfigSource, err)
		}
		return redisConfigSource, nil
	} else {
		return nil, fmt.Errorf("❗unsupported configuration source: %s", config.ConfigSource)
	}
}

type YamlConfigurationSource struct {
	changesChan chan bool

	config       map[string]any
	destinations map[string]*DestinationConfig
}

func NewYamlConfigurationSource(data []byte) (*YamlConfigurationSource, error) {
	cfg := make(map[string]any)
	err := yaml.Unmarshal(data, cfg)
	if err != nil {
		return nil, err
	}
	y := &YamlConfigurationSource{
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
		return fmt.Errorf("failed to parse destinations. Expected map[string]any got: %T", destinationsRaw)
	}
	results := make(map[string]*DestinationConfig, len(destinations))
	for id, destination := range destinations {
		cfg := &DestinationConfig{}
		err := mapstructure.Decode(destination, cfg)
		if err != nil {
			logging.Errorf("Failed to parse destination config %s: %v:\n%s", id, err, destination)
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

func (ycp *YamlConfigurationSource) Equals(other ConfigurationSource) bool {
	o, ok := other.(*YamlConfigurationSource)
	if !ok {
		return false
	}
	return reflect.DeepEqual(ycp.config, o.config)
}

func (ycp *YamlConfigurationSource) ChangesChannel() <-chan bool {
	return ycp.changesChan
}

func (ycp *YamlConfigurationSource) Close() error {
	close(ycp.changesChan)
	return nil
}
