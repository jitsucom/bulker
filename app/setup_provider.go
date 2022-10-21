package app

import (
	"fmt"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/bulker"
	"github.com/mitchellh/mapstructure"
	"gopkg.in/yaml.v3"
	"reflect"
)

const destinationsKey = "destinations"

type DestinationConfig struct {
	WorkspaceId   string `mapstructure:"workspace_id"`
	BatchSize     int    `mapstructure:"batch_size"`
	bulker.Config `mapstructure:",squash"`
	StreamConfig  bulker.StreamConfig `mapstructure:",squash"`
}

func (dc *DestinationConfig) Id() string {
	return fmt.Sprintf("%s_%s", dc.WorkspaceId, dc.Config.Id)
}

type SetupProvider interface {
	GetDestinationConfigs() []*DestinationConfig
	GetDestinationConfig(id string) *DestinationConfig
	GetValue(key string) any
	Equals(other SetupProvider) bool
}

type YamlSetupProvider struct {
	config       map[string]any
	destinations map[string]*DestinationConfig
}

func NewYamlSetupProvider(data []byte) (*YamlSetupProvider, error) {
	cfg := make(map[string]any)
	err := yaml.Unmarshal(data, cfg)
	if err != nil {
		return nil, err
	}
	y := &YamlSetupProvider{config: cfg}
	err = y.init()
	if err != nil {
		return nil, err
	}
	return y, nil
}

func (ycp *YamlSetupProvider) init() error {
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

func (ycp *YamlSetupProvider) GetDestinationConfigs() []*DestinationConfig {
	results := make([]*DestinationConfig, len(ycp.destinations))
	i := 0
	for _, destination := range ycp.destinations {
		results[i] = destination
		i++
	}
	return results
}

func (ycp *YamlSetupProvider) GetDestinationConfig(id string) *DestinationConfig {
	return ycp.destinations[id]
}

func (ycp *YamlSetupProvider) GetValue(key string) any {
	return ycp.config[key]
}

func (ycp *YamlSetupProvider) Equals(other SetupProvider) bool {
	o, ok := other.(*YamlSetupProvider)
	if !ok {
		return false
	}
	return reflect.DeepEqual(ycp.config, o.config)
}
