package app

import (
	"fmt"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/bulker"
	"sync/atomic"
)

type Repository struct {
	repository atomic.Pointer[repositoryInternal]
}

func (r *Repository) GetDestination(id string) *Destination {
	return r.repository.Load().GetDestination(id)
}

func (r *Repository) GetDestinations() []*Destination {
	return r.repository.Load().GetDestinations()
}

type repositoryInternal struct {
	configurationSource ConfigurationSource
	destinations        map[string]*Destination
}

type Destination struct {
	config        *DestinationConfig
	bulker        bulker.Bulker
	streamOptions []bulker.StreamOption
}

func NewRepository(config *AppConfig, configurationSource ConfigurationSource) (*Repository, error) {
	var repository atomic.Pointer[repositoryInternal]
	internal := &repositoryInternal{configurationSource: configurationSource, destinations: make(map[string]*Destination)}
	err := internal.init()
	if err != nil {
		return nil, err
	}
	repository.Store(internal)
	return &Repository{repository: repository}, nil
}

func (r *repositoryInternal) init() error {
	for _, cfg := range r.configurationSource.GetDestinationConfigs() {
		bulkerInstance, err := bulker.CreateBulker(cfg.Config)
		if err != nil {
			logging.Errorf("[%s] failed to init destination: %v", cfg.Id(), err)
			continue
		}
		options := make([]bulker.StreamOption, 0, len(cfg.StreamConfig.Options))
		for name, serializedOption := range cfg.StreamConfig.Options {
			opt, err := bulker.ParseOption(name, serializedOption)
			if err != nil {
				return err
			}
			options = append(options, opt)
		}
		r.destinations[cfg.Id()] = &Destination{config: cfg, bulker: bulkerInstance, streamOptions: options}
	}
	return nil
}

func (r *repositoryInternal) GetDestination(id string) *Destination {
	return r.destinations[id]
}

func (r *repositoryInternal) GetDestinations() []*Destination {
	destinations := make([]*Destination, 0, len(r.destinations))
	for _, destination := range r.destinations {
		destinations = append(destinations, destination)
	}
	return destinations
}

// TopicId generates topic id for Destination
func (d *Destination) TopicId(tableName string) string {
	if tableName == "" {
		tableName = d.config.StreamConfig.TableName
	}
	return fmt.Sprintf("incoming.destinationId.%s.mode.%s.tableName.%s", d.Id(), d.config.StreamConfig.BulkMode, tableName)
}

// Id returns destination id
func (d *Destination) Id() string {
	return d.config.Id()
}
