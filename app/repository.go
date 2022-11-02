package app

import (
	"fmt"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/bulker"
	"sync/atomic"
)

type Repository struct {
	configurationSource ConfigurationSource
	repository          atomic.Pointer[repositoryInternal]
}

func (r *Repository) GetDestination(id string) *Destination {
	return r.repository.Load().GetDestination(id)
}

func (r *Repository) GetDestinations() []*Destination {
	return r.repository.Load().GetDestinations()
}

func (r *Repository) init() error {
	internal := &repositoryInternal{destinations: make(map[string]*Destination)}
	err := internal.init(r.configurationSource)
	if err != nil {
		return err
	}
	//Cleanup
	//TODO: detect changes and close only changed destinations
	oldInternal := r.repository.Swap(internal)
	if oldInternal != nil {
		for id, destination := range oldInternal.destinations {
			logging.Infof("[%s] closing destination", id)
			_ = destination.Close()
		}
	}
	return nil
}

func (r *Repository) changeListener() {
	for range r.configurationSource.ChangesChannel() {
		err := r.init()
		if err != nil {
			logging.Errorf("[repository] failed to reload repository: %v", err)
		}
	}
	logging.Infof("[repository] change listener stopped.")
}

// Close Repository
func (r *Repository) Close() error {
	return nil
}

type repositoryInternal struct {
	destinations map[string]*Destination
}

func NewRepository(config *AppConfig, configurationSource ConfigurationSource) (*Repository, error) {
	r := Repository{configurationSource: configurationSource}
	err := r.init()
	if err != nil {
		return nil, err
	}
	go r.changeListener()
	return &r, nil
}

func (r *repositoryInternal) init(configurationSource ConfigurationSource) error {
	logging.Debugf("[repository] Initializing repository")
	for _, cfg := range configurationSource.GetDestinationConfigs() {
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
		logging.Infof("[%s] destination initialized", cfg.Id())
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

type Destination struct {
	//TODO: keep consumers and cron tasks ids here
	config        *DestinationConfig
	bulker        bulker.Bulker
	streamOptions []bulker.StreamOption
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
func (d *Destination) Close() error {
	return d.bulker.Close()
}
