package app

import (
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/objects"
	"github.com/jitsucom/bulker/bulker"
	"sync"
)

type RepositoryChange struct {
	AddedDestinations     []*Destination
	ChangedDestinations   []*Destination
	RemovedDestinationIds []string
}

type Repository struct {
	objects.ServiceBase
	sync.Mutex
	configurationSource ConfigurationSource
	repository          *repositoryInternal

	changesChan chan RepositoryChange
}

func (r *Repository) GetDestination(id string) *Destination {
	r.Lock()
	defer r.Unlock()
	return r.repository.GetDestination(id)
}

// LeaseDestination destination. destination cannot be closed while at lease one service is using it (e.g. batch consumer)
func (r *Repository) LeaseDestination(id string) *Destination {
	r.Lock()
	defer r.Unlock()
	return r.repository.LeaseDestination(id)
}

func (r *Repository) GetDestinations() []*Destination {
	r.Lock()
	defer r.Unlock()
	return r.repository.GetDestinations()
}

func (r *Repository) init() error {
	base := objects.NewServiceBase("repository")
	internal := &repositoryInternal{
		ServiceBase:  base,
		destinations: make(map[string]*Destination),
	}
	err := internal.init(r.configurationSource)
	if err != nil {
		return err
	}
	//Cleanup
	//TODO: detect changes and close only changed destinations
	r.Lock()
	oldInternal := r.repository
	r.repository = internal
	if oldInternal != nil {
		for id, destination := range oldInternal.destinations {
			r.Infof("retiring destination %s. Ver: %s", id, destination.config.UpdatedAt)
			oldInternal.retireDestination(destination)
		}
	}
	r.Unlock()
	repositoryChange := RepositoryChange{}
	var oldDestinations map[string]*Destination
	if oldInternal != nil {
		oldDestinations = oldInternal.destinations
	}
	for id, _ := range oldDestinations {
		newDst, ok := internal.destinations[id]
		if !ok {
			repositoryChange.RemovedDestinationIds = append(repositoryChange.RemovedDestinationIds, id)
		} else {
			//TODO: track changes for each destination individually
			repositoryChange.ChangedDestinations = append(repositoryChange.ChangedDestinations, newDst)
		}
	}
	for id, dst := range internal.destinations {
		_, ok := oldDestinations[id]
		if !ok {
			repositoryChange.AddedDestinations = append(repositoryChange.AddedDestinations, dst)
		}
	}
	select {
	case r.changesChan <- repositoryChange:
	default:
	}
	return nil
}

func (r *Repository) ChangesChannel() <-chan RepositoryChange {
	return r.changesChan
}

func (r *Repository) changeListener() {
	for range r.configurationSource.ChangesChannel() {
		err := r.init()
		if err != nil {
			r.Errorf("failed to reload repository: %w", err)
		}
	}
	r.Infof("change listener stopped.")
}

// Close Repository
func (r *Repository) Close() error {
	return nil
}

type repositoryInternal struct {
	objects.ServiceBase
	sync.Mutex
	destinations map[string]*Destination
}

func NewRepository(config *AppConfig, configurationSource ConfigurationSource) (*Repository, error) {
	base := objects.NewServiceBase("repository")
	r := Repository{
		ServiceBase:         base,
		configurationSource: configurationSource,
		changesChan:         make(chan RepositoryChange, 10),
	}
	err := r.init()
	if err != nil {
		return nil, err
	}
	go r.changeListener()
	return &r, nil
}

func (r *repositoryInternal) init(configurationSource ConfigurationSource) error {
	r.Debugf("Initializing repository")
	for _, cfg := range configurationSource.GetDestinationConfigs() {
		bulkerInstance, err := bulker.CreateBulker(cfg.Config)
		if err != nil {
			r.Errorf("failed to init destination %s: %v", cfg.Id(), err)
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
		r.destinations[cfg.Id()] = &Destination{config: cfg, bulker: bulkerInstance, streamOptions: options, owner: r}
		r.Infof("destination %s initialized. Ver: %s", cfg.Id(), cfg.UpdatedAt)
	}
	return nil
}

func (r *repositoryInternal) GetDestination(id string) *Destination {
	return r.destinations[id]
}

func (r *repositoryInternal) LeaseDestination(id string) *Destination {
	//TODO: move locks to destination ??
	r.Lock()
	defer r.Unlock()
	dst := r.destinations[id]
	if dst != nil {
		dst.incLeases()
	}
	return dst
}

func (r *repositoryInternal) leaseDestination(destination *Destination) {
	r.Lock()
	defer r.Unlock()
	destination.incLeases()
}

func (r *repositoryInternal) releaseDestination(destination *Destination) {
	r.Lock()
	defer r.Unlock()
	destination.decLeases()
}

func (r *repositoryInternal) retireDestination(destination *Destination) {
	r.Lock()
	defer r.Unlock()
	destination.retire()
}

func (r *repositoryInternal) GetDestinations() []*Destination {
	destinations := make([]*Destination, 0, len(r.destinations))
	for _, destination := range r.destinations {
		destinations = append(destinations, destination)
	}
	return destinations
}

type Destination struct {
	sync.Mutex
	config        *DestinationConfig
	bulker        bulker.Bulker
	streamOptions []bulker.StreamOption

	owner       *repositoryInternal
	retired     bool
	leasesCount int
}

// TopicId generates topic id for Destination
func (d *Destination) TopicId(tableName string) string {
	if tableName == "" {
		tableName = d.config.StreamConfig.TableName
	}
	return MakeTopicId(d.Id(), string(d.config.StreamConfig.BulkMode), tableName)
}

// Id returns destination id
func (d *Destination) Id() string {
	return d.config.Id()
}
func (d *Destination) retire() error {
	d.retired = true
	if d.leasesCount == 0 {
		logging.Infof("[%s] closing retired destination. Ver: %s", d.Id(), d.config.UpdatedAt)
		_ = d.bulker.Close()
	}
	return nil
}

func (d *Destination) incLeases() {
	d.leasesCount++
}

func (d *Destination) decLeases() {
	d.leasesCount--
	if d.retired && d.leasesCount == 0 {
		logging.Infof("[%s] closing retired destination. Ver: %s", d.Id(), d.config.UpdatedAt)
		_ = d.bulker.Close()
	}
}

// Lease destination. destination cannot be closed while at lease one service is using it (e.g. batch consumer)
func (d *Destination) Lease() {
	d.owner.leaseDestination(d)
}

// Release destination. See Lease
func (d *Destination) Release() {
	d.owner.releaseDestination(d)
}

//// AddBatchConsumer Add batch consumer to destination
//func (d *Destination) AddBatchConsumer(batchConsumer *BatchConsumer) {
//	d.Lock()
//	d.batchConsumers[batchConsumer.topicId] = batchConsumer
//	d.Unlock()
//}
//
//// RemoveBatchConsumer removes batch consumer from destination
//// and closes destination when no cosumers left and destination is retired
//func (d *Destination) RemoveBatchConsumer(batchConsumer *BatchConsumer) {
//	d.Lock()
//	bc := d.batchConsumers[batchConsumer.topicId]
//	if bc != batchConsumer {
//		logging.SystemErrorf("[%s] consumers for topic id: %s mismatches: %v != %v", d.Id(), batchConsumer.topicId, bc, batchConsumer)
//	}
//	delete(d.batchConsumers, batchConsumer.topicId)
//	if len(d.batchConsumers) == 0 {
//		if d.retired.Load() {
//			logging.Infof("[%s] closing retired destination", d.Id())
//			_ = d.bulker.Close()
//		} else {
//			logging.Warnf("[%s] no consumers left but destination is not retired", d.Id())
//		}
//	}
//	d.Unlock()
//}
