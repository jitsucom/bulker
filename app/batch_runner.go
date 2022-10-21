package app

import (
	"github.com/jitsucom/bulker/base/logging"
	"time"
)

type BatchRunner struct {
	config       *Config
	repository   *Repository
	topicManager *TopicManager
	batchPeriod  time.Duration
	closed       chan struct{}
}

func NewBatchRunner(config *Config, repository *Repository, topicManager *TopicManager) *BatchRunner {
	return &BatchRunner{
		config:       config,
		repository:   repository,
		topicManager: topicManager,
		batchPeriod:  time.Second * time.Duration(config.BatchRunnerPeriodSec),
		closed:       make(chan struct{}),
	}
}

// Start
func (br *BatchRunner) Start() {
	logging.Infof("Starting batch runner with period: %v", br.batchPeriod)
	go func() {
		ticker := time.NewTicker(br.batchPeriod)
		defer ticker.Stop()
		for {
			select {
			case <-br.closed:
				return
			case tm := <-ticker.C:
				logging.Infof("Running batch process: %v", tm)
				destinations := br.repository.GetDestinations()
				for _, destination := range destinations {
					topics := br.topicManager.GetTopicsSlice(destination.Id())
					for _, topic := range topics {
						batchSize := destination.config.BatchSize
						logging.Infof("[%s] Running batch task for destination topic %s batch size: %d", destination.Id(), topic, batchSize)
						batchConsumer, err := NewBatchConsumer(destination, topic, br.config, destination.config.BatchSize)
						if err != nil {
							logging.Errorf("[%s] Failed to create batch consumer for destination: %v", destination.Id(), err)
							continue
						}
						consumed, err := batchConsumer.ConsumeAll()
						_ = batchConsumer.Close()
						if err != nil {
							logging.Errorf("[%s] Consume aborted with error. Consumed successfully: %d. Error: %v", destination.Id(), consumed, err)
							continue
						}
						logging.Infof("[%s] Consumed successfully: %d", destination.Id(), consumed)
					}

				}
			}
		}
	}()
}

// Close
func (br *BatchRunner) Close() {
	close(br.closed)
}
