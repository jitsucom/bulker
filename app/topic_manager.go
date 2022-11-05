package app

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/utils"
	"regexp"
	"sync"
	"time"
)

var TopicPattern = regexp.MustCompile(`^incoming[.]destinationId[.](.*)[.]mode[.](.*)[.]tableName[.](.*)$`)

type TopicManager struct {
	sync.Mutex
	config      *AppConfig
	kafkaConfig *kafka.ConfigMap

	kafkaBootstrapServer string
	//consumer         *kafka.Consumer
	kaftaAdminClient *kafka.AdminClient

	repository *Repository
	cron       *Cron
	//topics by destinationId
	topics         map[string]utils.Set[string]
	abanonedTopics utils.Set[string]
	//batch consumers by destinationId
	batchConsumers  map[string][]*BatchConsumer
	streamConsumers map[string][]*StreamConsumer
	closed          chan struct{}
}

// NewTopicManager returns TopicManager
func NewTopicManager(config *AppConfig, kafkaConfig *kafka.ConfigMap, repository *Repository, cron *Cron) (*TopicManager, error) {
	admin, err := kafka.NewAdminClient(kafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("[topic-manager] Error creating kafka admin client: %w", err)
	}
	return &TopicManager{
		config:               config,
		kafkaConfig:          kafkaConfig,
		repository:           repository,
		cron:                 cron,
		kaftaAdminClient:     admin,
		kafkaBootstrapServer: config.KafkaBootstrapServers,
		topics:               make(map[string]utils.Set[string]),
		batchConsumers:       make(map[string][]*BatchConsumer),
		streamConsumers:      make(map[string][]*StreamConsumer),
		abanonedTopics:       utils.NewSet[string](),
		closed:               make(chan struct{}),
	}, nil
}

// Start starts TopicManager
func (tm *TopicManager) Start() error {
	metadata, err := tm.kaftaAdminClient.GetMetadata(nil, true, tm.config.KafkaAdminMetadataTimeoutMs)
	if err != nil {
		return fmt.Errorf("[topic-manager] Error getting metadata: %w", err)
	}
	// refresh metadata every 10 seconds
	go func() {
		ticker := time.NewTicker(time.Duration(tm.config.TopicManagerRefreshPeriodSec) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-tm.closed:
				return
			case changes := <-tm.repository.ChangesChannel():
				for _, changedDst := range changes.ChangedDestinations {
					tm.Lock()
					for _, consumer := range tm.batchConsumers[changedDst.Id()] {
						if consumer.batchPeriodSec != changedDst.config.BatchPeriodSec {
							consumer.batchPeriodSec = changedDst.config.BatchPeriodSec
							_, err := tm.cron.ReplaceBatchConsumer(consumer)
							if err != nil {
								consumer.Retire()
								logging.SystemErrorf("[%s] Failed to re-schedule consumer for destination topic: %s: %v", changedDst.Id(), consumer.topicId, err)
								continue
							}
							logging.Infof("[%s] Consumer for destination topic %s was re-scheduled with new batch period %d", changedDst.Id(), consumer.topicId, consumer.batchPeriodSec)
						}
					}
					for _, consumer := range tm.streamConsumers[changedDst.Id()] {
						err = consumer.UpdateDestination(changedDst)
						if err != nil {
							logging.SystemErrorf("[%s] Failed to re-create consumer for destination topic: %s: %v", changedDst.Id(), consumer.topicId, err)
							continue
						}
					}
					tm.Unlock()
				}
				for _, deletedDstId := range changes.RemovedDestinationIds {
					for _, consumer := range tm.batchConsumers[deletedDstId] {
						tm.Lock()
						consumer.Retire()
						delete(tm.batchConsumers, deletedDstId)
						tm.Unlock()
					}
					for _, consumer := range tm.streamConsumers[deletedDstId] {
						tm.Lock()
						_ = consumer.Close()
						delete(tm.streamConsumers, deletedDstId)
						tm.Unlock()
					}
				}
				if len(changes.AddedDestinations) > 0 {
					tm.Lock()
					tm.abanonedTopics.Clear()
					tm.Unlock()
				}
			case <-ticker.C:
				//start := time.Now()
				metadata, err = tm.kaftaAdminClient.GetMetadata(nil, true, tm.config.KafkaAdminMetadataTimeoutMs)
				if err != nil {
					logging.Errorf("[topic-manager] Error getting metadata: %v", err)
					continue
				}
				tm.Lock()
				for topic, _ := range metadata.Topics {
					if tm.abanonedTopics.Contains(topic) {
						continue
					}
					topicGroups := TopicPattern.FindStringSubmatch(topic)
					if len(topicGroups) == 4 {
						destinationId := topicGroups[1]
						mode := topicGroups[2]
						tableName := topicGroups[3]
						var set utils.Set[string]
						ok := false
						if set, ok = tm.topics[destinationId]; !ok {
							set = utils.NewSet[string]()
							tm.topics[destinationId] = set
						}
						if !set.Contains(topic) {
							logging.Infof("[topic-manager] Found topic %s for destination %s and table %s", topic, destinationId, tableName)
							destination := tm.repository.GetDestination(destinationId)
							if destination == nil {
								logging.Warnf("[topic-manager] No destination found for topic: %s", topic)
								tm.abanonedTopics.Put(topic)
								continue
							}
							if mode == "stream" {
								streamConsumer, err := NewStreamConsumer(tm.repository, destination, topic, tm.config, tm.kafkaConfig)
								if err != nil {
									logging.SystemErrorf("[%s] Failed to create consumer for destination topic: %s: %v", destination.Id(), topic, err)
									continue
								}
								tm.streamConsumers[destinationId] = append(tm.streamConsumers[destinationId], streamConsumer)
							} else if mode == "batch" {
								batchConsumer, err := NewBatchConsumer(tm.repository, destinationId, destination.config.BatchPeriodSec, topic, tm.config, tm.kafkaConfig)
								if err != nil {
									logging.Errorf("[%s] Failed to create batch consumer for destination topic: %s: %v", destination.Id(), topic, err)
									continue
								}
								tm.batchConsumers[destinationId] = append(tm.batchConsumers[destinationId], batchConsumer)
								_, err = tm.cron.AddBatchConsumer(batchConsumer)
								if err != nil {
									batchConsumer.Retire()
									logging.Errorf("[%s] Failed to schedule consumer for destination topic: %s: %v", destination.Id(), topic, err)
									continue
								} else {
									logging.Infof("[%s] Consumer for destination topic %s was scheduled with batch period %d", destination.Id(), topic, batchConsumer.batchPeriodSec)
								}
							} else {
								logging.Errorf("[%s] Unknown stream mode: %s for topic: %s", destination.Id(), mode, topic)
							}
							set.Put(topic)
						}

					}
				}
				tm.Unlock()
				//logging.Infof("[topic-manager] Refreshed metadata in %v", time.Since(start))
			}
		}
	}()
	return nil
}

//// GetTopicsSlice returns topics for destinationId
//func (tm *TopicManager) GetTopicsSlice(destinationId string) []string {
//	tm.Lock()
//	defer tm.Unlock()
//	if set, ok := tm.topics[destinationId]; ok {
//		return set.ToSlice()
//	}
//	return nil
//}

//// GetTopics returns topics for destinationId
//func (tm *TopicManager) GetTopics(destinationId string) utils.Set[string] {
//	tm.Lock()
//	defer tm.Unlock()
//	if set, ok := tm.topics[destinationId]; ok {
//		return set.Clone()
//	}
//	return nil
//}

// EnsureTopic creates topic if it doesn't exist
func (tm *TopicManager) EnsureTopic(destination *Destination, topicId string) error {
	tm.Lock()
	defer tm.Unlock()
	set := tm.topics[destination.Id()]
	if !set.Contains(topicId) {
		return tm.createTopic(destination, topicId)
	}
	return nil
}

//// Remove all topics for destination
//func (tm *TopicManager) RemoveTopics(destinationId string) {
//	tm.Lock()
//	defer tm.Unlock()
//	if set, ok := tm.topics[destinationId]; ok {
//		for topic, _ := range set {
//			_, err := tm.kaftaAdminClient.DeleteTopics([]string{topic})
//			if err != nil {
//				logging.Errorf("[topic-manager] Error deleting topic %s: %v", topic, err)
//			}
//		}
//		delete(tm.topics, destinationId)
//	}
//}

// CreateTopic creates topic for destinationId
func (tm *TopicManager) createTopic(destination *Destination, topic string) error {
	topicGroups := TopicPattern.FindStringSubmatch(topic)
	mode := ""
	if len(topicGroups) == 4 {
		mode = topicGroups[2]
		if mode != "stream" && mode != "batch" {
			return fmt.Errorf("[topic-manager] Unknown stream mode: %s for topic: %s", mode, topic)
		}
	} else {
		return fmt.Errorf("[topic-manager] invalid topic name %s", topic)
	}
	destinationId := destination.Id()
	var set utils.Set[string]
	ok := false
	if set, ok = tm.topics[destinationId]; !ok {
		set = utils.NewSet[string]()
		tm.topics[destinationId] = set
	}
	topicRes, err := tm.kaftaAdminClient.CreateTopics(context.Background(), []kafka.TopicSpecification{
		{
			Topic:         topic,
			NumPartitions: tm.config.KafkaTopicPartitionsCount,
			//TODO  get broker count from admin
			ReplicationFactor: tm.config.KafkaTopicReplicationFactor,
			Config: map[string]string{
				"retention.ms": fmt.Sprint(tm.config.KafkaTopicRetentionMs),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("[topic-manager] Error creating topic %s: %w", topic, err)
	}
	logging.Infof("[topic-manager] Created topic %s", topic)
	for _, res := range topicRes {
		if res.Error.Code() != kafka.ErrNoError {
			return fmt.Errorf("[topic-manager] Error creating topic %s: %v", topic, res.Error)
		}
	}
	switch mode {
	case "stream":
		streamConsumer, err := NewStreamConsumer(tm.repository, destination, topic, tm.config, tm.kafkaConfig)
		if err != nil {
			return fmt.Errorf("[%s] Failed to create consumer for destination topic: %s: %v", destination.Id(), topic, err)
		}
		tm.streamConsumers[destinationId] = append(tm.streamConsumers[destinationId], streamConsumer)
	case "batch":
		batchConsumer, err := NewBatchConsumer(tm.repository, destinationId, destination.config.BatchPeriodSec, topic, tm.config, tm.kafkaConfig)
		if err != nil {
			return fmt.Errorf("[%s] Failed to create batch consumer for destination topic: %s: %v", destinationId, topic, err)
		}
		tm.batchConsumers[destinationId] = append(tm.batchConsumers[destinationId], batchConsumer)
		_, err = tm.cron.AddBatchConsumer(batchConsumer)
		if err != nil {
			batchConsumer.Retire()
			return fmt.Errorf("[%s] Failed to schedule consumer for destination topic: %s: %v", destinationId, topic, err)
		} else {
			logging.Infof("[%s] Consumer for destination topic %s was scheduled with batch period %d", destination.Id(), topic, batchConsumer.batchPeriodSec)
		}
	}
	set.Put(topic)

	return nil
}

func (tm *TopicManager) Close() error {
	close(tm.closed)
	tm.kaftaAdminClient.Close()
	//close all batch consumers
	tm.Lock()
	defer tm.Unlock()
	for _, consumers := range tm.batchConsumers {
		for _, consumer := range consumers {
			consumer.Retire()
		}
	}
	return nil
}
