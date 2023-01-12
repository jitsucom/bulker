package app

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jitsucom/bulker/app/metrics"
	"github.com/jitsucom/bulker/base/objects"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/bulker"
	"regexp"
	"sync"
	"time"
)

var topicUnsupportedCharacters = regexp.MustCompile(`[^a-zA-Z0-9._-]`)
var topicPattern = regexp.MustCompile(`^in[.]id[.](.*)[.]m[.](.*)[.](t|b64)[.](.*)$`)

const topicExpression = "in.id.%s.m.%s.t.%s"
const topicExpressionBase64 = "in.id.%s.m.%s.b64.%s"

const retryTopicMode = "failed"
const deadTopicMode = "dead"

const topicLengthLimit = 200

type TopicManager struct {
	objects.ServiceBase
	sync.Mutex
	ready       bool
	config      *AppConfig
	kafkaConfig *kafka.ConfigMap

	kafkaBootstrapServer string
	//consumer         *kafka.Consumer
	kaftaAdminClient *kafka.AdminClient

	repository *Repository
	cron       *Cron
	//topics by destinationId
	topics          map[string]utils.Set[string]
	abandonedTopics utils.Set[string]
	allTopics       utils.Set[string]

	//batch consumers by destinationId
	batchConsumers  map[string][]BatchConsumer
	retryConsumers  map[string][]BatchConsumer
	streamConsumers map[string][]*StreamConsumer

	bulkerProducer   *Producer
	eventsLogService EventsLogService
	closed           chan struct{}
}

// NewTopicManager returns TopicManager
func NewTopicManager(appContext *AppContext) (*TopicManager, error) {
	base := objects.NewServiceBase("topic-manager")
	admin, err := kafka.NewAdminClient(appContext.kafkaConfig)
	if err != nil {
		return nil, base.NewError("Error creating kafka admin client: %w", err)
	}
	return &TopicManager{
		ServiceBase:          base,
		config:               appContext.config,
		kafkaConfig:          appContext.kafkaConfig,
		repository:           appContext.repository,
		cron:                 appContext.cron,
		kaftaAdminClient:     admin,
		kafkaBootstrapServer: appContext.config.KafkaBootstrapServers,
		topics:               make(map[string]utils.Set[string]),
		bulkerProducer:       appContext.producer,
		eventsLogService:     appContext.eventsLogService,
		batchConsumers:       make(map[string][]BatchConsumer),
		retryConsumers:       make(map[string][]BatchConsumer),
		streamConsumers:      make(map[string][]*StreamConsumer),
		abandonedTopics:      utils.NewSet[string](),
		allTopics:            utils.NewSet[string](),
		closed:               make(chan struct{}),
	}, nil
}

// Start starts TopicManager
func (tm *TopicManager) Start() {
	metadata, err := tm.kaftaAdminClient.GetMetadata(nil, true, tm.config.KafkaAdminMetadataTimeoutMs)
	if err != nil {
		metrics.TopicManagerError("load_metadata_error").Inc()
		tm.Errorf("Error getting metadata from brokers [%s]: %v", tm.kafkaBootstrapServer, err)
	} else {
		tm.loadMetadata(metadata)
		err = tm.EnsureTopic(tm.config.KafkaDestinationsTopicName)
		if err != nil {
			metrics.TopicManagerError("destination-topic_error").Inc()
			tm.Errorf("Failed to create destination topic [%s]: %v", tm.config.KafkaDestinationsTopicName, err)
		}
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
						if consumer.BatchPeriodSec() != bulker.BatchPeriodOption.Get(changedDst.streamOptions) {
							consumer.UpdateBatchPeriod(bulker.BatchPeriodOption.Get(changedDst.streamOptions))
							_, err := tm.cron.ReplaceBatchConsumer(consumer)
							if err != nil {
								metrics.TopicManagerError("reschedule_batch_consumer_error").Inc()
								consumer.Retire()
								tm.SystemErrorf("Failed to re-schedule consumer for destination topic: %s: %v", consumer.TopicId(), err)
								continue
							}
							tm.Infof("Consumer for destination topic %s was re-scheduled with new batch period %d", consumer.TopicId(), consumer.BatchPeriodSec())
						}
					}
					for _, consumer := range tm.retryConsumers[changedDst.Id()] {
						if consumer.BatchPeriodSec() != bulker.BatchPeriodOption.Get(changedDst.streamOptions) {
							consumer.UpdateBatchPeriod(bulker.BatchPeriodOption.Get(changedDst.streamOptions))
							_, err := tm.cron.ReplaceBatchConsumer(consumer)
							if err != nil {
								metrics.TopicManagerError("reschedule_batch_consumer_error").Inc()
								consumer.Retire()
								tm.SystemErrorf("Failed to re-schedule consumer for destination topic: %s: %v", consumer.TopicId(), err)
								continue
							}
							tm.Infof("Consumer for destination topic %s was re-scheduled with new batch period %d", consumer.TopicId(), consumer.BatchPeriodSec())
						}
					}
					for _, consumer := range tm.streamConsumers[changedDst.Id()] {
						err = consumer.UpdateDestination(changedDst)
						if err != nil {
							metrics.TopicManagerError("update_stream_consumer_error").Inc()
							tm.SystemErrorf("Failed to re-create consumer for destination topic: %s: %v", consumer.topicId, err)
							continue
						}
					}
					tm.Unlock()
				}
				for _, deletedDstId := range changes.RemovedDestinationIds {
					for _, consumer := range tm.batchConsumers[deletedDstId] {
						tm.Lock()
						_ = tm.cron.RemoveBatchConsumer(consumer)
						consumer.Retire()
						delete(tm.batchConsumers, deletedDstId)
						tm.Unlock()
					}
					for _, consumer := range tm.retryConsumers[deletedDstId] {
						tm.Lock()
						_ = tm.cron.RemoveBatchConsumer(consumer)
						consumer.Retire()
						delete(tm.retryConsumers, deletedDstId)
						tm.Unlock()
					}
					for _, consumer := range tm.streamConsumers[deletedDstId] {
						tm.Lock()
						_ = consumer.Close()
						delete(tm.streamConsumers, deletedDstId)
						tm.Unlock()
					}
					delete(tm.topics, deletedDstId)
				}
				if len(changes.AddedDestinations) > 0 {
					tm.Lock()
					tm.abandonedTopics.Clear()
					tm.Unlock()
				}
			case <-ticker.C:
				//start := time.Now()
				metadata, err = tm.kaftaAdminClient.GetMetadata(nil, true, tm.config.KafkaAdminMetadataTimeoutMs)
				if err != nil {
					metrics.TopicManagerError("load_metadata_error").Inc()
					tm.Errorf("Error getting metadata: %v", err)
					continue
				}
				tm.loadMetadata(metadata)
				err = tm.EnsureTopic(tm.config.KafkaDestinationsTopicName)
				if err != nil {
					metrics.TopicManagerError("destination-topic_error").Inc()
					tm.Errorf("Failed to create destination topic [%s]: %v", tm.config.KafkaDestinationsTopicName, err)
				}
			}
		}
	}()
}

func (tm *TopicManager) loadMetadata(metadata *kafka.Metadata) {
	tm.Lock()
	defer tm.Unlock()
	start := time.Now()
	var abandonedTopicsCount float64
	var otherTopicsCount float64
	topicsCountByMode := make(map[string]float64)
	topicsErrorsByMode := make(map[string]float64)

	for topic, _ := range metadata.Topics {
		tm.allTopics.Put(topic)
		if tm.abandonedTopics.Contains(topic) {
			abandonedTopicsCount++
			continue
		}
		destinationId, mode, tableName, err := ParseTopicId(topic)
		if err != nil {
			otherTopicsCount++
			continue
		}
		var set utils.Set[string]
		ok := false
		if set, ok = tm.topics[destinationId]; !ok {
			set = utils.NewSet[string]()
			tm.topics[destinationId] = set
		}
		if !set.Contains(topic) {
			tm.Infof("Found topic %s for destination %s and table %s", topic, destinationId, tableName)
			destination := tm.repository.GetDestination(destinationId)
			if destination == nil {
				tm.Warnf("No destination found for topic: %s", topic)
				tm.abandonedTopics.Put(topic)
				continue
			}
			switch mode {
			case "stream":
				streamConsumer, err := NewStreamConsumer(tm.repository, destination, topic, tm.config, tm.kafkaConfig, tm.bulkerProducer, tm.eventsLogService)
				if err != nil {
					topicsErrorsByMode[mode]++
					tm.SystemErrorf("Failed to create consumer for destination topic: %s: %v", topic, err)
					continue
				}
				tm.streamConsumers[destinationId] = append(tm.streamConsumers[destinationId], streamConsumer)
			case "batch":
				batchConsumer, err := NewBatchConsumer(tm.repository, destinationId, bulker.BatchPeriodOption.Get(destination.streamOptions), topic, tm.config, tm.kafkaConfig, tm.eventsLogService)
				if err != nil {
					topicsErrorsByMode[mode]++
					tm.Errorf("Failed to create batch consumer for destination topic: %s: %v", topic, err)
					continue
				}
				tm.batchConsumers[destinationId] = append(tm.batchConsumers[destinationId], batchConsumer)
				_, err = tm.cron.AddBatchConsumer(batchConsumer)
				if err != nil {
					topicsErrorsByMode[mode]++
					batchConsumer.Retire()
					tm.Errorf("Failed to schedule consumer for destination topic: %s: %v", topic, err)
					continue
				} else {
					tm.Infof("Consumer for destination topic %s was scheduled with batch period %d", topic, utils.Nvl(batchConsumer.batchPeriodSec, tm.config.BatchRunnerPeriodSec))
				}
			case retryTopicMode:
				retryConsumer, err := NewRetryConsumer(tm.repository, destinationId, bulker.BatchPeriodOption.Get(destination.streamOptions), topic, tm.config, tm.kafkaConfig)
				if err != nil {
					topicsErrorsByMode[mode]++
					tm.Errorf("Failed to create retry consumer for destination topic: %s: %v", topic, err)
					continue
				}
				tm.retryConsumers[destinationId] = append(tm.retryConsumers[destinationId], retryConsumer)
				_, err = tm.cron.AddBatchConsumer(retryConsumer)
				if err != nil {
					topicsErrorsByMode[mode]++
					retryConsumer.Retire()
					tm.Errorf("Failed to schedule retry consumer for destination topic: %s: %v", topic, err)
					continue
				} else {
					tm.Infof("Retry consumer for destination topic %s was scheduled with batch period %d", topic, utils.Nvl(retryConsumer.batchPeriodSec, tm.config.BatchRunnerPeriodSec))
				}
			case deadTopicMode:
				tm.Infof("Found topic %s for 'dead' events", topic)
			default:
				topicsErrorsByMode[mode]++
				tm.Errorf("Unknown stream mode: %s for topic: %s", mode, topic)
			}
			topicsCountByMode[mode]++
			set.Put(topic)
		}
	}
	for mode, count := range topicsCountByMode {
		metrics.TopicManagerDestinationTopics(mode).Set(count)
	}
	for mode, count := range topicsErrorsByMode {
		metrics.TopicManagerDestinationsError(mode).Set(count)
	}
	metrics.TopicManagerAbandonedTopics.Set(abandonedTopicsCount)
	metrics.TopicManagerOtherTopics.Set(otherTopicsCount)
	tm.Debugf("[topic-manager] Refreshed metadata in %v", time.Since(start))
	tm.ready = true
}

// IsReady returns true if topic manager is ready to serve requests
func (tm *TopicManager) IsReady() bool {
	tm.Lock()
	defer tm.Unlock()
	return tm.ready
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

// EnsureDestinationTopic creates destination topic if it doesn't exist
func (tm *TopicManager) EnsureDestinationTopic(destination *Destination, topicId string) error {
	tm.Lock()
	defer tm.Unlock()
	set := tm.topics[destination.Id()]
	if !set.Contains(topicId) {
		return tm.createDestinationTopic(destination, topicId)
	}
	return nil
}

// EnsureTopic creates topic if it doesn't exist
func (tm *TopicManager) EnsureTopic(topicId string) error {
	tm.Lock()
	defer tm.Unlock()
	if !tm.allTopics.Contains(topicId) {
		return tm.createTopic(topicId)
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

// createDestinationTopic creates topic for destination
func (tm *TopicManager) createDestinationTopic(destination *Destination, topic string) error {
	id, mode, tableName, err := ParseTopicId(topic)
	errorType := ""
	defer func() {
		if errorType != "" {
			metrics.TopicManagerCreateError(id, mode, tableName, errorType).Inc()
		} else {
			metrics.TopicManagerCreateSuccess(id, mode, tableName).Inc()
		}
	}()
	if err != nil {
		errorType = "invalid topic name"
		return tm.NewError("invalid topic name %s", topic)
	}
	if mode != "stream" && mode != "batch" {
		errorType = "unknown stream mode"
		return tm.NewError("Unknown stream mode: %s for topic: %s", mode, topic)
	}
	destinationId := destination.Id()
	failedTopic, _ := MakeTopicId(destinationId, retryTopicMode, tableName, false)
	deadTopic, _ := MakeTopicId(destinationId, deadTopicMode, tableName, false)
	topicRes, err := tm.kaftaAdminClient.CreateTopics(context.Background(), []kafka.TopicSpecification{
		{
			Topic:         topic,
			NumPartitions: 1,
			//TODO  get broker count from admin
			ReplicationFactor: tm.config.KafkaTopicReplicationFactor,
			Config: map[string]string{
				"retention.ms": fmt.Sprint(tm.config.KafkaTopicRetentionHours * 60 * 60 * 1000),
			},
		},
		{
			Topic:             failedTopic,
			NumPartitions:     1,
			ReplicationFactor: tm.config.KafkaTopicReplicationFactor,
			Config: map[string]string{
				"retention.ms": fmt.Sprint(tm.config.KafkaFailedTopicRetentionHours * 60 * 60 * 1000),
			},
		},
		{
			Topic:             deadTopic,
			NumPartitions:     1,
			ReplicationFactor: tm.config.KafkaTopicReplicationFactor,
			Config: map[string]string{
				"retention.ms": fmt.Sprint(tm.config.KafkaDeadTopicRetentionHours * 60 * 60 * 1000),
			},
		},
	})
	if err != nil {
		errorType = "kafka error"
		if err, ok := err.(kafka.Error); ok {
			errorType = metrics.KafkaErrorCode(err)
		}
		return tm.NewError("Error creating topic %s: %w", topic, err)
	}
	for _, res := range topicRes {
		if res.Error.Code() != kafka.ErrNoError && res.Error.Code() != kafka.ErrTopicAlreadyExists {
			errorType = metrics.KafkaErrorCode(res.Error)
			return tm.NewError("Error creating topic %s: %w", res.Topic, res.Error)
		}
	}
	tm.Infof("Created topics: %s, %s, %s", topic, failedTopic, deadTopic)
	return nil
}

// createTopic creates topic for any purpose
func (tm *TopicManager) createTopic(topic string) error {
	topicRes, err := tm.kaftaAdminClient.CreateTopics(context.Background(), []kafka.TopicSpecification{
		{
			Topic:         topic,
			NumPartitions: 1,
			//TODO  get broker count from admin
			ReplicationFactor: tm.config.KafkaTopicReplicationFactor,
			Config: map[string]string{
				"retention.ms": fmt.Sprint(tm.config.KafkaTopicRetentionHours * 60 * 60 * 1000),
			},
		},
	})
	if err != nil {
		return tm.NewError("Error creating topic %s: %w", topic, err)
	}
	for _, res := range topicRes {
		if res.Error.Code() != kafka.ErrNoError && res.Error.Code() != kafka.ErrTopicAlreadyExists {
			return tm.NewError("Error creating topic %s: %w", res.Topic, res.Error)
		}
	}
	tm.allTopics.Put(topic)
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
	for _, consumers := range tm.retryConsumers {
		for _, consumer := range consumers {
			consumer.Retire()
		}
	}
	for _, consumers := range tm.streamConsumers {
		for _, consumer := range consumers {
			consumer.Close()
		}
	}
	return nil
}

func ParseTopicId(topic string) (destinationId, mode, tableName string, err error) {
	topicGroups := topicPattern.FindStringSubmatch(topic)
	if len(topicGroups) == 5 {
		destinationId = topicGroups[1]
		mode = topicGroups[2]
		tableEncoding := topicGroups[3]
		tableName = topicGroups[4]
		if tableEncoding == "b64" {
			b, err := base64.RawURLEncoding.DecodeString(tableName)
			if err != nil {
				return "", "", "", fmt.Errorf("error decoding table name from topic: %s: %w", topic, err)
			}
			tableName = string(b)
		}

	} else {
		err = fmt.Errorf("topic name %s doesn't match pattern %s", topic, topicPattern.String())
	}
	return
}

func MakeTopicId(destinationId, mode, tableName string, checkLength bool) (string, error) {
	unsupportedChars := topicUnsupportedCharacters.FindString(tableName)
	topicId := ""
	if unsupportedChars != "" {
		tableName = base64.RawURLEncoding.EncodeToString([]byte(tableName))
		topicId = fmt.Sprintf(topicExpressionBase64, destinationId, mode, tableName)
	} else {
		topicId = fmt.Sprintf(topicExpression, destinationId, mode, tableName)
	}
	if checkLength && len(topicId) > topicLengthLimit {
		return "", fmt.Errorf("topic name %s length %d exceeds limit (%d). Please choose shorter table name. Recommended table name length is <= 63 symbols", topicId, len(topicId), topicLengthLimit)
	}
	return topicId, nil
}
