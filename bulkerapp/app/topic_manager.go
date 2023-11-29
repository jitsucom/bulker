package app

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jitsucom/bulker/bulkerapp/metrics"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/eventslog"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/safego"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"regexp"
	"sync"
	"time"
)

const (
	topicExpression       = "in.id.%s.m.%s.t.%s"
	topicExpressionBase64 = "in.id.%s.m.%s.b64.%s"

	retryTopicMode = "retry"
	deadTopicMode  = "dead"

	allTablesToken = "_all_"

	topicLengthLimit = 249
)

var topicUnsupportedCharacters = regexp.MustCompile(`[^a-zA-Z0-9._-]`)
var topicPattern = regexp.MustCompile(`^in[.]id[.](.*)[.]m[.](.*)[.](t|b64)[.](.*)$`)

type TopicManager struct {
	appbase.Service
	sync.Mutex
	ready                     bool
	config                    *Config
	kafkaConfig               *kafka.ConfigMap
	requiredDestinationTopics map[string]map[string]string

	kafkaBootstrapServer string
	//consumer         *kafka.Consumer
	kaftaAdminClient *kafka.AdminClient

	repository *Repository
	cron       *Cron
	// consumedTopics by destinationId. Consumed topics are topics that have consumer started
	consumedTopics  map[string]utils.Set[string]
	abandonedTopics utils.Set[string]
	allTopics       utils.Set[string]

	//batch consumers by destinationId
	batchConsumers  map[string][]BatchConsumer
	retryConsumers  map[string][]BatchConsumer
	streamConsumers map[string][]*StreamConsumer

	batchProducer    *Producer
	streamProducer   *Producer
	eventsLogService eventslog.EventsLogService
	refreshChan      chan bool
	closed           chan struct{}
}

// NewTopicManager returns TopicManager
func NewTopicManager(appContext *Context) (*TopicManager, error) {
	base := appbase.NewServiceBase("topic-manager")
	admin, err := kafka.NewAdminClient(appContext.kafkaConfig)
	if err != nil {
		return nil, base.NewError("Error creating kafka admin client: %v", err)
	}
	return &TopicManager{
		Service:              base,
		config:               appContext.config,
		kafkaConfig:          appContext.kafkaConfig,
		repository:           appContext.repository,
		cron:                 appContext.cron,
		kaftaAdminClient:     admin,
		kafkaBootstrapServer: appContext.config.KafkaBootstrapServers,
		consumedTopics:       make(map[string]utils.Set[string]),
		batchProducer:        appContext.batchProducer,
		streamProducer:       appContext.streamProducer,
		eventsLogService:     appContext.eventsLogService,
		batchConsumers:       make(map[string][]BatchConsumer),
		retryConsumers:       make(map[string][]BatchConsumer),
		streamConsumers:      make(map[string][]*StreamConsumer),
		abandonedTopics:      utils.NewSet[string](),
		allTopics:            utils.NewSet[string](),
		closed:               make(chan struct{}),
		refreshChan:          make(chan bool, 1),
		requiredDestinationTopics: map[string]map[string]string{
			retryTopicMode: {
				"cleanup.policy": "delete,compact",
				"segment.bytes":  fmt.Sprint(appContext.config.KafkaRetryTopicSegmentBytes),
				"retention.ms":   fmt.Sprint(appContext.config.KafkaTopicRetentionHours * 60 * 60 * 1000),
				"segment.ms":     fmt.Sprint(appContext.config.KafkaTopicSegmentHours * 60 * 60 * 1000),
			},
			deadTopicMode: {
				"cleanup.policy": "delete,compact",
				"retention.ms":   fmt.Sprint(appContext.config.KafkaDeadTopicRetentionHours * 60 * 60 * 1000),
				"segment.ms":     fmt.Sprint(appContext.config.KafkaTopicSegmentHours * 60 * 60 * 1000),
			},
		},
	}, nil
}

// Start starts TopicManager
func (tm *TopicManager) Start() {
	tm.LoadMetadata()
	safego.RunWithRestart(func() {
		ticker := time.NewTicker(time.Duration(tm.config.TopicManagerRefreshPeriodSec) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-tm.closed:
				return
			case changes := <-tm.repository.ChangesChannel():
				// listener for destination changes
				tm.changeListener(changes)
			case <-ticker.C:
				// refresh metadata every 10 seconds
				tm.LoadMetadata()
			case <-tm.refreshChan:
				// refreshes triggered by destination changes
				tm.LoadMetadata()
			}
		}
	})
}

func (tm *TopicManager) LoadMetadata() {
	metadata, err := tm.kaftaAdminClient.GetMetadata(nil, true, tm.config.KafkaAdminMetadataTimeoutMs)
	if err != nil {
		metrics.TopicManagerError("load_metadata_error").Inc()
		tm.Errorf("Error getting metadata: %v", err)
	} else {
		tm.processMetadata(metadata)
	}
}

func (tm *TopicManager) processMetadata(metadata *kafka.Metadata) {
	tm.Lock()
	defer tm.Unlock()
	start := time.Now()
	var abandonedTopicsCount float64
	var otherTopicsCount float64
	topicsCountByMode := make(map[string]float64)
	topicsErrorsByMode := make(map[string]float64)

	allTopics := utils.NewSet[string]()

	for topic, topicMetadata := range metadata.Topics {
		allTopics.Put(topic)
		if tm.abandonedTopics.Contains(topic) {
			abandonedTopicsCount++
			continue
		}
		destinationId, mode, tableName, err := ParseTopicId(topic)
		if err != nil {
			otherTopicsCount++
			continue
		}
		var dstTopics utils.Set[string]
		ok := false
		if dstTopics, ok = tm.consumedTopics[destinationId]; !ok {
			dstTopics = utils.NewSet[string]()
			tm.consumedTopics[destinationId] = dstTopics
		}
		if !dstTopics.Contains(topic) {
			tm.Debugf("Found topic %s for destination %s and table %s", topic, destinationId, tableName)
			destination := tm.repository.GetDestination(destinationId)
			if destination == nil {
				tm.Warnf("No destination found for topic: %s", topic)
				tm.abandonedTopics.Put(topic)
				continue
			}
			switch mode {
			case "stream":
				streamConsumer, err := NewStreamConsumer(tm.repository, destination, topic, tm.config, tm.kafkaConfig, tm.streamProducer, tm.eventsLogService)
				if err != nil {
					topicsErrorsByMode[mode]++
					tm.SystemErrorf("Failed to create consumer for destination topic: %s: %v", topic, err)
					continue
				} else {
					tm.Infof("Stream consumer for destination topic %s was started.", topic)
				}
				tm.streamConsumers[destinationId] = append(tm.streamConsumers[destinationId], streamConsumer)
			case "batch":
				batchPeriodSec := utils.Nvl(int(bulker.BatchFrequencyOption.Get(destination.streamOptions)*60), tm.config.BatchRunnerPeriodSec)
				// check topic partitions count
				var err error
				if len(topicMetadata.Partitions) > 1 {
					metrics.ConsumerErrors(topic, mode, destinationId, tableName, "invalid_partitions_count").Inc()
					err = fmt.Errorf("Topic has more than 1 partition. Batch Consumer supports only topics with a single partition")
				}
				var batchConsumer *BatchConsumerImpl
				if err == nil {
					batchConsumer, err = NewBatchConsumer(tm.repository, destinationId, batchPeriodSec, topic, tm.config, tm.kafkaConfig, tm.batchProducer, tm.eventsLogService)
				}
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
					tm.Infof("Consumer for destination topic %s was scheduled with batch period %ds.", topic, batchConsumer.BatchPeriodSec())
				}
			case retryTopicMode:
				retryPeriodSec := utils.Nvl(int(bulker.RetryFrequencyOption.Get(destination.streamOptions)*60), tm.config.BatchRunnerRetryPeriodSec)
				var err error
				if len(topicMetadata.Partitions) > 1 {
					metrics.ConsumerErrors(topic, mode, destinationId, tableName, "invalid_partitions_count").Inc()
					err = fmt.Errorf("Topic has more than 1 partition. Retry Consumer supports only topics with a single partition")
				}
				var retryConsumer *RetryConsumer
				if err == nil {
					retryConsumer, err = NewRetryConsumer(tm.repository, destinationId, retryPeriodSec, topic, tm.config, tm.kafkaConfig, tm.batchProducer)
				}
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
					tm.Infof("Retry consumer for destination topic %s was scheduled with batch period %ds", topic, retryConsumer.BatchPeriodSec())
				}
			case deadTopicMode:
				tm.Debugf("Found topic %s for 'dead' events", topic)
			default:
				topicsErrorsByMode[mode]++
				tm.Errorf("Unknown stream mode: %s for topic: %s", mode, topic)
			}
			topicsCountByMode[mode]++
			dstTopics.Put(topic)
		}
	}
	for _, destination := range tm.repository.GetDestinations() {
		dstTopics, ok := tm.consumedTopics[destination.Id()]
		for mode, config := range tm.requiredDestinationTopics {
			topicId, _ := MakeTopicId(destination.Id(), mode, allTablesToken, false)
			if !ok || !dstTopics.Contains(topicId) {
				//tm.Debugf("Creating topic %s for destination %s", topicId, destination.Id())
				err := tm.createDestinationTopic(topicId, config)
				if err != nil {
					tm.Errorf("Failed to create topic %s for destination %s: %v", topicId, destination.Id(), err)
				}
			}
		}
	}
	tm.allTopics = allTopics
	err := tm.ensureTopic(tm.config.KafkaDestinationsTopicName, tm.config.KafkaDestinationsTopicPartitions,
		map[string]string{
			"retention.ms": fmt.Sprint(tm.config.KafkaTopicRetentionHours * 60 * 60 * 1000),
			"segment.ms":   fmt.Sprint(tm.config.KafkaTopicSegmentHours * 60 * 60 * 1000),
		})
	if err != nil {
		metrics.TopicManagerError("destination-topic_error").Inc()
		tm.SystemErrorf("Failed to create destination topic [%s]: %v", tm.config.KafkaDestinationsTopicName, err)
	}
	if err != nil {
		metrics.TopicManagerError("destination-topic_error").Inc()
		tm.SystemErrorf("Failed to create multi-threaded destination topic [%s]: %v", tm.config.KafkaDestinationsTopicName, err)
	}
	err = tm.ensureTopic(tm.config.KafkaDestinationsDeadLetterTopicName, 1, map[string]string{
		"cleanup.policy": "delete,compact",
		"retention.ms":   fmt.Sprint(tm.config.KafkaDeadTopicRetentionHours * 60 * 60 * 1000),
		"segment.ms":     fmt.Sprint(tm.config.KafkaTopicSegmentHours * 60 * 60 * 1000),
	})
	if err != nil {
		metrics.TopicManagerError("destination-topic_error").Inc()
		tm.SystemErrorf("Failed to create destination dead letter topic [%s]: %v", tm.config.KafkaDestinationsDeadLetterTopicName, err)
	}
	destinationsRetryTopicName := tm.config.KafkaDestinationsRetryTopicName
	err = tm.ensureTopic(destinationsRetryTopicName, 1, map[string]string{
		"cleanup.policy": "delete,compact",
		"segment.bytes":  fmt.Sprint(tm.config.KafkaRetryTopicSegmentBytes),
		"retention.ms":   fmt.Sprint(tm.config.KafkaTopicRetentionHours * 60 * 60 * 1000),
		"segment.ms":     fmt.Sprint(tm.config.KafkaTopicSegmentHours * 60 * 60 * 1000),
	})
	if err != nil {
		metrics.TopicManagerError("destination-topic_error").Inc()
		tm.SystemErrorf("Failed to create destination retry topic [%s]: %v", destinationsRetryTopicName, err)
	}
	if _, dstRetryCnsmrStarted := tm.retryConsumers[destinationsRetryTopicName]; !dstRetryCnsmrStarted {
		retryPeriodSec := tm.config.BatchRunnerRetryPeriodSec
		retryConsumer, err := NewRetryConsumer(nil, "", retryPeriodSec, destinationsRetryTopicName, tm.config, tm.kafkaConfig, tm.batchProducer)
		if err != nil {
			tm.SystemErrorf("Failed to create retry consumer for destination topic: %s: %v", destinationsRetryTopicName, err)
		} else {
			tm.retryConsumers[destinationsRetryTopicName] = append(tm.retryConsumers[destinationsRetryTopicName], retryConsumer)
			_, err = tm.cron.AddBatchConsumer(retryConsumer)
			if err != nil {
				retryConsumer.Retire()
				tm.SystemErrorf("Failed to schedule retry consumer for destination topic: %s: %v", destinationsRetryTopicName, err)
			} else {
				tm.Infof("Retry consumer for destination topic %s was scheduled with batch period %ds", destinationsRetryTopicName, retryConsumer.BatchPeriodSec())
			}
		}
	}

	for mode, count := range topicsCountByMode {
		metrics.TopicManagerDestinations(mode, "success").Set(count)
	}
	for mode, count := range topicsErrorsByMode {
		metrics.TopicManagerDestinations(mode, "error").Set(count)
	}
	metrics.TopicManagerAbandonedTopics.Set(abandonedTopicsCount)
	metrics.TopicManagerOtherTopics.Set(otherTopicsCount)
	tm.Debugf("[topic-manager] Refreshed metadata in %v", time.Since(start))
	tm.ready = true
}

func (tm *TopicManager) changeListener(changes RepositoryChange) {
	for _, changedDst := range changes.ChangedDestinations {
		tm.Lock()
		for _, consumer := range tm.batchConsumers[changedDst.Id()] {
			batchPeriodSec := utils.Nvl(int(bulker.BatchFrequencyOption.Get(changedDst.streamOptions)*60), tm.config.BatchRunnerPeriodSec)
			if consumer.BatchPeriodSec() != batchPeriodSec {
				consumer.UpdateBatchPeriod(batchPeriodSec)
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
			retryPeriodSec := utils.Nvl(int(bulker.RetryFrequencyOption.Get(changedDst.streamOptions)*60), tm.config.BatchRunnerRetryPeriodSec)
			if consumer.BatchPeriodSec() != retryPeriodSec {
				consumer.UpdateBatchPeriod(retryPeriodSec)
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
			err := consumer.UpdateDestination(changedDst)
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
		delete(tm.consumedTopics, deletedDstId)
	}
	if len(changes.AddedDestinations) > 0 {
		tm.Lock()
		tm.abandonedTopics.Clear()
		tm.Unlock()
	}
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
	set := tm.consumedTopics[destination.Id()]
	if !set.Contains(topicId) {
		return tm.createDestinationTopic(topicId, nil)
	}
	return nil
}

// ensureTopic creates topic if it doesn't exist
func (tm *TopicManager) ensureTopic(topicId string, partitions int, config map[string]string) error {
	if !tm.allTopics.Contains(topicId) {
		return tm.createTopic(topicId, partitions, config)
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
func (tm *TopicManager) createDestinationTopic(topic string, config map[string]string) error {
	id, mode, tableName, err := ParseTopicId(topic)
	errorType := ""
	defer func() {
		if errorType != "" {
			metrics.TopicManagerCreate(topic, id, mode, tableName, "error", errorType).Inc()
		} else {
			metrics.TopicManagerCreate(topic, id, mode, tableName, "success", "").Inc()
		}
	}()
	if err != nil {
		errorType = "invalid topic name"
		return tm.NewError("invalid topic name %s", topic)
	}
	switch mode {
	case "stream", "batch", deadTopicMode, retryTopicMode:
		// ok
	default:
		errorType = "unknown stream mode"
		return tm.NewError("Unknown stream mode: %s for topic: %s", mode, topic)
	}
	topicConfig := map[string]string{
		"retention.ms":     fmt.Sprint(tm.config.KafkaTopicRetentionHours * 60 * 60 * 1000),
		"segment.ms":       fmt.Sprint(tm.config.KafkaTopicSegmentHours * 60 * 60 * 1000),
		"compression.type": tm.config.KafkaTopicCompression,
	}
	utils.MapPutAll(topicConfig, config)
	topicRes, err := tm.kaftaAdminClient.CreateTopics(context.Background(), []kafka.TopicSpecification{
		{
			Topic:         topic,
			NumPartitions: 1,
			//TODO  get broker count from admin
			ReplicationFactor: tm.config.KafkaTopicReplicationFactor,
			Config:            topicConfig,
		},
	})
	if err != nil {
		errorType = "kafka error"
		if err, ok := err.(kafka.Error); ok {
			errorType = metrics.KafkaErrorCode(err)
		}
		return tm.NewError("Error creating topic %s: %v", topic, err)
	}
	for _, res := range topicRes {
		if res.Error.Code() != kafka.ErrNoError && res.Error.Code() != kafka.ErrTopicAlreadyExists {
			errorType = metrics.KafkaErrorCode(res.Error)
			return tm.NewError("Error creating topic %s: %v", res.Topic, res.Error)
		}
	}
	tm.Infof("Created topic: %s", topic)
	tm.Refresh()
	return nil
}

// createTopic creates topic for any purpose
func (tm *TopicManager) createTopic(topic string, partitions int, config map[string]string) error {
	errorType := ""
	defer func() {
		if errorType != "" {
			metrics.TopicManagerCreate(topic, "", "", "", "error", errorType).Inc()
		} else {
			metrics.TopicManagerCreate(topic, "", "", "", "success", "").Inc()
		}
	}()
	topicConfig := map[string]string{
		"compression.type": tm.config.KafkaTopicCompression,
		"retention.ms":     fmt.Sprint(tm.config.KafkaTopicRetentionHours * 60 * 60 * 1000),
		"segment.ms":       fmt.Sprint(tm.config.KafkaTopicSegmentHours * 60 * 60 * 1000),
	}
	utils.MapPutAll(topicConfig, config)
	topicRes, err := tm.kaftaAdminClient.CreateTopics(context.Background(), []kafka.TopicSpecification{
		{
			Topic:         topic,
			NumPartitions: partitions,
			//TODO  get broker count from admin
			ReplicationFactor: tm.config.KafkaTopicReplicationFactor,
			Config:            topicConfig,
		},
	})
	if err != nil {
		errorType = "kafka error"
		if err, ok := err.(kafka.Error); ok {
			errorType = metrics.KafkaErrorCode(err)
		}
		return tm.NewError("Error creating topic %s: %v", topic, err)
	}
	for _, res := range topicRes {
		if res.Error.Code() != kafka.ErrNoError && res.Error.Code() != kafka.ErrTopicAlreadyExists {
			errorType = metrics.KafkaErrorCode(res.Error)
			return tm.NewError("Error creating topic %s: %v", res.Topic, res.Error)
		}
	}
	tm.Infof("Created topic: %s", topic)
	tm.Refresh()
	return nil
}

func (tm *TopicManager) Refresh() {
	select {
	case tm.refreshChan <- true:
	default:
	}
}

func (tm *TopicManager) Close() error {
	if tm == nil {
		return nil
	}
	close(tm.closed)
	close(tm.refreshChan)
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
				return "", "", "", fmt.Errorf("error decoding table name from topic: %s: %v", topic, err)
			}
			tableName = string(b)
		}

	} else {
		err = fmt.Errorf("topic name %s doesn't match pattern %s", topic, topicPattern.String())
	}
	return
}

func MakeTopicId(destinationId, mode, tableName string, checkLength bool) (string, error) {
	if mode == retryTopicMode || mode == deadTopicMode {
		tableName = allTablesToken
	}
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
