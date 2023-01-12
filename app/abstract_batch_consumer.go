package app

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jitsucom/bulker/app/metrics"
	"github.com/jitsucom/bulker/base/objects"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/bulker"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const retryTimeHeader = "retry_time"
const retriesCountHeader = "retries"
const originalTopicHeader = "original_topic"

const pauseHeartBeatInterval = 1 * time.Second

type BatchFunction func(destination *Destination, batchSize, retryBatchSize int) (counters BatchCounters, nextBatch bool, err error)

type BatchConsumer interface {
	RunJob()
	ConsumeAll() (consumed BatchCounters, err error)
	Retire()
	BatchPeriodSec() int
	UpdateBatchPeriod(batchPeriodSec int)
	TopicId() string
}

type AbstractBatchConsumer struct {
	sync.Mutex
	objects.ServiceBase
	config         *AppConfig
	repository     *Repository
	destinationId  string
	batchPeriodSec int
	consumerConfig kafka.ConfigMap
	consumer       *kafka.Consumer
	//it is not allowed to close consumer twice
	consumerClosed  bool
	producer        *kafka.Producer
	topicId         string
	tableName       string
	waitForMessages time.Duration
	closed          chan struct{}

	//AbstractBatchConsumer marked as no longer needed. We cannot close it immediately because it can be in the middle of processing batch
	retired atomic.Bool
	//idle AbstractBatchConsumer that is not running any batch jobs. It can be closed immediately. retired idle consumer automatically closes itself
	idle atomic.Bool

	paused        atomic.Bool
	resumeChannel chan struct{}

	batchFunc BatchFunction
}

func NewAbstractBatchConsumer(repository *Repository, destinationId string, batchPeriodSec int, topicId string, config *AppConfig, kafkaConfig *kafka.ConfigMap) (*AbstractBatchConsumer, error) {
	base := objects.NewServiceBase(topicId)
	_, _, tableName, err := ParseTopicId(topicId)
	if err != nil {
		metrics.BatchConsumerErrors("INVALID_TOPIC", "INVALID_TOPIC:"+topicId, "failed to parse topic").Inc()
		return nil, base.NewError("Failed to parse topic: %w", err)
	}
	consumerConfig := kafka.ConfigMap(utils.MapPutAll(kafka.ConfigMap{
		"group.id":                      topicId,
		"auto.offset.reset":             "earliest",
		"group.instance.id":             config.InstanceId,
		"enable.auto.commit":            false,
		"partition.assignment.strategy": config.KafkaConsumerPartitionsAssigmentStrategy,
		"isolation.level":               "read_committed",
	}, *kafkaConfig))
	if config.BatchRunnerWaitForMessagesSec > 30 {
		_ = consumerConfig.SetKey("session.timeout.ms", config.BatchRunnerWaitForMessagesSec*1000*2)
	}
	consumer, err := kafka.NewConsumer(&consumerConfig)
	if err != nil {
		metrics.BatchConsumerErrors(destinationId, tableName, metrics.KafkaErrorCode(err)).Inc()
		return nil, base.NewError("Error creating consumer: %w", err)
	}
	// check topic partitions count
	metadata, err := consumer.GetMetadata(&topicId, false, 10000)
	if err != nil {
		metrics.BatchConsumerErrors(destinationId, tableName, metrics.KafkaErrorCode(err)).Inc()
		return nil, base.NewError("Failed to get consumer metadata: %w", err)
	}
	for _, topic := range metadata.Topics {
		if topic.Topic == topicId {
			if len(topic.Partitions) > 1 {
				metrics.BatchConsumerErrors(destinationId, tableName, "invalid_partitions_count").Inc()
				return nil, base.NewError("Topic has more than 1 partition. Batch Consumer supports only topics with a single partition")
			}
			break
		}
	}

	err = consumer.Subscribe(topicId, nil)
	if err != nil {
		metrics.BatchConsumerErrors(destinationId, tableName, metrics.KafkaErrorCode(err)).Inc()
		_ = consumer.Close()
		return nil, base.NewError("Failed to subscribe to topic: %w", err)
	}

	producerConfig := kafka.ConfigMap(utils.MapPutAll(kafka.ConfigMap{
		"transactional.id": fmt.Sprintf("%s_failed_%s", topicId, config.InstanceId),
	}, *kafkaConfig))
	producer, err := kafka.NewProducer(&producerConfig)
	if err != nil {
		metrics.BatchConsumerErrors(destinationId, tableName, metrics.KafkaErrorCode(err)).Inc()
		_ = consumer.Close()
		return nil, base.NewError("error creating kafka producer: %w", err)
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	//enable transactions support for producer
	err = producer.InitTransactions(ctx)
	if err != nil {
		metrics.BatchConsumerErrors(destinationId, tableName, metrics.KafkaErrorCode(err)).Inc()
		_ = consumer.Close()
		return nil, base.NewError("error initializing kafka producer transactions for 'failed' producer: %w", err)
	}
	bc := &AbstractBatchConsumer{
		ServiceBase:     base,
		config:          config,
		repository:      repository,
		destinationId:   destinationId,
		tableName:       tableName,
		batchPeriodSec:  batchPeriodSec,
		topicId:         topicId,
		consumerConfig:  consumerConfig,
		consumer:        consumer,
		producer:        producer,
		waitForMessages: time.Duration(config.BatchRunnerWaitForMessagesSec) * time.Second,
		closed:          make(chan struct{}),
		resumeChannel:   make(chan struct{}),
	}

	// Delivery reports channel for 'failed' producer messages
	go func() {
		for {
			select {
			case <-bc.closed:
				return
			case e := <-bc.producer.Events():
				switch ev := e.(type) {
				case *kafka.Message:
					messageId := GetKafkaHeader(ev, MessageIdHeader)
					if ev.TopicPartition.Error != nil {
						bc.Errorf("Error sending message (ID: %s) to kafka topic %s: %s", messageId, ev.TopicPartition.Topic, ev.TopicPartition.Error.Error())
					} else {
						bc.Debugf("Message ID: %s delivered to topic %s [%d] at offset %v", messageId, *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
					}
					//case kafka.Error:
					//	bc.Errorf("Producer error: %w", ev)
				}
			}
		}
	}()
	return bc, nil
}

func (bc *AbstractBatchConsumer) BatchPeriodSec() int {
	return bc.batchPeriodSec
}

func (bc *AbstractBatchConsumer) UpdateBatchPeriod(batchPeriodSec int) {
	bc.batchPeriodSec = batchPeriodSec
}

func (bc *AbstractBatchConsumer) TopicId() string {
	return bc.topicId
}

func (bc *AbstractBatchConsumer) RunJob() {
	_, _ = bc.ConsumeAll()
}

func (bc *AbstractBatchConsumer) ConsumeAll() (consumed BatchCounters, err error) {
	bc.Lock()
	defer bc.Unlock()
	if bc.consumerClosed {
		bc.Errorf("No messages were consumed. Consumer is closed.")
		return BatchCounters{}, bc.NewError("Consumer is closed")
	}
	bc.Debugf("Starting consuming messages from topic")
	metrics.BatchConsumerBatchRuns(bc.destinationId, bc.tableName).Inc()
	bc.idle.Store(false)
	defer func() {
		bc.idle.Store(true)
		bc.pause()
		if err != nil {
			bc.Errorf("Consume finished with error: %w stats: %s", err, consumed)
		} else {
			if consumed.processed > 0 {
				bc.Infof("Successfully %s", consumed)
			} else {
				bc.Infof("No messages were processed: %s", consumed)
			}
		}
	}()
	destination := bc.repository.LeaseDestination(bc.destinationId)
	if destination == nil {
		bc.Retire()
		return BatchCounters{}, bc.NewError("destination not found: %s. Retiring consumer", bc.destinationId)
	}
	defer func() {
		destination.Release()
	}()

	retryBatchSize := bulker.RetryBatchSizeOption.Get(destination.streamOptions)
	if retryBatchSize <= 0 {
		retryBatchSize = bc.config.BatchRunnerDefaultRetryBatchSize
	}

	maxBatchSize := bulker.BatchSizeOption.Get(destination.streamOptions)
	if maxBatchSize <= 0 {
		maxBatchSize = bc.config.BatchRunnerDefaultBatchSize
	}

	for {
		if bc.retired.Load() {
			return
		}
		batchStats, nextBatch, err2 := bc.processBatch(destination, maxBatchSize, retryBatchSize)
		if err2 != nil {
			bc.Errorf("Batch finished with error: %w stats: %s nextBatch: %t", err2, batchStats, nextBatch)
		}
		consumed.accumulate(batchStats)
		if !nextBatch {
			err = err2
			return
		}
	}
}

func (bc *AbstractBatchConsumer) close() error {
	close(bc.closed)
	bc.producer.Close()
	if !bc.consumerClosed {
		err := bc.consumer.Close()
		return err
	}
	return nil
}

func (bc *AbstractBatchConsumer) processBatch(destination *Destination, batchSize, retryBatchSize int) (counters BatchCounters, nextBath bool, err error) {
	err = bc.resume()
	if err != nil {
		metrics.BatchConsumerErrors(bc.destinationId, bc.tableName, "resume_error").Inc()
		err = bc.NewError("failed to resume kafka consumer to process batch: %w", err)
		return
	}
	return bc.batchFunc(destination, batchSize, retryBatchSize)
}

// pause consumer.
func (bc *AbstractBatchConsumer) pause() {
	if bc.idle.Load() && bc.retired.Load() {
		// Close retired idling consumer
		bc.Infof("Consumer is retired. Closing")
		_ = bc.close()
		return
	}
	if !bc.paused.CompareAndSwap(false, true) {
		return
	}
	bc.pauseKafkaConsumer()

	go func() {
		errorReported := false
		//this loop keeps heatbeating consumer to prevent it from being kicked out from group
	loop:
		for {
			if bc.idle.Load() && bc.retired.Load() {
				// Close retired idling consumer
				bc.Infof("Consumer is retired. Closing")
				_ = bc.close()
				return
			}
			select {
			case <-bc.resumeChannel:
				bc.paused.Store(false)
				bc.Debugf("Consumer resumed.")
				break loop
			default:
			}
			message, err := bc.consumer.ReadMessage(pauseHeartBeatInterval)
			if err != nil {
				kafkaErr := err.(kafka.Error)
				if kafkaErr.Code() == kafka.ErrTimedOut {
					bc.Debugf("Consumer paused. Heartbeat sent.")
					continue
				}
				metrics.BatchConsumerErrors(bc.destinationId, bc.tableName, "error_while_paused").Inc()
				if !errorReported {
					bc.Errorf("Error on paused consumer: %v", kafkaErr)
					errorReported = true
				}
				if kafkaErr.IsRetriable() {
					time.Sleep(pauseHeartBeatInterval)
				} else {
					bc.restartConsumer()
				}
			} else if message != nil {
				bc.Debugf("Unexpected message on paused consumer: %v", message)
				//If message slipped through pause, rollback offset and make sure consumer is paused
				err = bc.consumer.Seek(message.TopicPartition, 0)
				if err != nil {
					metrics.BatchConsumerErrors(bc.destinationId, bc.tableName, "ROLLBACK_ON_PAUSE_ERR").Inc()
					bc.SystemErrorf("Failed to rollback offset on paused consumer: %v", err)
				}
				bc.pauseKafkaConsumer()
			}
		}
	}()
}

func (bc *AbstractBatchConsumer) restartConsumer() {
	bc.Lock()
	err := bc.consumer.Close()
	bc.Infof("Previous consumer closed: %v", err)
	bc.consumerClosed = true
	bc.Unlock()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	// for faster reaction on retiring
	pauseTicker := time.NewTicker(pauseHeartBeatInterval)
	defer pauseTicker.Stop()

	for {
		select {
		case <-pauseTicker.C:
			if bc.idle.Load() && bc.retired.Load() {
				return
			}
		case <-ticker.C:
			bc.Infof("Restarting consumer")
			consumer, err := kafka.NewConsumer(&bc.consumerConfig)
			if err != nil {
				metrics.BatchConsumerErrors(bc.destinationId, bc.tableName, metrics.KafkaErrorCode(err)).Inc()
				bc.Errorf("Error creating kafka consumer: %w", err)
				break
			}
			err = consumer.SubscribeTopics([]string{bc.topicId}, nil)
			if err != nil {
				metrics.BatchConsumerErrors(bc.destinationId, bc.tableName, metrics.KafkaErrorCode(err)).Inc()
				_ = consumer.Close()
				bc.Errorf("Failed to subscribe to topic: %w", err)
				break
			}
			bc.Lock()
			bc.consumer = consumer
			bc.consumerClosed = false
			bc.Unlock()
			return
		}
	}
}

func (bc *AbstractBatchConsumer) pauseKafkaConsumer() {
	partitions, err := bc.consumer.Assignment()
	if len(partitions) > 0 {
		err = bc.consumer.Pause(partitions)
	}
	if err != nil {
		metrics.BatchConsumerErrors(bc.destinationId, bc.tableName, "pause_error").Inc()
		bc.SystemErrorf("Failed to pause kafka consumer: %w", err)
	} else {
		if len(partitions) > 0 {
			bc.Debugf("Consumer paused.")
		}
	}
}

func (bc *AbstractBatchConsumer) resume() (err error) {
	if !bc.paused.Load() {
		return nil
	}
	partitions, err := bc.consumer.Assignment()
	if err != nil {
		return err
	}
	select {
	case bc.resumeChannel <- struct{}{}:
		return bc.consumer.Resume(partitions)
	case <-time.After(pauseHeartBeatInterval * 2):
		return bc.NewError("Resume timeout.")
		//return bc.consumer.Resume(partitions)
	}
}

// Retire Mark consumer as retired
// Consumer will close itself when com
func (bc *AbstractBatchConsumer) Retire() {
	bc.Infof("Retiring batch consumer")
	bc.retired.Store(true)
}

type BatchCounters struct {
	consumed        int
	skipped         int
	processed       int
	notReadyReadded int
	retryScheduled  int
	deadLettered    int
}

// accumulate stats from batch
func (bs *BatchCounters) accumulate(batchStats BatchCounters) {
	bs.consumed += batchStats.consumed
	bs.skipped += batchStats.skipped
	bs.processed += batchStats.processed
	bs.notReadyReadded += batchStats.notReadyReadded
	bs.retryScheduled += batchStats.retryScheduled
	bs.deadLettered += batchStats.deadLettered
}

// to string
func (bs BatchCounters) String() string {
	// print non-zero values
	var sb strings.Builder
	if bs.consumed > 0 {
		sb.WriteString(fmt.Sprintf("consumed: %d ", bs.consumed))
	}
	if bs.skipped > 0 {
		sb.WriteString(fmt.Sprintf("skipped: %d ", bs.skipped))
	}
	if bs.processed > 0 {
		sb.WriteString(fmt.Sprintf("processed: %d ", bs.processed))
	}
	if bs.notReadyReadded > 0 {
		sb.WriteString(fmt.Sprintf("notReadyReadded: %d ", bs.notReadyReadded))
	}
	if bs.retryScheduled > 0 {
		sb.WriteString(fmt.Sprintf("retryScheduled: %d ", bs.retryScheduled))
	}
	if bs.deadLettered > 0 {
		sb.WriteString(fmt.Sprintf("deadLettered: %d ", bs.deadLettered))
	}
	return sb.String()
}
