package app

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jitsucom/bulker/bulkerapp/metrics"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/jitsubase/safego"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// retryTimeHeader - time of scheduled retry
const retryTimeHeader = "retry_time"
const retriesCountHeader = "retries"
const originalTopicHeader = "original_topic"
const errorHeader = "error"

const pauseHeartBeatInterval = 120 * time.Second

type BatchFunction func(destination *Destination, batchNum, batchSize, retryBatchSize int, highOffset int64) (counters BatchCounters, nextBatch bool, err error)

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
	*AbstractConsumer
	repository      *Repository
	destinationId   string
	batchPeriodSec  int
	consumerConfig  kafka.ConfigMap
	consumer        atomic.Pointer[kafka.Consumer]
	producerConfig  kafka.ConfigMap
	producer        atomic.Pointer[kafka.Producer]
	mode            string
	tableName       string
	waitForMessages time.Duration

	closed chan struct{}

	running atomic.Bool

	//AbstractBatchConsumer marked as no longer needed. We cannot close it immediately because it can be in the middle of processing batch
	retired atomic.Bool
	//idle AbstractBatchConsumer that is not running any batch jobs. retired idle consumer automatically closes itself
	idle atomic.Bool
	//consumer can be paused between batches(idle) and also can be paused during loading batch to destination(not idle)
	paused        atomic.Bool
	resumeChannel chan struct{}

	batchFunc BatchFunction
}

func NewAbstractBatchConsumer(repository *Repository, destinationId string, batchPeriodSec int, topicId, mode string, config *Config, kafkaConfig *kafka.ConfigMap, bulkerProducer *Producer) (*AbstractBatchConsumer, error) {
	abstract := NewAbstractConsumer(config, repository, topicId, bulkerProducer)
	var tableName string
	var err error
	if destinationId != "" {
		_, _, tableName, err = ParseTopicId(topicId)
		if err != nil {
			metrics.ConsumerErrors(topicId, mode, "INVALID_TOPIC", "INVALID_TOPIC", "failed to parse topic").Inc()
			return nil, abstract.NewError("Failed to parse topic: %v", err)
		}
	}

	consumerConfig := kafka.ConfigMap(utils.MapPutAll(kafka.ConfigMap{
		"group.id":                      topicId,
		"auto.offset.reset":             "earliest",
		"allow.auto.create.topics":      false,
		"group.instance.id":             abstract.GetInstanceId(),
		"enable.auto.commit":            false,
		"partition.assignment.strategy": config.KafkaConsumerPartitionsAssigmentStrategy,
		"isolation.level":               "read_committed",
		"session.timeout.ms":            config.KafkaSessionTimeoutMs,
		"max.poll.interval.ms":          config.KafkaMaxPollIntervalMs,
	}, *kafkaConfig))
	consumer, err := kafka.NewConsumer(&consumerConfig)
	if err != nil {
		metrics.ConsumerErrors(topicId, mode, destinationId, tableName, metrics.KafkaErrorCode(err)).Inc()
		return nil, abstract.NewError("Error creating consumer: %v", err)
	}
	producerConfig := kafka.ConfigMap(utils.MapPutAll(kafka.ConfigMap{
		"transactional.id": fmt.Sprintf("%s_failed_%s", topicId, config.InstanceId),
	}, *kafkaConfig))

	bc := &AbstractBatchConsumer{
		AbstractConsumer: abstract,
		repository:       repository,
		destinationId:    destinationId,
		tableName:        tableName,
		batchPeriodSec:   batchPeriodSec,
		mode:             mode,
		consumerConfig:   consumerConfig,
		producerConfig:   producerConfig,
		waitForMessages:  time.Duration(config.BatchRunnerWaitForMessagesSec) * time.Second,
		closed:           make(chan struct{}),
		resumeChannel:    make(chan struct{}),
	}
	bc.consumer.Store(consumer)
	bc.idle.Store(true)

	err = consumer.Subscribe(topicId, bc.rebalanceCallback)
	if err != nil {
		metrics.ConsumerErrors(topicId, mode, destinationId, tableName, metrics.KafkaErrorCode(err)).Inc()
		_ = consumer.Close()
		return nil, abstract.NewError("Failed to subscribe to topic: %v", err)
	}

	// Delivery reports channel for 'failed' producer messages
	safego.RunWithRestart(func() {
		for {
			producer := bc.producer.Load()
			if producer == nil {
				time.Sleep(time.Second * 10)
				continue
			}
			select {
			case <-bc.closed:
				bc.Infof("Closing producer.")
				producer.Close()
				return
			case e := <-producer.Events():
				switch ev := e.(type) {
				case *kafka.Message:
					messageId := GetKafkaHeader(ev, MessageIdHeader)
					if ev.TopicPartition.Error != nil {
						metrics.ProducerMessages(ProducerMessageLabels(*ev.TopicPartition.Topic, "error", metrics.KafkaErrorCode(ev.TopicPartition.Error))).Inc()
						bc.Errorf("Error sending message (ID: %s) to kafka topic %s: %s", messageId, *ev.TopicPartition.Topic, ev.TopicPartition.Error.Error())
					} else {
						metrics.ProducerMessages(ProducerMessageLabels(*ev.TopicPartition.Topic, "delivered", "")).Inc()
						bc.Debugf("Message ID: %s delivered to topic %s [%d] at offset %v", messageId, *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
					}
					//case kafka.Error:
					//	bc.Errorf("Producer error: %v", ev)
				}
			}
		}
	})
	return bc, nil
}

func (bc *AbstractBatchConsumer) initProducer() *kafka.Producer {
	producer := bc.producer.Load()
	if producer != nil {
		return producer
	}
	bc.Infof("Setting up transactional producer.")
	producer, err := kafka.NewProducer(&bc.producerConfig)
	if err != nil {
		metrics.ConsumerErrors(bc.topicId, bc.mode, bc.destinationId, bc.tableName, metrics.KafkaErrorCode(err)).Inc()
		panic(bc.NewError("error creating kafka producer: %v", err))
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	//enable transactions support for producer
	err = producer.InitTransactions(ctx)
	if err != nil {
		metrics.ConsumerErrors(bc.topicId, bc.mode, bc.destinationId, bc.tableName, metrics.KafkaErrorCode(err)).Inc()
		panic(bc.NewError("error initializing kafka producer transactions for 'failed' producer: %v", err))
	}
	bc.producer.Store(producer)
	return producer
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
	if bc.running.CompareAndSwap(false, true) {
		defer func() {
			bc.running.Store(false)
		}()
		_, _ = bc.ConsumeAll()
	} else {
		bc.Warnf("Previous job is still running")
	}
}

func (bc *AbstractBatchConsumer) ConsumeAll() (counters BatchCounters, err error) {
	bc.Lock()
	defer bc.Unlock()
	if bc.retired.Load() {
		bc.Errorf("No messages were consumed. Consumer is retired.")
		return BatchCounters{}, bc.NewError("Consumer is retired")
	}
	bc.Debugf("Starting consuming messages from topic")
	bc.idle.Store(false)
	defer func() {
		bc.idle.Store(true)
		bc.pause()
		bc.countersMetric(counters)
		if err != nil {
			metrics.ConsumerRuns(bc.topicId, bc.mode, bc.destinationId, bc.tableName, "fail").Inc()
			bc.Errorf("Consume finished with error: %v stats: %s", err, counters.String())
		} else {
			metrics.ConsumerRuns(bc.topicId, bc.mode, bc.destinationId, bc.tableName, "success").Inc()
			if counters.processed > 0 {
				bc.Infof("Successfully %s", counters.String())
			} else {
				countersString := counters.String()
				if countersString != "" {
					countersString = ": " + countersString
					bc.Infof("No messages were processed%s", countersString)
				} else {
					bc.Debugf("No messages were processed")
				}
			}
		}
	}()
	streamOptions := &bulker.StreamOptions{}
	var destination *Destination
	if bc.destinationId != "" {
		destination = bc.repository.LeaseDestination(bc.destinationId)
		if destination == nil {
			bc.Retire()
			return BatchCounters{}, bc.NewError("destination not found: %s. Retiring consumer", bc.destinationId)
		}
		streamOptions = destination.streamOptions
		defer func() {
			destination.Release()
		}()
	}

	maxBatchSize := bulker.BatchSizeOption.Get(streamOptions)
	if maxBatchSize <= 0 {
		maxBatchSize = bc.config.BatchRunnerDefaultBatchSize
	}

	retryBatchSize := bulker.RetryBatchSizeOption.Get(streamOptions)
	if retryBatchSize <= 0 {
		retryBatchSize = int(float64(maxBatchSize) * bc.config.BatchRunnerDefaultRetryBatchFraction)
	}
	_, highOffset, err := bc.consumer.Load().QueryWatermarkOffsets(bc.topicId, 0, 10_000)
	if err != nil {
		bc.errorMetric("query_watermark_failed")
		return BatchCounters{}, bc.NewError("Failed to query watermark offsets: %v", err)
	}
	batchNumber := 1
	for {
		if bc.retired.Load() {
			return
		}
		batchStats, nextBatch, err2 := bc.processBatch(destination, batchNumber, maxBatchSize, retryBatchSize, highOffset)
		if err2 != nil {
			if nextBatch {
				bc.Errorf("Batch finished with error: %v stats: %s nextBatch: %t", err2, batchStats, nextBatch)
			}
		}
		counters.accumulate(batchStats)
		if !nextBatch {
			err = err2
			return
		}
		batchNumber++
	}
}

func (bc *AbstractBatchConsumer) close() error {
	select {
	case <-bc.closed:
	default:
		close(bc.closed)
	}
	return bc.consumer.Load().Close()
}

func (bc *AbstractBatchConsumer) processBatch(destination *Destination, batchNum, batchSize, retryBatchSize int, highOffset int64) (counters BatchCounters, nextBath bool, err error) {
	bc.resume()
	return bc.batchFunc(destination, batchNum, batchSize, retryBatchSize, highOffset)
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

	safego.RunWithRestart(func() {
		errorReported := false
		//this loop keeps heatbeating consumer to prevent it from being kicked out from group
		pauseTicker := time.NewTicker(time.Duration(bc.config.KafkaMaxPollIntervalMs) * time.Millisecond / 2)
		defer pauseTicker.Stop()
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
			case <-pauseTicker.C:
			}
			message, err := bc.consumer.Load().ReadMessage(bc.waitForMessages)
			if err != nil {
				kafkaErr := err.(kafka.Error)
				if kafkaErr.Code() == kafka.ErrTimedOut {
					bc.Debugf("Consumer paused. Heartbeat sent.")
					continue
				}
				bc.errorMetric("error_while_paused")
				if !errorReported {
					bc.Errorf("Error on paused consumer: %v", kafkaErr)
					errorReported = true
				}
				if kafkaErr.IsRetriable() {
					time.Sleep(10 * time.Second)
				} else {
					bc.restartConsumer()
				}
			} else if message != nil {
				bc.Debugf("Unexpected message on paused consumer: %v", message)
				//If message slipped through pause, rollback offset and make sure consumer is paused
				_, err = bc.consumer.Load().SeekPartitions([]kafka.TopicPartition{message.TopicPartition})
				if err != nil {
					bc.errorMetric("ROLLBACK_ON_PAUSE_ERR")
					bc.SystemErrorf("Failed to rollback offset on paused consumer: %v", err)
				}
				bc.pauseKafkaConsumer()
			}
		}
	})
}

func (bc *AbstractBatchConsumer) restartConsumer() {
	if bc.retired.Load() {
		return
	}
	bc.Infof("Restarting consumer")
	go func(c *kafka.Consumer) {
		bc.Infof("Closing previous consumer")
		err := c.Close()
		bc.Infof("Previous consumer closed: %v", err)
	}(bc.consumer.Load())

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	// for faster reaction on retiring
	pauseTicker := time.NewTicker(1 * time.Second)
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
				bc.errorMetric("consumer_error:" + metrics.KafkaErrorCode(err))
				bc.Errorf("Error creating kafka consumer: %v", err)
				break
			}
			err = consumer.SubscribeTopics([]string{bc.topicId}, bc.rebalanceCallback)
			if err != nil {
				bc.errorMetric("consumer_error:" + metrics.KafkaErrorCode(err))
				_ = consumer.Close()
				bc.Errorf("Failed to subscribe to topic: %v", err)
				break
			}
			bc.consumer.Store(consumer)
			bc.Infof("Restarted successfully")
			return
		}
	}
}

func (bc *AbstractBatchConsumer) pauseKafkaConsumer() {
	partitions, err := bc.consumer.Load().Assignment()
	if len(partitions) > 0 {
		err = bc.consumer.Load().Pause(partitions)
	}
	if err != nil {
		bc.errorMetric("pause_error")
		bc.SystemErrorf("Failed to pause kafka consumer: %v", err)
	} else {
		if len(partitions) > 0 {
			bc.Debugf("Consumer paused.")
		}
		// otherwise rebalanceCallback will handle pausing
	}
}

func (bc *AbstractBatchConsumer) rebalanceCallback(consumer *kafka.Consumer, event kafka.Event) error {
	assignedParts, ok := event.(kafka.AssignedPartitions)
	bc.Debugf("Rebalance event: %v . Paused: %t", event, bc.paused.Load())
	if ok && bc.paused.Load() {
		err := consumer.Pause(assignedParts.Partitions)
		if err != nil {
			bc.errorMetric("pause_error")
			bc.SystemErrorf("Failed to pause kafka consumer: %v", err)
			return err
		} else {
			bc.Debugf("Consumer paused.")
		}
	}
	return nil
}

func (bc *AbstractBatchConsumer) resume() {
	if !bc.paused.Load() {
		return
	}
	var err error
	defer func() {
		if err != nil {
			bc.errorMetric("resume_error")
			bc.SystemErrorf("failed to resume kafka consumer.: %v", err)
		}
	}()
	partitions, err := bc.consumer.Load().Assignment()
	if err != nil {
		return
	}
	select {
	case bc.resumeChannel <- struct{}{}:
		err = bc.consumer.Load().Resume(partitions)
	case <-time.After(time.Duration(bc.config.KafkaMaxPollIntervalMs) * time.Millisecond):
		err = bc.NewError("Resume timeout.")
		//return bc.consumer.Resume(partitions)
	}
}

// Retire Mark consumer as retired
// Consumer will close itself when com
func (bc *AbstractBatchConsumer) Retire() {
	bc.Infof("Retiring %s consumer", bc.mode)
	bc.retired.Store(true)
}
func (bc *AbstractBatchConsumer) errorMetric(errorType string) {
	metrics.ConsumerErrors(bc.topicId, bc.mode, bc.destinationId, bc.tableName, errorType).Inc()
}

func (bc *AbstractBatchConsumer) countersMetric(counters BatchCounters) {
	countersValue := reflect.ValueOf(counters)
	countersType := countersValue.Type()
	for i := 0; i < countersValue.NumField(); i++ {
		value := countersValue.Field(i).Int()
		if value > 0 {
			metrics.ConsumerMessages(bc.topicId, bc.mode, bc.destinationId, bc.tableName, countersType.Field(i).Name).Add(float64(value))
		}
	}
}

type BatchCounters struct {
	consumed        int
	skipped         int
	processed       int
	notReadyReadded int
	retryScheduled  int
	retried         int
	deadLettered    int
	failed          int
}

// accumulate stats from batch
func (bs *BatchCounters) accumulate(batchStats BatchCounters) {
	bs.consumed += batchStats.consumed
	bs.skipped += batchStats.skipped
	bs.processed += batchStats.processed
	bs.notReadyReadded += batchStats.notReadyReadded
	bs.retryScheduled += batchStats.retryScheduled
	bs.deadLettered += batchStats.deadLettered
	bs.retried += batchStats.retried
}

// to string
func (bs *BatchCounters) String() string {
	// print non-zero values
	var sb strings.Builder
	countersValue := reflect.ValueOf(*bs)
	countersType := countersValue.Type()
	for i := 0; i < countersValue.NumField(); i++ {
		value := countersValue.Field(i).Int()
		if value > 0 {
			sb.WriteString(fmt.Sprintf("%s: %d ", countersType.Field(i).Name, value))
		}
	}
	return sb.String()
}
