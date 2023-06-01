package app

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jitsucom/bulker/app/metrics"
	"github.com/jitsucom/bulker/base/objects"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/bulker"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const retryTimeHeader = "retry_time"
const retriesCountHeader = "retries"
const originalTopicHeader = "original_topic"

const pauseHeartBeatInterval = 1 * time.Second

type BatchFunction func(destination *Destination, batchNum, batchSize, retryBatchSize int) (counters BatchCounters, nextBatch bool, err error)

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
	consumerClosed  atomic.Bool
	producer        *kafka.Producer
	topicId         string
	mode            string
	tableName       string
	waitForMessages time.Duration

	closed chan struct{}

	//AbstractBatchConsumer marked as no longer needed. We cannot close it immediately because it can be in the middle of processing batch
	retired atomic.Bool
	//idle AbstractBatchConsumer that is not running any batch jobs. retired idle consumer automatically closes itself
	idle atomic.Bool
	//consumer can be paused between batches(idle) and also can be paused during loading batch to destination(not idle)
	paused        atomic.Bool
	resumeChannel chan struct{}

	batchFunc BatchFunction
}

func NewAbstractBatchConsumer(repository *Repository, destinationId string, batchPeriodSec int, topicId, mode string, config *AppConfig, kafkaConfig *kafka.ConfigMap) (*AbstractBatchConsumer, error) {
	base := objects.NewServiceBase(topicId)
	_, _, tableName, err := ParseTopicId(topicId)
	if err != nil {
		metrics.ConsumerErrors(topicId, mode, "INVALID_TOPIC", "INVALID_TOPIC", "failed to parse topic").Inc()
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
		metrics.ConsumerErrors(topicId, mode, destinationId, tableName, metrics.KafkaErrorCode(err)).Inc()
		return nil, base.NewError("Error creating consumer: %w", err)
	}
	// check topic partitions count
	metadata, err := consumer.GetMetadata(&topicId, false, 10000)
	if err != nil {
		metrics.ConsumerErrors(topicId, mode, destinationId, tableName, metrics.KafkaErrorCode(err)).Inc()
		return nil, base.NewError("Failed to get consumer metadata: %w", err)
	}
	for _, topic := range metadata.Topics {
		if topic.Topic == topicId {
			if len(topic.Partitions) > 1 {
				metrics.ConsumerErrors(topicId, mode, destinationId, tableName, "invalid_partitions_count").Inc()
				return nil, base.NewError("Topic has more than 1 partition. Batch Consumer supports only topics with a single partition")
			}
			break
		}
	}

	err = consumer.Subscribe(topicId, nil)
	if err != nil {
		metrics.ConsumerErrors(topicId, mode, destinationId, tableName, metrics.KafkaErrorCode(err)).Inc()
		_ = consumer.Close()
		return nil, base.NewError("Failed to subscribe to topic: %w", err)
	}

	producerConfig := kafka.ConfigMap(utils.MapPutAll(kafka.ConfigMap{
		"transactional.id": fmt.Sprintf("%s_failed_%s", topicId, config.InstanceId),
	}, *kafkaConfig))
	producer, err := kafka.NewProducer(&producerConfig)
	if err != nil {
		metrics.ConsumerErrors(topicId, mode, destinationId, tableName, metrics.KafkaErrorCode(err)).Inc()
		_ = consumer.Close()
		return nil, base.NewError("error creating kafka producer: %w", err)
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	//enable transactions support for producer
	err = producer.InitTransactions(ctx)
	if err != nil {
		metrics.ConsumerErrors(topicId, mode, destinationId, tableName, metrics.KafkaErrorCode(err)).Inc()
		_ = consumer.Close()
		return nil, base.NewError("error initializing kafka producer transactions for 'failed' producer: %w", err)
	}
	idle := atomic.Bool{}
	idle.Store(true)
	bc := &AbstractBatchConsumer{
		ServiceBase:     base,
		config:          config,
		repository:      repository,
		destinationId:   destinationId,
		tableName:       tableName,
		batchPeriodSec:  batchPeriodSec,
		topicId:         topicId,
		mode:            mode,
		consumerConfig:  consumerConfig,
		consumer:        consumer,
		producer:        producer,
		waitForMessages: time.Duration(config.BatchRunnerWaitForMessagesSec) * time.Second,
		closed:          make(chan struct{}),
		resumeChannel:   make(chan struct{}),
		idle:            idle,
	}

	// Delivery reports channel for 'failed' producer messages
	go func() {
		for {
			select {
			case <-bc.closed:
				bc.Infof("Closing producer.")
				bc.producer.Close()
				return
			case e := <-bc.producer.Events():
				switch ev := e.(type) {
				case *kafka.Message:
					messageId := GetKafkaHeader(ev, MessageIdHeader)
					if ev.TopicPartition.Error != nil {
						bc.Errorf("Error sending message (ID: %s) to kafka topic %s: %s", messageId, *ev.TopicPartition.Topic, ev.TopicPartition.Error.Error())
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

func (bc *AbstractBatchConsumer) ConsumeAll() (counters BatchCounters, err error) {
	bc.Lock()
	defer bc.Unlock()
	if bc.consumerClosed.Load() {
		bc.Errorf("No messages were consumed. Consumer is closed.")
		return BatchCounters{}, bc.NewError("Consumer is closed")
	}
	bc.Debugf("Starting consuming messages from topic")
	bc.idle.Store(false)
	defer func() {
		bc.idle.Store(true)
		bc.pause()
		bc.countersMetric(counters)
		if err != nil {
			metrics.ConsumerRuns(bc.topicId, bc.mode, bc.destinationId, bc.tableName, "fail").Inc()
			bc.Errorf("Consume finished with error: %w stats: %s", err, counters)
		} else {
			metrics.ConsumerRuns(bc.topicId, bc.mode, bc.destinationId, bc.tableName, "success").Inc()
			if counters.processed > 0 {
				bc.Infof("Successfully %s", counters)
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
	destination := bc.repository.LeaseDestination(bc.destinationId)
	if destination == nil {
		bc.Retire()
		return BatchCounters{}, bc.NewError("destination not found: %s. Retiring consumer", bc.destinationId)
	}
	defer func() {
		destination.Release()
	}()

	maxBatchSize := bulker.BatchSizeOption.Get(destination.streamOptions)
	if maxBatchSize <= 0 {
		maxBatchSize = bc.config.BatchRunnerDefaultBatchSize
	}

	retryBatchSize := bulker.RetryBatchSizeOption.Get(destination.streamOptions)
	if retryBatchSize <= 0 {
		retryBatchSize = int(float64(maxBatchSize) * bc.config.BatchRunnerDefaultRetryBatchFraction)
	}

	batchNumber := 1
	for {
		if bc.retired.Load() {
			return
		}
		batchStats, nextBatch, err2 := bc.processBatch(destination, batchNumber, maxBatchSize, retryBatchSize)
		if err2 != nil {
			bc.Errorf("Batch finished with error: %w stats: %s nextBatch: %t", err2, batchStats, nextBatch)
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
	if bc.consumerClosed.CompareAndSwap(false, true) {
		err := bc.consumer.Close()
		return err
	}
	return nil
}

func (bc *AbstractBatchConsumer) processBatch(destination *Destination, batchNum, batchSize, retryBatchSize int) (counters BatchCounters, nextBath bool, err error) {
	err = bc.resume()
	if err != nil {
		bc.errorMetric("resume_error")
		err = bc.NewError("failed to resume kafka consumer to process batch: %w", err)
		return
	}
	return bc.batchFunc(destination, batchNum, batchSize, retryBatchSize)
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
				bc.errorMetric("error_while_paused")
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
				err = bc.consumer.Seek(message.TopicPartition, 10_000)
				if err != nil {
					bc.errorMetric("ROLLBACK_ON_PAUSE_ERR")
					bc.SystemErrorf("Failed to rollback offset on paused consumer: %v", err)
				}
				bc.pauseKafkaConsumer()
			}
		}
	}()
}

func (bc *AbstractBatchConsumer) restartConsumer() {
	if bc.retired.Load() {
		return
	}
	if bc.consumerClosed.CompareAndSwap(false, true) {
		err := bc.consumer.Close()
		bc.Infof("Previous consumer closed: %v", err)
	}

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
				bc.errorMetric("consumer_error:" + metrics.KafkaErrorCode(err))
				bc.Errorf("Error creating kafka consumer: %w", err)
				break
			}
			err = consumer.SubscribeTopics([]string{bc.topicId}, nil)
			if err != nil {
				bc.errorMetric("consumer_error:" + metrics.KafkaErrorCode(err))
				_ = consumer.Close()
				bc.Errorf("Failed to subscribe to topic: %w", err)
				break
			}
			bc.Lock()
			bc.consumer = consumer
			bc.consumerClosed.Store(false)
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
		bc.errorMetric("pause_error")
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
}

// to string
func (bs BatchCounters) String() string {
	// print non-zero values
	var sb strings.Builder
	countersValue := reflect.ValueOf(bs)
	countersType := countersValue.Type()
	for i := 0; i < countersValue.NumField(); i++ {
		value := countersValue.Field(i).Int()
		if value > 0 {
			sb.WriteString(fmt.Sprintf("%s: %d ", countersType.Field(i).Name, value))
		}
	}
	return sb.String()
}
