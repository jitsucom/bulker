package app

import (
	"bytes"
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/objects"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/types"
	jsoniter "github.com/json-iterator/go"
	"sync"
	"sync/atomic"
	"time"
)

const pauseHeartBeatInterval = 1 * time.Second

type BatchConsumer struct {
	sync.Mutex
	objects.ServiceBase
	config         *AppConfig
	repository     *Repository
	destinationId  string
	batchPeriodSec int
	//bulkerStream bulker.BulkerStream
	consumerConfig kafka.ConfigMap
	consumer       *kafka.Consumer
	//it is not allowed to close consumer twice
	consumerClosed bool
	failedProducer *kafka.Producer

	topicId         string
	tableName       string
	waitForMessages time.Duration
	closed          chan struct{}

	//BatchConsumer marked as no longer needed. We cannot close it immediately because it can be in the middle of processing batch
	retired atomic.Bool
	//idle BatchConsumer that is not running any batch jobs. It can be closed immediately. retired idle consumer automatically closes itself
	idle atomic.Bool

	paused        bool
	resumeChannel chan struct{}
}

func NewBatchConsumer(repository *Repository, destinationId string, batchPeriodSec int, topicId string, config *AppConfig, kafkaConfig *kafka.ConfigMap) (*BatchConsumer, error) {
	base := objects.NewServiceBase(topicId)
	_, _, tableName, err := ParseTopicId(topicId)
	if err != nil {
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
		return nil, base.NewError("Error creating consumer: %w", err)
	}
	// check topic partitions count
	metadata, err := consumer.GetMetadata(&topicId, false, 10000)
	if err != nil {
		return nil, base.NewError("Failed to get consumer metadata: %w", err)
	}
	for _, topic := range metadata.Topics {
		if topic.Topic == topicId {
			if len(topic.Partitions) > 1 {
				return nil, base.NewError("Topic has more than 1 partition. Batch Consumer supports only topics with a single partition")
			}
			break
		}
	}

	err = consumer.Subscribe(topicId, nil)
	if err != nil {
		_ = consumer.Close()
		return nil, base.NewError("Failed to subscribe to topic: %w", err)
	}

	producerConfig := kafka.ConfigMap(utils.MapPutAll(kafka.ConfigMap{
		"transactional.id": fmt.Sprintf("%s_failed_%s", topicId, config.InstanceId),
	}, *kafkaConfig))
	failedProducer, err := kafka.NewProducer(&producerConfig)
	if err != nil {
		_ = consumer.Close()
		return nil, base.NewError("error creating kafka producer: %w", err)
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	//enable transactions support for producer
	err = failedProducer.InitTransactions(ctx)
	if err != nil {
		_ = consumer.Close()
		return nil, base.NewError("error initializing kafka producer transactions for 'failed' producer: %w", err)
	}
	bc := &BatchConsumer{
		ServiceBase:     base,
		config:          config,
		repository:      repository,
		destinationId:   destinationId,
		tableName:       tableName,
		batchPeriodSec:  batchPeriodSec,
		topicId:         topicId,
		consumerConfig:  consumerConfig,
		consumer:        consumer,
		failedProducer:  failedProducer,
		waitForMessages: time.Duration(config.BatchRunnerWaitForMessagesSec) * time.Second,
		closed:          make(chan struct{}),
		resumeChannel:   make(chan struct{}),
	}
	bc.pause()

	// Delivery reports channel for 'failed' producer messages
	go func() {
		for {
			select {
			case <-bc.closed:
				return
			case e := <-bc.failedProducer.Events():
				switch ev := e.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						bc.Errorf("Error sending message to kafka topic %s: %w", ev.TopicPartition.Topic, ev.TopicPartition.Error)
					} else {
						bc.Infof("Message delivered to topic %s [%d] at offset %v", *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
					}
					//case kafka.Error:
					//	bc.Errorf("Producer error: %w", ev)
				}
			}
		}
	}()
	return bc, nil
}

func (bc *BatchConsumer) RunJob() {
	_, _ = bc.ConsumeAll()
}

func (bc *BatchConsumer) ConsumeAll() (consumed int, err error) {
	bc.Lock()
	defer bc.Unlock()
	if bc.consumerClosed {
		bc.Errorf("No messages were consumed. Consumer is closed.")
		return 0, bc.NewError("Consumer is closed")
	}
	bc.Infof("Starting consuming messages from topic")
	bc.idle.Store(false)
	defer func() {
		bc.idle.Store(true)
		bc.pause()
		if err != nil {
			bc.Errorf("Consume finished with error. %d messages consumed. Error: %w", consumed, err)
		} else {
			if consumed > 0 {
				bc.Infof("Successfully consumed %d messages", consumed)
			} else {
				bc.Infof("No messages were consumed")
			}
		}
	}()
	destination := bc.repository.LeaseDestination(bc.destinationId)
	if destination == nil {
		bc.Retire()
		return 0, bc.NewError("destination not found: %s. Retiring consumer", bc.destinationId)
	}
	defer func() {
		destination.Release()
	}()
	maxBatchSize := destination.config.BatchSize
	if maxBatchSize <= 0 {
		maxBatchSize = bc.config.BatchRunnerDefaultBatchSize
	}
	for {
		select {
		case <-bc.closed:
			return consumed, nil
		default:
			var batchSize int
			batchSize, err = bc.processBatch(destination, maxBatchSize)
			if err != nil {
				return
			}
			consumed += batchSize
			if batchSize < maxBatchSize {
				//we've consumed fewer events than batch size. It means that there are no more events in topic
				return
			}
			if bc.retired.Load() {
				return
			}
		}
	}
}

func (bc *BatchConsumer) close() error {
	close(bc.closed)
	bc.failedProducer.Close()
	if !bc.consumerClosed {
		err := bc.consumer.Close()
		return err
	}
	return nil
}

func (bc *BatchConsumer) processBatch(destination *Destination, batchSize int) (consumed int, err error) {
	err = bc.resume()
	if err != nil {
		err = bc.NewError("failed to resume kafka consumer to process batch: %w", err)
		return
	}
	bulkerStream, err := destination.bulker.CreateStream(bc.topicId, bc.tableName, bulker.Transactional, destination.streamOptions...)
	if err != nil {
		return 0, bc.NewError("Failed to create bulker stream: %w", err)
	}

	i := 0
	//position of last message in batch in case of failed. Needed for processFailed
	var failedPosition *kafka.TopicPartition
	defer func() {
		if err != nil && failedPosition != nil {
			err2 := bc.processFailed(failedPosition)
			if err2 != nil {
				logging.SystemError(err2)
			}
		}
	}()
	// we collect batchSize of messages but no longer than for waitForMessages period
	timeEnd := time.Now().Add(bc.waitForMessages)
	latestPosition := kafka.TopicPartition{}
	for ; i < batchSize; i++ {
		//TODO: do we heed to interrupt batch on close?

		wait := timeEnd.Sub(time.Now())
		if wait <= 0 {
			break
		}
		message, err := bc.consumer.ReadMessage(wait)
		if err != nil {
			kafkaErr := err.(kafka.Error)
			if kafkaErr.Code() == kafka.ErrTimedOut {
				// waitForMessages period is over. it's ok. considering batch as full
				break
			}
			_, _ = bulkerStream.Abort(context.Background())
			return 0, bc.NewError("Failed to consume event from topic. Retryable: %t: %w", kafkaErr.IsRetriable(), kafkaErr)
		}
		latestPosition = message.TopicPartition
		bc.Infof("%d. Message claimed: offset = %s, partition = %d, timestamp = %v, topic = %s", i, latestPosition.Offset.String(), latestPosition.Partition, message.Timestamp, *latestPosition.Topic)
		obj := types.Object{}
		dec := jsoniter.NewDecoder(bytes.NewReader(message.Value))
		dec.UseNumber()
		err = dec.Decode(&obj)
		if err == nil {
			err = bulkerStream.Consume(context.Background(), obj)
		}
		if err != nil {
			failedPosition = &latestPosition
			_, _ = bulkerStream.Abort(context.Background())
			return 0, bc.NewError("Failed to process event to bulker stream: %w", err)
		}
	}
	//we've consumed some messages. it is time to commit them
	if i > 0 {
		// we need to pause consumer to avoid kafka session timeout while loading huge batches to slow destinations
		bc.pause()

		bc.Infof("Committing %d events to destination.", i)
		_, err = bulkerStream.Complete(context.Background())
		if err != nil {
			failedPosition = &latestPosition
			return 0, bc.NewError("Failed to commit bulker stream: %w", err)
		}
		_, err = bc.consumer.Commit()
		if err != nil {
			bc.SystemErrorf("Failed to commit kafka consumer after batch was successfully committed to the destination: %w", err)
			return i, bc.NewError("Failed to commit kafka consumer: %w", err)
		}
	} else {
		_, _ = bulkerStream.Abort(context.Background())
	}
	return i, nil
}

// processFailed consumes the latest failed batch of messages and sends them to the 'failed' topic
func (bc *BatchConsumer) processFailed(failedPosition *kafka.TopicPartition) (err error) {
	defer func() {
		if err != nil {
			err = bc.NewError("Failed to put unsuccessful batch to 'failed' producer: %w", err)
		}
	}()
	err = bc.resume()
	if err != nil {
		return fmt.Errorf("failed to resume kafka consumer: %w", err)
	}
	//Get committed offset for the topic partition
	tp, err := bc.consumer.Committed([]kafka.TopicPartition{*failedPosition}, 10_000)
	if err != nil || len(tp) == 0 {
		return fmt.Errorf("failed to get committed offset for kafka consumer: %w", err)
	}
	seekPosition := tp[0]
	bc.Infof("Rolling back to last committed offset %d (failed at %d)", seekPosition.Offset, failedPosition.Offset)
	//Rollback consumer to committed offset
	err = bc.consumer.Seek(seekPosition, 10_000)
	if err != nil {
		return fmt.Errorf("failed to rollback kafka consumer offset: %w", err)
	}
	err = bc.failedProducer.BeginTransaction()
	if err != nil {
		return fmt.Errorf("failed to begin kafka transaction: %w", err)
	}
	defer func() {
		if err != nil {
			//cleanup
			_ = bc.failedProducer.AbortTransaction(context.Background())
			_ = bc.consumer.Seek(seekPosition, 10_000)
		}
	}()

	for {
		message, err := bc.consumer.ReadMessage(pauseHeartBeatInterval)
		if err != nil {
			kafkaErr := err.(kafka.Error)
			if kafkaErr.Code() == kafka.ErrTimedOut {
				return fmt.Errorf("failed to consume message: %w", err)
			}
			if kafkaErr.IsRetriable() {
				time.Sleep(pauseHeartBeatInterval)
				continue
			} else {
				bc.restartConsumer()
				return fmt.Errorf("failed to consume message: %w", err)
			}
		}

		failedTopic, _ := MakeTopicId(bc.destinationId, "failed", bc.tableName, false)
		err = bc.failedProducer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &failedTopic, Partition: kafka.PartitionAny},
			Value:          message.Value,
		}, nil)
		if err != nil {
			return fmt.Errorf("failed to put message to producer: %w", err)
		}
		//stop consuming on the latest message before failure
		if message.TopicPartition.Offset == failedPosition.Offset {
			break
		}
	}
	groupMetadata, err := bc.consumer.GetConsumerGroupMetadata()
	if err != nil {
		return fmt.Errorf("failed to get consumer group metadata: %w", err)
	}
	offset := *failedPosition
	offset.Offset++
	//set consumer offset to the next message after failure. that happens atomically with whole producer transaction
	err = bc.failedProducer.SendOffsetsToTransaction(context.Background(), []kafka.TopicPartition{offset}, groupMetadata)
	if err != nil {
		return fmt.Errorf("failed to send consumer offset to producer transaction: %w", err)
	}
	err = bc.failedProducer.CommitTransaction(context.Background())
	if err != nil {
		return fmt.Errorf("failed to commit kafka transaction for producer: %w", err)
	}
	return nil
}

// pause consumer.
func (bc *BatchConsumer) pause() {
	if bc.idle.Load() && bc.retired.Load() {
		// Close retired idling consumer
		bc.Infof("Consumer is retired. Closing")
		_ = bc.close()
		return
	}
	if bc.paused {
		return
	}
	bc.paused = true
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
				bc.paused = false
				bc.Infof("Consumer resumed.")
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
				if !errorReported {
					bc.SystemErrorf("Error on paused consumer: %v", kafkaErr)
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
					bc.SystemErrorf("Failed to rollback offset on paused consumer: %v", err)
				}
				bc.pauseKafkaConsumer()
			}
		}
	}()
}

func (bc *BatchConsumer) restartConsumer() {
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
				bc.Errorf("Error creating kafka consumer: %w", err)
				break
			}
			err = consumer.SubscribeTopics([]string{bc.topicId}, nil)
			if err != nil {
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

func (bc *BatchConsumer) pauseKafkaConsumer() {
	partitions, err := bc.consumer.Assignment()
	if len(partitions) > 0 {
		err = bc.consumer.Pause(partitions)
	}
	if err != nil {
		bc.SystemErrorf("Failed to pause kafka consumer: %w", err)
	} else {
		if len(partitions) > 0 {
			bc.Infof("Consumer paused.")
		}
	}
}

func (bc *BatchConsumer) resume() (err error) {
	if !bc.paused {
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
func (bc *BatchConsumer) Retire() {
	bc.Infof("Retiring batch consumer")
	bc.retired.Store(true)
}
