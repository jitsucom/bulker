package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/types"
	"sync/atomic"
	"time"
)

const pauseHeartBeatInterval = 1 * time.Second

type BatchConsumer struct {
	config         *AppConfig
	repository     *Repository
	destinationId  string
	batchPeriodSec int
	//bulkerStream bulker.BulkerStream
	consumer *kafka.Consumer

	topicId         string
	waitForMessages time.Duration
	closed          chan struct{}

	//consumer that marked as no longer needed. We cannot close it immediately because it can be in the middle of processing batch
	retired atomic.Bool

	resumeChannel chan struct{}
}

func NewBatchConsumer(repository *Repository, destinationId string, batchPeriodSec int, topicId string, config *AppConfig, kafkaConfig *kafka.ConfigMap) (*BatchConsumer, error) {
	consumerConfig := kafka.ConfigMap(utils.MapPutAll(kafka.ConfigMap{
		"group.id":           topicId,
		"auto.offset.reset":  "earliest",
		"group.instance.id":  config.InstanceId,
		"enable.auto.commit": false,
		"isolation.level":    "read_committed",
	}, *kafkaConfig))
	if config.BatchRunnerWaitForMessagesSec > 30 {
		_ = consumerConfig.SetKey("session.timeout.ms", config.BatchRunnerWaitForMessagesSec*1000*2)
	}
	consumer, err := kafka.NewConsumer(&consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("[%s] Error creating consumer: %w", destinationId, err)
	}
	//consumer.Assign([]kafka.TopicPartition{{Topic: &topicId, Partition: kafka.PartitionAny}})
	//md, _ := consumer.GetMetadata(&topicId, false, 1000)
	//md.Topics[topicId].Partitions[0].
	err = consumer.Subscribe(topicId, nil)
	if err != nil {
		_ = consumer.Close()
		return nil, fmt.Errorf("[%s] Failed to subscribe to topic %s: %w", destinationId, topicId, err)
	}

	bc := &BatchConsumer{
		config:          config,
		repository:      repository,
		destinationId:   destinationId,
		batchPeriodSec:  batchPeriodSec,
		topicId:         topicId,
		consumer:        consumer,
		waitForMessages: time.Duration(config.BatchRunnerWaitForMessagesSec) * time.Second,
		closed:          make(chan struct{}),
		resumeChannel:   make(chan struct{}),
	}
	err = bc.pause(true)
	if err != nil {
		_ = bc.close()
		return nil, fmt.Errorf("[%s] Failed to set consumer on pause %s: %w", destinationId, topicId, err)
	}
	//runtime.SetFinalizer(bc, func(bc *BatchConsumer) {
	//	logging.Infof("[%s] closing batch consumer", d.Id())
	//	_ = d.bulker.Close()
	//})
	return bc, nil
}

func (bc *BatchConsumer) ConsumeAll() (consumed int, err error) {
	logging.Infof("[%s] Starting consuming messages from topic %s", bc.destinationId, bc.topicId)
	defer func() {
		if err != nil {
			logging.Errorf("[%s] Consume finished with error for topic %s. %d messages consumed. Error: %v", bc.destinationId, bc.topicId, consumed, err)
		} else {
			logging.Infof("[%s] Successfully consumed %d messages from topic %s", bc.destinationId, consumed, bc.topicId)
		}
	}()
	destination := bc.repository.LeaseDestination(bc.destinationId)
	if destination == nil {
		bc.Retire()
		return 0, fmt.Errorf("destination not found: %s. Retiring consumer", bc.destinationId)
	}
	defer func() {
		destination.Release()
	}()
	maxBatchSize := destination.config.BatchSize
	if maxBatchSize <= 0 {
		maxBatchSize = bc.config.BatchRunnerDefaultBatchSize
	}
	err = bc.resume()
	if err != nil {
		return 0, fmt.Errorf("failed to resume kafka consumer to process batch: %s: %v", bc.topicId, err)
	}
	defer func() {
		err = bc.pause(true)
		if err != nil {
			logging.SystemErrorf("[%s] Failed to pause kafka consumer after processing batch: %s: %v", bc.destinationId, bc.topicId, err)
		}
	}()
	for {
		select {
		case <-bc.closed:
			return consumed, nil
		default:
			batchSize, err := bc.processBatch(destination, maxBatchSize)
			if err != nil {
				return consumed, err
			}
			consumed += batchSize
			if batchSize < maxBatchSize {
				//we've consumed fewer events than batch size. It means that there are no more events in topic
				return consumed, nil
			}
		}
	}
}

func (bc *BatchConsumer) close() error {
	close(bc.closed)
	err := bc.consumer.Close()
	return err
}

func (bc *BatchConsumer) processBatch(destination *Destination, batchSize int) (int, error) {
	matches := TopicPattern.FindStringSubmatch(bc.topicId)
	if len(matches) != 4 {
		return 0, fmt.Errorf("[%s] Failed to parse topic %s", bc.destinationId, bc.topicId)
	}
	tableName := matches[3]
	bulkerStream, err := destination.bulker.CreateStream(bc.topicId, tableName, bulker.Transactional, destination.streamOptions...)
	if err != nil {
		return 0, fmt.Errorf("[%s] Failed to create bulker stream: %w", bc.destinationId, err)
	}

	i := 0
	timeEnd := time.Now().Add(bc.waitForMessages)
	for ; i < batchSize; i++ {
		wait := timeEnd.Sub(time.Now())
		if wait <= 0 {
			break
		}
		message, err := bc.consumer.ReadMessage(wait)
		//TODO: do we heed to interrupt batch on close?
		if err == nil {
			logging.Infof("[%s] %d. Message claimed: offset = %s, partition = %d, timestamp = %v, topic = %s\n", bc.destinationId, i, message.TopicPartition.Offset.String(), message.TopicPartition.Partition, message.Timestamp, *message.TopicPartition.Topic)
			obj := types.Object{}
			dec := json.NewDecoder(bytes.NewReader(message.Value))
			dec.UseNumber()
			err := dec.Decode(&obj)
			if err != nil {
				_, _ = bulkerStream.Abort(context.Background())
				return 0, fmt.Errorf("[%s] Failed to parse event from message: %s: %w", bc.destinationId, message.Value, err)
			}
			err = bulkerStream.Consume(context.Background(), obj)
			if err != nil {
				_, _ = bulkerStream.Abort(context.Background())
				return 0, fmt.Errorf("[%s] Failed to inject event to bulker stream: %s: %w", bc.destinationId, bc.topicId, err)
			}
		} else if (err.(kafka.Error)).Code() == kafka.ErrTimedOut {
			break
		} else {
			_, _ = bulkerStream.Abort(context.Background())
			return 0, fmt.Errorf("[%s] Failed to consume event from topic: %s: %w", bc.destinationId, bc.topicId, err)
		}
	}
	if i > 0 {
		err = bc.pause(false)
		if err != nil {
			_, _ = bulkerStream.Abort(context.Background())
			return 0, fmt.Errorf("[%s] Failed to pause kafka consumer to process batch: %w", bc.destinationId, err)
		}
		logging.Infof("[%s] Consumer paused. Committing %d events to destination.", bc.destinationId, i)
		_, err = bulkerStream.Complete(context.Background())
		if err != nil {
			return 0, fmt.Errorf("[%s] Failed to commit bulker stream: %w", bc.destinationId, err)
		}
		_, err = bc.consumer.Commit()
		if err != nil {
			return i, fmt.Errorf("[%s] Failed to commit kafka consumer: %w", bc.destinationId, err)
		}
		err = bc.resume()
		if err != nil {
			return i, fmt.Errorf("[%s] Failed to resume kafka consumer: %w", bc.destinationId, err)
		} else {
			logging.Infof("[%s] Consumer resumed.", bc.destinationId)
		}
	} else {
		_, _ = bulkerStream.Abort(context.Background())
	}
	return i, nil
}

// pause consumer.
// Set idle to true when consumer is not used
// idle=false to pause consumer in the middle of loading batch to destination
func (bc *BatchConsumer) pause(idle bool) error {
	if idle && bc.retired.Load() {
		// Close retired idling consumer
		logging.Infof("[%s] Consumer %s is retired. Closing", bc.destinationId, bc.topicId)
		return bc.close()
	}
	err := bc.pauseKafkaConsumer()
	if err != nil {
		return err
	}
	go func() {
		//this loop keeps heatbeating consumer to prevent it from being kicked out from group
	loop:
		for {
			if idle && bc.retired.Load() {
				// Close retired idling consumer
				logging.Infof("[%s] Consumer %s is retired. Closing", bc.destinationId, bc.topicId)
				_ = bc.close()
				return
			}
			select {
			case <-bc.resumeChannel:
				break loop
			default:
			}
			// ReadMessage on paused consumer must always return ErrTimedOut
			message, err := bc.consumer.ReadMessage(pauseHeartBeatInterval)
			if err != nil {
				if (err.(kafka.Error)).Code() == kafka.ErrTimedOut {
					logging.Debugf("[%s] Consumer paused. Heartbeat sent.", bc.destinationId)
					continue
				}
				logging.SystemErrorf("[%s] Error on paused consumer: %v", bc.destinationId, err)
			} else if message != nil {
				//If message slipped through pause, rollback offset and make sure consumer is paused
				topicPartition := message.TopicPartition
				err = bc.consumer.Seek(topicPartition, 0)
				if err != nil {
					logging.SystemErrorf("[%s] Failed to rollback offset on paused consumer: %v", bc.destinationId, err)
				}
				err = bc.pauseKafkaConsumer()
				if err != nil {
					logging.SystemError(err)
					continue
				}
			}
		}

	}()
	return nil
}

func (bc *BatchConsumer) pauseKafkaConsumer() error {
	partitions, err := bc.consumer.Assignment()
	if len(partitions) > 0 {
		err = bc.consumer.Pause(partitions)
	}
	if err != nil {
		return fmt.Errorf("[%s] Failed to pause kafka consumer: %w", bc.destinationId, err)
	}
	return nil
}

func (bc *BatchConsumer) resume() error {
	partitions, err := bc.consumer.Assignment()
	if err != nil {
		return err
	}
	select {
	case bc.resumeChannel <- struct{}{}:
		return bc.consumer.Resume(partitions)
	case <-time.After(pauseHeartBeatInterval * 2):
		return fmt.Errorf("resume timeout")
	}
}

// Retire Mark consumer as retired
// Consumer will close itself when com
func (bc *BatchConsumer) Retire() {
	logging.Infof("[%s] Retiring batch consumer for topic: %s", bc.destinationId, bc.topicId)
	bc.retired.Store(true)
}
