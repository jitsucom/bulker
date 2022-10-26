package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/types"
	"time"
)

const pauseHeartBeatInterval = 1 * time.Second

type BatchConsumer struct {
	destination *Destination
	//bulkerStream bulker.BulkerStream
	consumer *kafka.Consumer

	topicId         string
	batchSize       int
	waitForMessages time.Duration
	closed          chan struct{}

	resumeChannel chan struct{}
}

func NewBatchConsumer(destination *Destination, topicId string, config *AppConfig, kafkaConfig *kafka.ConfigMap, batchSize int) (*BatchConsumer, error) {
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
		return nil, fmt.Errorf("[%s] Error creating consumer: %w", destination.Id(), err)
	}
	err = consumer.SubscribeTopics([]string{topicId}, nil)
	if err != nil {
		_ = consumer.Close()
		return nil, fmt.Errorf("[%s] Failed to subscribe to topic %s: %w", destination.Id(), topicId, err)
	}
	if batchSize <= 0 {
		batchSize = config.BatchRunnerDefaultBatchSize
	}
	return &BatchConsumer{
		destination:     destination,
		topicId:         topicId,
		consumer:        consumer,
		batchSize:       batchSize,
		waitForMessages: time.Duration(config.BatchRunnerWaitForMessagesSec) * time.Second,
		closed:          make(chan struct{}),
		resumeChannel:   make(chan struct{}),
	}, nil
}

func (bc *BatchConsumer) ConsumeAll() (int, error) {
	consumed := 0
	for {
		select {
		case <-bc.closed:
			return consumed, nil
		default:
			batchSize, err := bc.processBatch()
			if err != nil {
				return consumed, err
			}
			consumed += batchSize
			if batchSize < bc.batchSize {
				//we've consumed fewer events than batch size. It means that there are no more events in topic
				return consumed, nil
			}
		}
	}
}

func (bc *BatchConsumer) Close() error {
	close(bc.closed)
	return bc.consumer.Close()
}

func (bc *BatchConsumer) processBatch() (int, error) {
	matches := TopicPattern.FindStringSubmatch(bc.topicId)
	if len(matches) != 4 {
		return 0, fmt.Errorf("[%s] Failed to parse topic %s", bc.destination.Id(), bc.topicId)
	}
	tableName := matches[3]
	streamConfig := bc.destination.config.StreamConfig
	bulkerStream, err := bc.destination.bulker.CreateStream(bc.topicId, tableName, streamConfig.BulkMode, bc.destination.streamOptions...)
	if err != nil {
		return 0, fmt.Errorf("[%s] Failed to create bulker stream: %w", bc.destination.Id(), err)
	}

	i := 0
	timeEnd := time.Now().Add(bc.waitForMessages)
	for ; i < bc.batchSize; i++ {
		wait := timeEnd.Sub(time.Now())
		if wait <= 0 {
			break
		}
		message, err := bc.consumer.ReadMessage(wait)
		if err == nil {
			fmt.Printf("[%s] %d. Message claimed: offset = %s, partition = %d, timestamp = %v, topic = %s\n", bc.destination.Id(), i, message.TopicPartition.Offset.String(), message.TopicPartition.Partition, message.Timestamp, *message.TopicPartition.Topic)
			obj := types.Object{}
			dec := json.NewDecoder(bytes.NewReader(message.Value))
			dec.UseNumber()
			err := dec.Decode(&obj)
			if err != nil {
				_, _ = bulkerStream.Abort(context.Background())
				return 0, fmt.Errorf("[%s] Failed to parse event from message: %s: %w", bc.destination.Id(), message.Value, err)
			}
			err = bulkerStream.Consume(context.Background(), obj)
			if err != nil {
				_, _ = bulkerStream.Abort(context.Background())
				return 0, fmt.Errorf("[%s] Failed to inject event to bulker stream: %s: %w", bc.destination.Id(), bc.topicId, err)
			}
		} else if (err.(kafka.Error)).Code() == kafka.ErrTimedOut {
			break
		} else {
			_, _ = bulkerStream.Abort(context.Background())
			return 0, fmt.Errorf("[%s] Failed to consume event from topic: %s: %w", bc.destination.Id(), bc.topicId, err)
		}
	}
	if i > 0 {
		err = bc.pause()
		if err != nil {
			_, _ = bulkerStream.Abort(context.Background())
			return 0, fmt.Errorf("[%s] Failed to pause kafka consumer to process batch: %w", bc.destination.Id(), err)
		}
		logging.Infof("[%s] Consumer paused. Committing %d events to destination.", bc.destination.Id(), i)
		_, err = bulkerStream.Complete(context.Background())
		if err != nil {
			return 0, fmt.Errorf("[%s] Failed to commit bulker stream: %w", bc.destination.Id(), err)
		}
		_, err = bc.consumer.Commit()
		if err != nil {
			return i, fmt.Errorf("[%s] Failed to commit kafka consumer: %w", bc.destination.Id(), err)
		}
		err = bc.resume()
		if err != nil {
			return i, fmt.Errorf("[%s] Failed to resume kafka consumer: %w", bc.destination.Id(), err)
		} else {
			logging.Infof("[%s] Consumer resumed.", bc.destination.Id())
		}
	} else {
		_, _ = bulkerStream.Abort(context.Background())
	}
	return i, nil
}

func (bc *BatchConsumer) pause() error {
	partitions, err := bc.consumer.Assignment()
	if err != nil {
		return fmt.Errorf("[%s] Failed to pause kafka consumer to process batch: %w", bc.destination.Id(), err)
	}
	err = bc.consumer.Pause(partitions)
	if err != nil {
		return fmt.Errorf("[%s] Failed to pause kafka consumer to process batch: %w", bc.destination.Id(), err)
	}
	go func() {
		//this loop keeps heatbeating consumer to prevent it from being kicked out from group
	loop:
		for {
			select {
			case <-bc.resumeChannel:
				break loop
			default:
			}
			// ReadMessage on paused consumer must always return ErrTimedOut
			message, err := bc.consumer.ReadMessage(pauseHeartBeatInterval)
			if err != nil {
				if (err.(kafka.Error)).Code() == kafka.ErrTimedOut {
					logging.Debugf("[%s] Consumer paused. Heartbeat sent.", bc.destination.Id())
					continue
				}
				logging.SystemErrorf("[%s] Error on paused consumer: %v", bc.destination.Id(), err)
			} else if message != nil {
				logging.SystemErrorf("[%s] Received message while paused: %v", bc.destination.Id(), message)
			}
		}
	}()
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
