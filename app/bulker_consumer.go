package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jitsucom/bulker/types"
	"time"
)

type BatchConsumer struct {
	destination *Destination
	//bulkerStream bulker.BulkerStream
	consumer *kafka.Consumer

	topicId         string
	batchSize       int
	waitForMessages time.Duration
	closed          chan struct{}
}

func NewBatchConsumer(destination *Destination, topicId string, config *Config, batchSize int) (*BatchConsumer, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  config.KafkaBootstrapServers,
		"group.id":           topicId,
		"auto.offset.reset":  "earliest",
		"group.instance.id":  config.InstanceId,
		"enable.auto.commit": false,
		"isolation.level":    "read_committed",
	})
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
		return -1, fmt.Errorf("[%s] Failed to create bulker stream: %w", bc.destination.Id(), err)
	}

	i := 0
	for ; i < bc.batchSize; i++ {
		message, err := bc.consumer.ReadMessage(bc.waitForMessages)
		if err == nil {
			obj := types.Object{}
			dec := json.NewDecoder(bytes.NewReader(message.Value))
			dec.UseNumber()
			err := dec.Decode(&obj)
			if err != nil {
				_, _ = bulkerStream.Abort(context.Background())
				return -1, fmt.Errorf("[%s] Failed to parse event from message: %s: %w", bc.destination.Id(), message.Value, err)
			}
			err = bulkerStream.Consume(context.Background(), obj)
			if err != nil {
				_, _ = bulkerStream.Abort(context.Background())
				return -1, fmt.Errorf("[%s] Failed to inject event to bulker stream: %s: %w", bc.destination.Id(), bc.topicId, err)
			}
			fmt.Printf("[%s] %d. Message claimed: offset = %s, partition = %d, timestamp = %v, topic = %s\n", bc.destination.Id(), i, message.TopicPartition.Offset.String(), message.TopicPartition.Partition, message.Timestamp, *message.TopicPartition.Topic)
		} else if (err.(kafka.Error)).Code() == kafka.ErrTimedOut {
			break
		} else {
			_, _ = bulkerStream.Abort(context.Background())
			return -1, fmt.Errorf("[%s] Failed to consume event from topic: %s: %w", bc.destination.Id(), bc.topicId, err)
		}
	}
	if i > 0 {
		_, err = bulkerStream.Complete(context.Background())
		if err != nil {
			return -1, fmt.Errorf("[%s] Failed to commit bulker stream: %w", bc.destination.Id(), err)
		}
		_, _ = bc.consumer.Commit()
	} else {
		_, _ = bulkerStream.Abort(context.Background())
	}
	return i, nil
}
