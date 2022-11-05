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

const streamConsumerMessageWaitTimeout = 1 * time.Second

type StreamConsumer struct {
	config      *AppConfig
	repository  *Repository
	destination *Destination
	stream      atomic.Pointer[bulker.BulkerStream]
	consumer    *kafka.Consumer

	topicId   string
	tableName string

	closed chan struct{}
}

func NewStreamConsumer(repository *Repository, destination *Destination, topicId string, config *AppConfig, kafkaConfig *kafka.ConfigMap) (*StreamConsumer, error) {
	consumerConfig := kafka.ConfigMap(utils.MapPutAll(kafka.ConfigMap{
		"group.id":           topicId,
		"auto.offset.reset":  "earliest",
		"group.instance.id":  config.InstanceId,
		"enable.auto.commit": true,
		"isolation.level":    "read_committed",
	}, *kafkaConfig))

	consumer, err := kafka.NewConsumer(&consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("[%s] Error creating kafka consumer: %w", destination.Id(), err)
	}

	err = consumer.SubscribeTopics([]string{topicId}, nil)
	if err != nil {
		_ = consumer.Close()
		return nil, fmt.Errorf("[%s] Failed to subscribe to topic %s: %w", destination.Id(), topicId, err)
	}
	//destination := repository.LeaseDestination(destinationId)
	//if destination == nil {
	//	return nil, fmt.Errorf("[%s] Destination not found", destinationId)
	//}

	sc := &StreamConsumer{
		config:      config,
		repository:  repository,
		destination: destination,
		topicId:     topicId,
		consumer:    consumer,
		closed:      make(chan struct{}),
	}

	err = sc.start()
	if err != nil {
		return nil, err
	}
	sc.destination.Lease()
	return sc, nil
}

// start consuming messages from kafka
func (sc *StreamConsumer) start() error {
	logging.Infof("[%s] Starting stream consumer for topic: %s. Ver: %s", sc.destination.Id(), sc.topicId, sc.destination.config.UpdatedAt)
	matches := TopicPattern.FindStringSubmatch(sc.topicId)
	if len(matches) != 4 {
		return fmt.Errorf("[%s] Failed to parse topic %s", sc.destination.Id(), sc.topicId)
	}
	tableName := matches[3]
	sc.tableName = tableName

	bulkerStream, err := sc.destination.bulker.CreateStream(sc.topicId, tableName, bulker.AutoCommit, sc.destination.streamOptions...)
	if err != nil {
		return fmt.Errorf("[%s] Failed to create bulker stream: %w", sc.destination.Id(), err)
	}
	sc.stream.Store(&bulkerStream)

	go func() {
		var err error
		defer func() {
			var state bulker.State
			if err != nil {
				state, _ = (*sc.stream.Load()).Abort(context.Background())
			} else {
				state, _ = (*sc.stream.Load()).Complete(context.Background())
			}
			logging.Infof("[%s] Closed stream state: %+v", sc.destination.Id(), state)
		}()
		for {
			select {
			case <-sc.closed:
				_ = sc.consumer.Close()
				return
			default:
				message, err := sc.consumer.ReadMessage(streamConsumerMessageWaitTimeout)
				if err == nil {
					logging.Infof("[%s] Message claimed: offset = %s, partition = %d, timestamp = %v, topic = %s\n", sc.destination.Id(), message.TopicPartition.Offset.String(), message.TopicPartition.Partition, message.Timestamp, *message.TopicPartition.Topic)
					obj := types.Object{}
					dec := json.NewDecoder(bytes.NewReader(message.Value))
					dec.UseNumber()
					err := dec.Decode(&obj)
					if err != nil {
						logging.Errorf("[%s] Failed to parse event from message: %s: %w", sc.destination.Id(), message.Value, err)
						continue
					}
					err = (*sc.stream.Load()).Consume(context.Background(), obj)
					if err != nil {
						logging.Errorf("[%s] Failed to inject event to bulker stream: %s: %w", sc.destination.Id(), sc.topicId, err)
						continue
					}
				} else if (err.(kafka.Error)).Code() != kafka.ErrTimedOut {
					logging.Errorf("[%s] Error reading message from topic %s: %v", sc.destination.Id(), sc.topicId, err)
					time.Sleep(streamConsumerMessageWaitTimeout * 10)
				}
			}
		}
	}()
	return nil
}

// Close consumer
func (sc *StreamConsumer) Close() error {
	logging.Infof("[%s] Closing stream consumer for topic: %s. Ver: %s", sc.destination.Id(), sc.topicId, sc.destination.config.UpdatedAt)
	close(sc.closed)
	sc.destination.Release()
	//TODO: wait for closing?
	return nil
}

// UpdateDestination
func (sc *StreamConsumer) UpdateDestination(destination *Destination) error {
	logging.Infof("[%s] Updating stream consumer for topic: %s. Ver: %s", sc.destination.Id(), sc.topicId, sc.destination.config.UpdatedAt)
	destination.Lease()

	//create new stream
	bulkerStream, err := destination.bulker.CreateStream(sc.topicId, sc.tableName, bulker.AutoCommit, destination.streamOptions...)
	if err != nil {
		return fmt.Errorf("[%s] Failed to create bulker stream: %w", destination.Id(), err)
	}
	oldBulkerStream := sc.stream.Swap(&bulkerStream)
	state, err := (*oldBulkerStream).Complete(context.Background())
	logging.Infof("[%s] Previous stream state: %+v", sc.destination.Id(), state)
	oldDestination := sc.destination
	oldDestination.Release()

	sc.destination = destination
	return nil
}
