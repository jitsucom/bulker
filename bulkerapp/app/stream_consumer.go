package app

import (
	"bytes"
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jitsucom/bulker/bulkerapp/metrics"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/eventslog"
	"github.com/jitsucom/bulker/jitsubase/safego"
	"github.com/jitsucom/bulker/jitsubase/timestamp"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/jitsucom/bulker/kafkabase"
	jsoniter "github.com/json-iterator/go"
	"strconv"
	"sync/atomic"
	"time"
)

const streamConsumerMessageWaitTimeout = 5 * time.Second

type StreamConsumerImpl struct {
	*AbstractConsumer
	repository     *Repository
	destination    *Destination
	stream         atomic.Pointer[bulker.BulkerStream]
	consumerConfig kafka.ConfigMap
	consumer       *kafka.Consumer

	eventsLogService eventslog.EventsLogService

	tableName string

	closed chan struct{}
}

type StreamConsumer interface {
	Consumer
	UpdateDestination(destination *Destination) error
}

func NewStreamConsumer(repository *Repository, destination *Destination, topicId string, config *Config, kafkaConfig *kafka.ConfigMap, bulkerProducer *Producer, eventsLogService eventslog.EventsLogService) (*StreamConsumerImpl, error) {
	abstract := NewAbstractConsumer(config, repository, topicId, bulkerProducer)
	_, _, tableName, err := ParseTopicId(topicId)
	if err != nil {
		metrics.ConsumerErrors(topicId, "stream", "INVALID_TOPIC", "INVALID_TOPIC:"+topicId, "failed to parse topic").Inc()
		return nil, abstract.NewError("Failed to parse topic: %v", err)
	}
	consumerConfig := kafka.ConfigMap(utils.MapPutAll(kafka.ConfigMap{
		"group.id":                      topicId,
		"auto.offset.reset":             "earliest",
		"allow.auto.create.topics":      false,
		"group.instance.id":             abstract.GetInstanceId(),
		"partition.assignment.strategy": config.KafkaConsumerPartitionsAssigmentStrategy,
		"enable.auto.commit":            true,
		"isolation.level":               "read_committed",
		"session.timeout.ms":            config.KafkaSessionTimeoutMs,
		"max.poll.interval.ms":          config.KafkaMaxPollIntervalMs,
	}, *kafkaConfig))

	consumer, err := kafka.NewConsumer(&consumerConfig)
	if err != nil {
		metrics.ConsumerErrors(topicId, "stream", destination.Id(), tableName, metrics.KafkaErrorCode(err)).Inc()
		return nil, abstract.NewError("Error creating kafka consumer: %v", err)
	}

	err = consumer.SubscribeTopics([]string{topicId}, nil)
	if err != nil {
		_ = consumer.Close()
		metrics.ConsumerErrors(topicId, "stream", destination.Id(), tableName, metrics.KafkaErrorCode(err)).Inc()
		return nil, abstract.NewError("Failed to subscribe to topic: %v", err)
	}

	//destination := repository.LeaseDestination(destinationId)
	//if destination == nil {
	//	return nil, fmt.Errorf("[%s] Destination not found", destinationId)
	//}

	sc := &StreamConsumerImpl{
		AbstractConsumer: abstract,
		repository:       repository,
		destination:      destination,
		tableName:        tableName,
		consumerConfig:   consumerConfig,
		consumer:         consumer,
		eventsLogService: eventsLogService,
		closed:           make(chan struct{}),
	}
	var bs bulker.BulkerStream
	bs = &StreamWrapper{destination: destination, topicId: topicId, tableName: tableName}
	sc.stream.Store(&bs)
	sc.start()
	return sc, nil
}

type StreamWrapper struct {
	destination *Destination
	stream      bulker.BulkerStream
	topicId     string
	tableName   string
}

func (sw *StreamWrapper) Consume(ctx context.Context, object types.Object) (state bulker.State, processedObject types.Object, err error) {
	if sw.stream == nil {
		sw.destination.Lease()
		sw.destination.InitBulkerInstance()
		bulkerStream, err := sw.destination.bulker.CreateStream(sw.topicId, sw.tableName, bulker.Stream, sw.destination.streamOptions.Options...)
		if err != nil {
			metrics.ConsumerErrors(sw.topicId, "stream", sw.destination.Id(), sw.tableName, "failed to create bulker stream").Inc()
			return bulker.State{}, nil, fmt.Errorf("Failed to create bulker stream: %v", err)
		}
		sw.stream = bulkerStream
	}
	return sw.stream.Consume(ctx, object)
}

func (sw *StreamWrapper) Abort(ctx context.Context) (bulker.State, error) {
	if sw.stream == nil {
		return bulker.State{}, nil
	}
	sw.destination.Release()
	return sw.stream.Abort(ctx)
}

func (sw *StreamWrapper) Complete(ctx context.Context) (bulker.State, error) {
	if sw.stream == nil {
		return bulker.State{}, nil
	}
	sw.destination.Release()
	return sw.stream.Complete(ctx)
}

func (sc *StreamConsumerImpl) restartConsumer() {
	sc.Infof("Restarting consumer")
	go func(c *kafka.Consumer) {
		err := c.Close()
		sc.Infof("Previous consumer closed: %v", err)
	}(sc.consumer)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-sc.closed:
			return
		case <-ticker.C:
			sc.Infof("Restarting consumer")
			consumer, err := kafka.NewConsumer(&sc.consumerConfig)
			if err != nil {
				metrics.ConsumerErrors(sc.topicId, "stream", sc.destination.Id(), sc.tableName, metrics.KafkaErrorCode(err)).Inc()
				sc.Errorf("Error creating kafka consumer: %v", err)
				break
			}
			err = consumer.SubscribeTopics([]string{sc.topicId}, nil)
			if err != nil {
				metrics.ConsumerErrors(sc.topicId, "stream", sc.destination.Id(), sc.tableName, metrics.KafkaErrorCode(err)).Inc()
				_ = consumer.Close()
				sc.Errorf("Failed to subscribe to topic: %v", err)
				break
			}
			sc.consumer = consumer
			sc.Infof("Restarted successfully")
			return
		}
	}
}

// start consuming messages from kafka
func (sc *StreamConsumerImpl) start() {
	sc.Infof("Starting stream consumer for topic. Ver: %s", sc.destination.config.UpdatedAt)
	safego.RunWithRestart(func() {
		var err error
		for {
			select {
			case <-sc.closed:
				_ = sc.consumer.Close()
				var state bulker.State
				if err != nil {
					state, _ = (*sc.stream.Load()).Abort(context.Background())
				} else {
					state, _ = (*sc.stream.Load()).Complete(context.Background())
				}
				sc.Infof("Closed stream state: %+v", state)
				return
			default:
				var message *kafka.Message
				message, err = sc.consumer.ReadMessage(streamConsumerMessageWaitTimeout)
				if err != nil {
					kafkaErr := err.(kafka.Error)
					if kafkaErr.Code() != kafka.ErrTimedOut {
						metrics.ConsumerErrors(sc.topicId, "stream", sc.destination.Id(), sc.tableName, metrics.KafkaErrorCode(kafkaErr)).Inc()
						sc.Errorf("Error reading message from topic: %v retryable: %t", kafkaErr, kafkaErr.IsRetriable())
						if kafkaErr.IsRetriable() {
							time.Sleep(10 * time.Second)
						} else {
							sc.restartConsumer()
						}
					} else {
						time.Sleep(streamConsumerMessageWaitTimeout)
					}
					continue
				}
				metricsMeta := kafkabase.GetKafkaHeader(message, MetricsMetaHeader)
				metrics.ConsumerMessages(sc.topicId, "stream", sc.destination.Id(), sc.tableName, "consumed").Inc()
				obj := types.Object{}
				dec := jsoniter.NewDecoder(bytes.NewReader(message.Value))
				dec.UseNumber()
				err = dec.Decode(&obj)
				if err != nil {
					metrics.ConsumerErrors(sc.topicId, "stream", sc.destination.Id(), sc.tableName, "parse_event_error").Inc()
					sc.postEventsLog(message.Value, nil, nil, err)
					sc.Errorf("Failed to parse event from message: %s offset: %s: %v", message.Value, message.TopicPartition.Offset.String(), err)
				} else {
					sc.Debugf("Consumed Message ID: %s Offset: %s (Retries: %s) for: %s", obj.Id(), message.TopicPartition.Offset.String(), kafkabase.GetKafkaHeader(message, retriesCountHeader), sc.destination.config.BulkerType)
					var state bulker.State
					var processedObject types.Object
					state, processedObject, err = (*sc.stream.Load()).Consume(context.Background(), obj)
					sc.postEventsLog(message.Value, state.Representation, processedObject, err)
					if err != nil {
						metrics.ConsumerErrors(sc.topicId, "stream", sc.destination.Id(), sc.tableName, "bulker_stream_error").Inc()
						sc.Errorf("Failed to inject event to bulker stream: %v", err)
					} else {
						sc.SendMetrics(metricsMeta, "success", 1)
						metrics.ConsumerMessages(sc.topicId, "stream", sc.destination.Id(), sc.tableName, "processed").Inc()
					}
				}
				if err != nil {
					originalError := err
					failedTopic, _ := MakeTopicId(sc.destination.Id(), retryTopicMode, allTablesToken, false)
					retries, err := kafkabase.GetKafkaIntHeader(message, retriesCountHeader)
					if err != nil {
						sc.Errorf("failed to read retry header: %v", err)
					}
					metricStatus := "error"
					if retries > 0 {
						metricStatus = "retry_error"
					}
					sc.SendMetrics(metricsMeta, metricStatus, 1)
					status := "retryScheduled"
					if retries >= sc.config.MessagesRetryCount {
						//no attempts left - send to dead-letter topic
						status = "deadLettered"
						failedTopic, _ = MakeTopicId(sc.destination.Id(), deadTopicMode, allTablesToken, false)
					}
					headers := message.Headers
					kafkabase.PutKafkaHeader(&headers, errorHeader, originalError.Error())
					kafkabase.PutKafkaHeader(&headers, originalTopicHeader, sc.topicId)
					kafkabase.PutKafkaHeader(&headers, retriesCountHeader, strconv.Itoa(retries))
					kafkabase.PutKafkaHeader(&headers, retryTimeHeader, timestamp.ToISOFormat(RetryBackOffTime(sc.config, retries+1).UTC()))
					retryMessage := kafka.Message{
						Key:            message.Key,
						TopicPartition: kafka.TopicPartition{Topic: &failedTopic, Partition: kafka.PartitionAny},
						Headers:        headers,
						Value:          message.Value,
					}
					err = sc.bulkerProducer.ProduceSync(failedTopic, retryMessage)
					if err != nil {
						sc.Errorf("failed to store event to 'failed' topic: %s: %v", failedTopic, err)
						metrics.ConsumerMessages(sc.topicId, "stream", sc.destination.Id(), sc.tableName, "LOST").Inc()
						continue
					}
					metrics.ConsumerMessages(sc.topicId, "stream", sc.destination.Id(), sc.tableName, "failed").Inc()
					metrics.ConsumerMessages(sc.topicId, "stream", sc.destination.Id(), sc.tableName, status).Inc()
				}

			}
		}
	})
}

func (sc *StreamConsumerImpl) TopicId() string {
	return sc.topicId
}

// Close consumer
func (sc *StreamConsumerImpl) Retire() {
	select {
	case <-sc.closed:
		return
	default:
	}
	sc.Infof("Closing stream consumer. Ver: %s", sc.destination.config.UpdatedAt)
	close(sc.closed)
	sc.destination.Release()
	//TODO: wait for closing?
	return
}

// UpdateDestination
func (sc *StreamConsumerImpl) UpdateDestination(destination *Destination) error {
	sc.Infof("[Updating stream consumer for topic. Ver: %s", sc.destination.config.UpdatedAt)

	//create new stream
	var bs bulker.BulkerStream
	bs = &StreamWrapper{destination: destination, topicId: sc.topicId, tableName: sc.tableName}
	oldBulkerStream := sc.stream.Swap(&bs)
	state, _ := (*oldBulkerStream).Complete(context.Background())
	sc.Infof("Previous stream state: %+v", state)
	sc.destination = destination
	return nil
}

func (sc *StreamConsumerImpl) postEventsLog(message []byte, representation any, processedObject types.Object, processedErr error) {
	object := map[string]any{
		"original": string(message),
		"status":   "SUCCESS",
	}
	if representation != nil {
		object["representation"] = representation
	}
	if len(processedObject) > 0 {
		object["mappedData"] = processedObject
	}

	level := eventslog.LevelInfo
	if processedErr != nil {
		object["error"] = processedErr.Error()
		object["status"] = "FAILED"
		level = eventslog.LevelError
	}
	sc.eventsLogService.PostAsync(&eventslog.ActorEvent{eventslog.EventTypeProcessed, level, sc.destination.Id(), object})
}
