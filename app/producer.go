package app

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hashicorp/go-multierror"
	"github.com/jitsucom/bulker/app/metrics"
	"github.com/jitsucom/bulker/base/objects"
	"github.com/jitsucom/bulker/base/utils"
	"time"
)

const MessageIdHeader = "message_id"

type Producer struct {
	objects.ServiceBase
	producer *kafka.Producer

	asyncDeliveryChannel chan kafka.Event
	waitForDelivery      time.Duration
	closed               chan struct{}
}

// NewProducer creates new Producer
func NewProducer(config *AppConfig, kafkaConfig *kafka.ConfigMap) (*Producer, error) {
	base := objects.NewServiceBase("producer")

	producerConfig := kafka.ConfigMap(utils.MapPutAll(kafka.ConfigMap{
		//TODO: add producer specific config here
	}, *kafkaConfig))
	producer, err := kafka.NewProducer(&producerConfig)
	if err != nil {
		return nil, base.NewError("error creating kafka producer: %w", err)

	}
	return &Producer{
		ServiceBase:          base,
		producer:             producer,
		asyncDeliveryChannel: make(chan kafka.Event, 1000),
		closed:               make(chan struct{}),
		waitForDelivery:      time.Millisecond * time.Duration(config.ProducerWaitForDeliveryMs),
	}, nil
}

func (p *Producer) Start() {
	go func() {
		for e := range p.producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				messageId := GetKafkaHeader(ev, MessageIdHeader)
				if ev.TopicPartition.Error != nil {
					//TODO: check for retrieable errors
					metrics.ProducerDeliveryErrors(ProducerLabelsWithErr(*ev.TopicPartition.Topic, metrics.KafkaErrorCode(ev.TopicPartition.Error))).Inc()
					p.Errorf("Error sending message (ID: %s) to kafka topic %s: %s", messageId, ev.TopicPartition.Topic, ev.TopicPartition.Error.Error())
				} else {
					metrics.ProducerMessagesDelivered(ProducerLabels(*ev.TopicPartition.Topic, "")).Inc()
					p.Debugf("Message ID: %s delivered to topic %s [%d] at offset %v", messageId, *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
				//case kafka.Error:
				//	p.Debugf("Producer error: %w", ev)
			}
		}
		p.Infof("Producer closed")
	}()
	// report size metrics
	go func() {
		ticker := time.NewTicker(time.Second * 15)
		for {
			select {
			case <-p.closed:
				return
			case <-ticker.C:
				metrics.ProducerQueueLength.Set(float64(len(p.producer.ProduceChannel())))
			}
		}
	}()
}

// ProduceSync TODO: transactional delivery?
// produces messages to kafka
func (p *Producer) ProduceSync(topic string, events ...kafka.Message) error {
	if p.isClosed() {
		return p.NewError("producer is closed")
	}
	started := time.Now()
	deliveryChan := make(chan kafka.Event, len(events))
	errors := multierror.Error{}
	sent := 0
	for _, event := range events {
		err := p.producer.Produce(&event, deliveryChan)
		if err != nil {
			metrics.ProducerProduceErrors(ProducerLabelsWithErr(topic, metrics.KafkaErrorCode(err))).Inc()
			errors.Errors = append(errors.Errors, err)
		} else {
			metrics.ProducerMessagesProduced(ProducerLabels(topic, "")).Inc()
			sent++
		}
	}
	if sent > 0 {
		p.producer.Flush(2)
		p.Debugf("Sent %d messages to kafka topic %s in %s", sent, topic, time.Since(started))
		until := time.After(p.waitForDelivery)
	loop:
		for i := 0; i < sent; i++ {
			select {
			case e := <-deliveryChan:
				m := e.(*kafka.Message)
				messageId := GetKafkaHeader(m, MessageIdHeader)
				if m.TopicPartition.Error != nil {
					metrics.ProducerDeliveryErrors(ProducerLabelsWithErr(topic, metrics.KafkaErrorCode(m.TopicPartition.Error))).Inc()
					p.Errorf("Error sending message (ID: %s) to kafka topic %s: %v", messageId, topic, m.TopicPartition.Error)
					errors.Errors = append(errors.Errors, m.TopicPartition.Error)
				} else {
					metrics.ProducerMessagesDelivered(ProducerLabels(topic, "")).Inc()
					p.Debugf("Message ID: %s delivered to topic %s [%d] at offset %v", messageId, *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
			case <-until:
				metrics.ProducerDeliveryErrors(ProducerLabelsWithErr(topic, "sync_delivery_timeout")).Inc()
				p.Errorf("Timeout waiting for delivery")
				errors.Errors = append(errors.Errors, fmt.Errorf("timeout waiting for delivery"))
				break loop
			}
		}
		p.Infof("Delivered %d messages to kafka topic %s in %s", sent, topic, time.Since(started))
	}
	return errors.ErrorOrNil()
}

// ProduceAsync TODO: transactional delivery?
// produces messages to kafka
func (p *Producer) ProduceAsync(topic string, events ...[]byte) error {
	if p.isClosed() {
		return p.NewError("producer is closed")
	}
	errors := multierror.Error{}
	for _, event := range events {
		err := p.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          event,
		}, nil)
		if err != nil {
			metrics.ProducerProduceErrors(ProducerLabelsWithErr(topic, metrics.KafkaErrorCode(err))).Inc()
			errors.Errors = append(errors.Errors, err)
		} else {
			metrics.ProducerMessagesProduced(ProducerLabels(topic, "")).Inc()
		}
	}
	return errors.ErrorOrNil()
}

// Close closes producer
func (p *Producer) Close() error {
	if p.isClosed() {
		return nil
	}
	notProduced := p.producer.Flush(3000)
	if notProduced > 0 {
		p.Errorf("%d message left unsent in producer queue.", notProduced)
		//TODO: suck p.producer.ProduceChannel() and store to fallback file or some retry queue
	}
	close(p.closed)
	p.producer.Close()
	close(p.asyncDeliveryChannel)
	return nil
}

func (p *Producer) isClosed() bool {
	select {
	case <-p.closed:
		return true
	default:
		return false
	}
}

func ProducerLabels(topic string, errText string) (destinationId, mode, tableName string) {
	destinationId, mode, tableName, topicErr := ParseTopicId(topic)
	if topicErr != nil {
		return topic, "", ""
	} else {
		return destinationId, mode, tableName
	}
}

func ProducerLabelsWithErr(topic string, errText string) (destinationId, mode, tableName, err string) {
	destinationId, mode, tableName, topicErr := ParseTopicId(topic)
	if topicErr != nil {
		return topic, "", "", errText
	} else {
		return destinationId, mode, tableName, errText
	}
}
