package app

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/hashicorp/go-multierror"
	"github.com/jitsucom/bulker/bulkerapp/metrics"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/safego"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"time"
)

const MessageIdHeader = "message_id"

type Producer struct {
	appbase.Service
	producer *kafka.Producer

	asyncDeliveryChannel chan kafka.Event
	waitForDelivery      time.Duration
	closed               chan struct{}
}

// NewProducer creates new Producer
func NewProducer(config *Config, kafkaConfig *kafka.ConfigMap) (*Producer, error) {
	base := appbase.NewServiceBase("producer")

	producerConfig := kafka.ConfigMap(utils.MapPutAll(kafka.ConfigMap{
		"batch.size": config.ProducerBatchSize,
		"linger.ms":  config.ProducerLingerMs,
	}, *kafkaConfig))
	producer, err := kafka.NewProducer(&producerConfig)
	if err != nil {
		return nil, base.NewError("error creating kafka producer: %v", err)

	}
	return &Producer{
		Service:              base,
		producer:             producer,
		asyncDeliveryChannel: make(chan kafka.Event, 1000),
		closed:               make(chan struct{}),
		waitForDelivery:      time.Millisecond * time.Duration(config.ProducerWaitForDeliveryMs),
	}, nil
}

func (p *Producer) Start() {
	safego.RunWithRestart(func() {
		for e := range p.producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				messageId := GetKafkaHeader(ev, MessageIdHeader)
				if ev.TopicPartition.Error != nil {
					//TODO: check for retrieable errors
					metrics.ProducerMessages(ProducerMessageLabels(*ev.TopicPartition.Topic, "error", metrics.KafkaErrorCode(ev.TopicPartition.Error))).Inc()
					p.Errorf("Error sending message (ID: %s) to kafka topic %s: %s", messageId, *ev.TopicPartition.Topic, ev.TopicPartition.Error.Error())
				} else {
					metrics.ProducerMessages(ProducerMessageLabels(*ev.TopicPartition.Topic, "delivered", "")).Inc()
					p.Debugf("Message ID: %s delivered to topic %s [%d] at offset %v", messageId, *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
				//case kafka.Error:
				//	p.Debugf("Producer error: %v", ev)
			}
		}
		p.Infof("Producer closed")
	})
	// report size metrics
	safego.RunWithRestart(func() {
		ticker := time.NewTicker(time.Second * 15)
		for {
			select {
			case <-p.closed:
				return
			case <-ticker.C:
				metrics.ProducerQueueLength.Set(float64(p.producer.Len()))
			}
		}
	})
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
			metrics.ProducerMessages(ProducerMessageLabels(topic, "error", metrics.KafkaErrorCode(err))).Inc()
			errors.Errors = append(errors.Errors, err)
		} else {
			metrics.ProducerMessages(ProducerMessageLabels(topic, "produced", "")).Inc()
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
					metrics.ProducerMessages(ProducerMessageLabels(topic, "error", metrics.KafkaErrorCode(m.TopicPartition.Error))).Inc()
					p.Errorf("Error sending message (ID: %s) to kafka topic %s: %v", messageId, *m.TopicPartition.Topic, m.TopicPartition.Error)
					errors.Errors = append(errors.Errors, m.TopicPartition.Error)
				} else {
					metrics.ProducerMessages(ProducerMessageLabels(topic, "delivered", "")).Inc()
					p.Debugf("Message ID: %s delivered to topic %s [%d] at offset %v", messageId, *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
			case <-until:
				metrics.ProducerMessages(ProducerMessageLabels(topic, "error", "sync_delivery_timeout")).Inc()
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
func (p *Producer) ProduceAsync(topic string, messageKey string, event []byte) error {
	if p.isClosed() {
		return p.NewError("producer is closed")
	}
	errors := multierror.Error{}
	var key []byte
	if messageKey != "" {
		key = []byte(messageKey)
	}
	err := p.producer.Produce(&kafka.Message{
		Key:            key,
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          event,
	}, nil)
	if err != nil {
		metrics.ProducerMessages(ProducerMessageLabels(topic, "error", metrics.KafkaErrorCode(err))).Inc()
		errors.Errors = append(errors.Errors, err)
	} else {
		metrics.ProducerMessages(ProducerMessageLabels(topic, "produced", "")).Inc()
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
	p.Infof("Closing producer.")
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

func ProducerMessageLabels(topicId string, status, errText string) (topic, destinationId, mode, tableName, st string, err string) {
	destinationId, mode, tableName, topicErr := ParseTopicId(topicId)
	if topicErr != nil {
		return topicId, "", "", "", status, errText
	} else {
		return topicId, destinationId, mode, tableName, status, errText
	}
}

func ProducerErrorLabels(topicId string, errText string) (topic, destinationId, mode, tableName, err string) {
	destinationId, mode, tableName, topicErr := ParseTopicId(topicId)
	if topicErr != nil {
		return topicId, "", "", "", errText
	} else {
		return topicId, destinationId, mode, tableName, errText
	}
}
