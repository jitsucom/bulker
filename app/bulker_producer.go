package app

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hashicorp/go-multierror"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/utils"
	"time"
)

type Producer struct {
	producer *kafka.Producer

	waitForDelivery time.Duration
	closed          bool
}

// NewProducer creates new Producer
func NewProducer(config *AppConfig, kafkaConfig *kafka.ConfigMap) (*Producer, error) {
	producerConfig := kafka.ConfigMap(utils.MapPutAll(kafka.ConfigMap{
		//TODO: add producer specific config here
	}, *kafkaConfig))
	producer, err := kafka.NewProducer(&producerConfig)
	if err != nil {
		return nil, fmt.Errorf("error creating kafka producer: %w", err)

	}
	return &Producer{
		producer:        producer,
		waitForDelivery: time.Millisecond * time.Duration(config.ProducerWaitForDeliveryMs),
	}, nil
}

// ProduceSync TODO: transactional delivery
// produces messages to kafka
func (p *Producer) ProduceSync(topic string, events ...[]byte) error {
	if p.closed {
		return fmt.Errorf("producer is closed")
	}
	started := time.Now()
	deliveryChan := make(chan kafka.Event, len(events))
	errors := multierror.Error{}
	sent := 0
	for _, event := range events {
		err := p.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          event,
		}, deliveryChan)
		if err != nil {
			errors.Errors = append(errors.Errors, err)
		} else {
			sent++
		}
	}
	if sent > 0 {
		p.producer.Flush(2)
		logging.Infof("Sent %d messages to kafka topic %s in %s", sent, topic, time.Since(started))
		until := time.After(p.waitForDelivery)
	loop:
		for i := 0; i < sent; i++ {
			select {
			case e := <-deliveryChan:
				m := e.(*kafka.Message)
				if m.TopicPartition.Error != nil {
					logging.Errorf("Error sending message to kafka topic %s: %v", topic, m.TopicPartition.Error)
					errors.Errors = append(errors.Errors, m.TopicPartition.Error)
				} else {
					logging.Infof("Message delivered to topic %s [%d] at offset %v", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
			case <-until:
				logging.Errorf("Timeout waiting for delivery")
				errors.Errors = append(errors.Errors, fmt.Errorf("timeout waiting for delivery"))
				break loop
			}
		}
		logging.Infof("Delivered %d messages to kafka topic %s in %s", sent, topic, time.Since(started))
	}
	return errors.ErrorOrNil()
}

// Close closes producer
func (p *Producer) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true
	p.producer.Close()
	return nil
}
