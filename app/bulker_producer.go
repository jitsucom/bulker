package app

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hashicorp/go-multierror"
	"github.com/jitsucom/bulker/base/objects"
	"github.com/jitsucom/bulker/base/utils"
	"time"
)

type Producer struct {
	objects.ServiceBase
	producer *kafka.Producer

	asyncDeliveryChannel chan kafka.Event
	waitForDelivery      time.Duration
	closed               bool
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
		waitForDelivery:      time.Millisecond * time.Duration(config.ProducerWaitForDeliveryMs),
	}, nil
}

func (p *Producer) Start() {
	go func() {
		for e := range p.producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					//TODO: check for retrieable errors
					p.Errorf("Error sending message to kafka topic %s: %w", ev.TopicPartition.Topic, ev.TopicPartition.Error)
				} else {
					p.Infof("Message delivered to topic %s [%d] at offset %v", *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
				//case kafka.Error:
				//	p.Debugf("Producer error: %w", ev)
			}
		}
		p.Infof("Producer closed")
	}()
}

// ProduceSync TODO: transactional delivery?
// produces messages to kafka
func (p *Producer) ProduceSync(topic string, events ...[]byte) error {
	if p.closed {
		return p.NewError("producer is closed")
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
		p.Infof("Sent %d messages to kafka topic %s in %s", sent, topic, time.Since(started))
		until := time.After(p.waitForDelivery)
	loop:
		for i := 0; i < sent; i++ {
			select {
			case e := <-deliveryChan:
				m := e.(*kafka.Message)
				if m.TopicPartition.Error != nil {
					p.Errorf("Error sending message to kafka topic %s: %v", topic, m.TopicPartition.Error)
					errors.Errors = append(errors.Errors, m.TopicPartition.Error)
				} else {
					p.Infof("Message delivered to topic %s [%d] at offset %v", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
			case <-until:
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
	if p.closed {
		return p.NewError("producer is closed")
	}
	errors := multierror.Error{}
	for _, event := range events {
		err := p.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          event,
		}, nil)
		if err != nil {
			errors.Errors = append(errors.Errors, err)
		}
	}
	return errors.ErrorOrNil()
}

// Close closes producer
func (p *Producer) Close() error {
	if p.closed {
		return nil
	}
	notProduced := p.producer.Flush(3000)
	if notProduced > 0 {
		p.Errorf("%d message left unsent in producer queue.", notProduced)
		//TODO: suck p.producer.ProduceChannel() and store to fallback file or some retry queue
	}
	p.closed = true
	p.producer.Close()
	close(p.asyncDeliveryChannel)
	return nil
}