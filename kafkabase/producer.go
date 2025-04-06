package kafkabase

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/hashicorp/go-multierror"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/safego"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"sync/atomic"
	"time"
)

const MessageIdHeader = "message_id"

type MetricsLabelsFunc func(topicId string, status, errText string) (topic, destinationId, mode, tableName, st string, err string)

type PartitionSelector interface {
	SelectPartition() int32
}

type DummyPartitionSelector struct {
}

func (dps *DummyPartitionSelector) SelectPartition() int32 {
	return kafka.PartitionAny

}

type Producer struct {
	appbase.Service
	producer *kafka.Producer

	reportQueueLength    bool
	asyncDeliveryChannel chan kafka.Event
	waitForDelivery      time.Duration
	closed               atomic.Bool
	metricsLabelFunc     MetricsLabelsFunc
}

// NewProducer creates new Producer
func NewProducer(config *KafkaConfig, kafkaConfig *kafka.ConfigMap, reportQueueLength bool, metricsLabelFunc MetricsLabelsFunc) (*Producer, error) {
	base := appbase.NewServiceBase("producer")
	producer, err := kafka.NewProducer(kafkaConfig)
	if err != nil {
		return nil, base.NewError("error creating kafka producer: %v", err)

	}
	if metricsLabelFunc == nil {
		metricsLabelFunc = defaultMetricsLabelFunc
	}
	return &Producer{
		Service:              base,
		producer:             producer,
		reportQueueLength:    reportQueueLength,
		asyncDeliveryChannel: make(chan kafka.Event, 1000),
		waitForDelivery:      time.Millisecond * time.Duration(config.ProducerWaitForDeliveryMs),
		metricsLabelFunc:     metricsLabelFunc,
	}, nil
}

func (p *Producer) Start() {
	safego.RunWithRestart(func() {
		for e := range p.producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				//messageId := GetKafkaHeader(ev, MessageIdHeader)
				if ev.TopicPartition.Error != nil {
					//TODO: check for retrieable errors
					ProducerMessages(p.metricsLabelFunc(*ev.TopicPartition.Topic, "error", KafkaErrorCode(ev.TopicPartition.Error))).Inc()
					p.Errorf("Error sending message to kafka topic %s: %s", *ev.TopicPartition.Topic, ev.TopicPartition.Error.Error())
				} else {
					ProducerMessages(p.metricsLabelFunc(*ev.TopicPartition.Topic, "delivered", "")).Inc()
					//p.Debugf("Message ID: %s delivered to topic %s [%d] at offset %v", messageId, *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			case *kafka.Error, kafka.Error:
				p.Errorf("Producer error: %v", ev)
			}
		}
		p.Infof("Producer closed")
	})
	if p.reportQueueLength {
		// report size metrics
		safego.RunWithRestart(func() {
			ticker := time.NewTicker(time.Second * 15)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					ProducerQueueLength.Set(float64(p.producer.Len()))
					if p.closed.Load() {
						return
					}
				}
			}
		})
	}
}

// ProduceSync TODO: transactional delivery?
// produces messages to kafka
func (p *Producer) ProduceSync(topic string, event kafka.Message) error {
	if p.isClosed() {
		return p.NewError("producer is closed")
	}
	started := time.Now()
	deliveryChan := make(chan kafka.Event, 1)
	err := p.producer.Produce(&event, deliveryChan)
	if err != nil {
		ProducerMessages(p.metricsLabelFunc(topic, "error", KafkaErrorCode(err))).Inc()
		return err
	} else {
		ProducerMessages(p.metricsLabelFunc(topic, "produced", "")).Inc()
	}
	p.Debugf("Sent message to kafka topic %s in %s", topic, time.Since(started))
	until := time.After(p.waitForDelivery)
	select {
	case e := <-deliveryChan:
		m := e.(*kafka.Message)
		//messageId := GetKafkaHeader(m, MessageIdHeader)
		if m.TopicPartition.Error != nil {
			ProducerMessages(p.metricsLabelFunc(topic, "error", KafkaErrorCode(m.TopicPartition.Error))).Inc()
			p.Errorf("Error sending message to kafka topic %s: %v", *m.TopicPartition.Topic, m.TopicPartition.Error)
			return m.TopicPartition.Error
		} else {
			ProducerMessages(p.metricsLabelFunc(topic, "delivered", "")).Inc()
			//p.Debugf("Message ID: %s delivered to topic %s [%d] at offset %v", messageId, *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
		}
	case <-until:
		ProducerMessages(p.metricsLabelFunc(topic, "error", "sync_delivery_timeout")).Inc()
		p.Errorf("Timeout waiting for delivery")
		return fmt.Errorf("timeout waiting for delivery")
	}
	p.Infof("Delivered message to kafka topic %s in %s", topic, time.Since(started))
	return nil
}

// ProduceAsync TODO: transactional delivery?
// produces messages to kafka
func (p *Producer) ProduceAsync(topic string, messageKey string, event []byte, headers map[string]string, partition int32) error {
	if p.isClosed() {
		return p.NewError("producer is closed")
	}
	errors := multierror.Error{}
	var key []byte
	if messageKey != "" {
		key = []byte(messageKey)
	}
	err := p.producer.Produce(&kafka.Message{
		Key: key,
		Headers: utils.MapToSlice(headers, func(k string, v string) kafka.Header {
			return kafka.Header{Key: k, Value: []byte(v)}
		}),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: partition},
		Value:          event,
	}, nil)
	if err != nil {
		ProducerMessages(p.metricsLabelFunc(topic, "error", KafkaErrorCode(err))).Inc()
		errors.Errors = append(errors.Errors, err)
	} else {
		ProducerMessages(p.metricsLabelFunc(topic, "produced", "")).Inc()
	}
	return errors.ErrorOrNil()
}

// Close closes producer
func (p *Producer) Close() error {
	if p == nil || p.isClosed() {
		return nil
	}
	p.closed.Store(true)
	notProduced := p.producer.Flush(10000)
	if notProduced > 0 {
		p.Errorf("%d message left unsent in producer queue.", notProduced)
		//TODO: suck p.producer.ProduceChannel() and store to fallback file or some retry queue
	}
	p.Infof("Closing producer.")
	p.producer.Close()
	close(p.asyncDeliveryChannel)
	return nil
}

func (p *Producer) isClosed() bool {
	return p.closed.Load()
}

func (p *Producer) QueueSize() (int, error) {
	if p.isClosed() {
		return 0, p.NewError("producer is closed")
	}

	return p.producer.Len(), nil
}

func defaultMetricsLabelFunc(topicId string, status, errText string) (topic, destinationId, mode, tableName, st string, err string) {
	return topicId, "", "", "", status, errText
}
