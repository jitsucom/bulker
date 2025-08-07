package kafkabase

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	producerMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "producer",
		Name:      "messages",
	}, []string{"topicId", "destinationId", "mode", "tableName", "status", "errorType"})
	ProducerMessages = func(topicId, destinationId, mode, tableName, status, errorType string) prometheus.Counter {
		return producerMessages.WithLabelValues(topicId, destinationId, mode, tableName, status, errorType)
	}

	ProducerQueueLength = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "bulkerapp",
		Subsystem: "producer",
		Name:      "queue_length",
	})
)

func KafkaErrorCode(err error) string {
	if err == nil {
		return ""
	}

	if kafkaError, ok := err.(kafka.Error); ok {
		return fmt.Sprintf("kafka %serror: %s", utils.Ternary(kafkaError.IsRetriable(), "retriable ", ""), kafkaError.Code().String())
	}

	return "kafka_error"
}

func KafkaRetryableError(err error) bool {
	if err == nil {
		return false
	}

	if kafkaError, ok := err.(kafka.Error); ok {
		return kafkaError.IsRetriable()
	}

	return true
}
