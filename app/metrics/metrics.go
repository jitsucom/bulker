package metrics

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	EventsHandlerError = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "events_handler",
		Name:      "error",
		Help:      "Events handler errors by destination Id",
	}, []string{"destinationId", "tableName", "errorType"})

	EventsHandlerSuccess = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "events_handler",
		Name:      "success",
		Help:      "Events handler successes by destination Id",
	}, []string{"destinationId", "tableName"})

	TopicManagerCreateSuccess = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "topic_manager",
		Name:      "create_success",
	})
	TopicManagerCreateError = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "topic_manager",
		Name:      "create_error",
	}, []string{"errorType"})
	TopicManagerError = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "topic_manager",
		Name:      "error",
	}, []string{"errorType"})
	TopicManagerDestinationsError = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bulkerapp",
		Subsystem: "topic_manager",
		Name:      "destination_error",
		Help:      "Number of destination topics with errors by destination mode",
	}, []string{"mode"})
	TopicManagerDestinationTopics = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bulkerapp",
		Subsystem: "topic_manager",
		Name:      "destination_topics",
		Help:      "Total number of destination topics destination mode. Including ones with errors",
	}, []string{"mode"})
	TopicManagerAbandonedTopics = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "bulkerapp",
		Subsystem: "topic_manager",
		Name:      "abandoned_topics",
		Help:      "Number of abandoned topics. Abandoned topics are destination topics that are not used by any destination",
	})
	TopicManagerOtherTopics = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "bulkerapp",
		Subsystem: "topic_manager",
		Name:      "other_topics",
		Help:      "Number of other topics. Other topics are any kafka topics not managed by the topic manager",
	})

	ProducerMessagesProduced = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "producer",
		Name:      "messages_produced",
	}, []string{"destinationId", "mode", "tableName"})

	ProducerMessagesDelivered = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "producer",
		Name:      "messages_delivered",
	}, []string{"destinationId", "mode", "tableName"})

	ProducerProduceErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "producer",
		Name:      "produce_error",
	}, []string{"destinationId", "mode", "tableName", "errorType"})

	ProducerDeliveryErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "producer",
		Name:      "delivery_error",
	}, []string{"destinationId", "mode", "tableName", "errorType"})

	ProducerQueueLength = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "bulkerapp",
		Subsystem: "producer",
		Name:      "queue_length",
	})

	StreamConsumerMessageConsumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "stream_consumer",
		Name:      "message_consumed",
	}, []string{"destinationId", "tableName"})

	StreamConsumerMessageErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "stream_consumer",
		Name:      "message_error",
	}, []string{"destinationId", "tableName", "errorType"})

	StreamConsumerErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "stream_consumer",
		Name:      "error",
	}, []string{"destinationId", "tableName", "errorType"})

	BatchConsumerMessageConsumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "batch_consumer",
		Name:      "message_consumed",
	}, []string{"destinationId", "tableName"})

	BatchConsumerMessageErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "batch_consumer",
		Name:      "message_error",
	}, []string{"destinationId", "tableName", "errorType"})

	BatchConsumerErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "batch_consumer",
		Name:      "error",
	}, []string{"destinationId", "tableName", "errorType"})

	BatchConsumerBatchSuccesses = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "batch_consumer",
		Name:      "batch_success",
	}, []string{"destinationId", "tableName"})

	BatchConsumerBatchMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "batch_consumer",
		Name:      "messages",
	}, []string{"destinationId", "tableName"})

	BatchConsumerBatchFailedMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "batch_consumer",
		Name:      "failed_messages",
	}, []string{"destinationId", "tableName"})

	BatchConsumerBatchFailedMessagesRelocated = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "batch_consumer",
		Name:      "failed_messages_relocated",
	}, []string{"destinationId", "tableName"})

	BatchConsumerBatchFails = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "batch_consumer",
		Name:      "batch_fail",
	}, []string{"destinationId", "tableName"})
)

func KafkaErrorCode(err error) string {
	if err == nil {
		return ""
	}

	if kafkaError, ok := err.(kafka.Error); ok {
		return fmt.Sprintf("kafka error %d", kafkaError.Code())
	}

	return "kafka_error"
}
