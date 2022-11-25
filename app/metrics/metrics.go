package metrics

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	eventsHandlerError = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "events_handler",
		Name:      "error",
		Help:      "Events handler errors by destination Id",
	}, []string{"destinationId", "tableName", "errorType"})
	EventsHandlerError = func(destinationId, tableName, errorType string) prometheus.Counter {
		return eventsHandlerError.WithLabelValues(destinationId, tableName, errorType)
	}

	eventsHandlerSuccess = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "events_handler",
		Name:      "success",
		Help:      "Events handler successes by destination Id",
	}, []string{"destinationId", "tableName"})
	EventsHandlerSuccess = func(destinationId, tableName string) prometheus.Counter {
		return eventsHandlerSuccess.WithLabelValues(destinationId, tableName)
	}

	topicManagerCreateSuccess = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "topic_manager",
		Name:      "create_success",
	}, []string{"destinationId", "mode", "tableName", "errorType"})
	TopicManagerCreateSuccess = func(destinationId, mode, tableName string) prometheus.Counter {
		return topicManagerCreateSuccess.WithLabelValues(destinationId, mode, tableName)
	}

	topicManagerCreateError = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "topic_manager",
		Name:      "create_error",
	}, []string{"destinationId", "mode", "tableName", "errorType"})
	TopicManagerCreateError = func(destinationId, mode, tableName, errorType string) prometheus.Counter {
		return topicManagerCreateError.WithLabelValues(errorType)
	}

	topicManagerError = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "topic_manager",
		Name:      "error",
	}, []string{"errorType"})
	TopicManagerError = func(errorType string) prometheus.Counter {
		return topicManagerError.WithLabelValues(errorType)
	}

	topicManagerDestinationsError = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bulkerapp",
		Subsystem: "topic_manager",
		Name:      "destination_error",
		Help:      "Number of destination topics with errors by destination mode",
	}, []string{"mode"})
	TopicManagerDestinationsError = func(mode string) prometheus.Gauge {
		return topicManagerDestinationsError.WithLabelValues(mode)
	}

	topicManagerDestinationTopics = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bulkerapp",
		Subsystem: "topic_manager",
		Name:      "destination_topics",
		Help:      "Total number of destination topics destination mode. Including ones with errors",
	}, []string{"mode"})
	TopicManagerDestinationTopics = func(mode string) prometheus.Gauge {
		return topicManagerDestinationTopics.WithLabelValues(mode)
	}

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

	producerMessagesProduced = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "producer",
		Name:      "messages_produced",
	}, []string{"destinationId", "mode", "tableName"})
	ProducerMessagesProduced = func(destinationId, mode, tableName string) prometheus.Counter {
		return producerMessagesProduced.WithLabelValues(destinationId, mode, tableName)
	}

	producerMessagesDelivered = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "producer",
		Name:      "messages_delivered",
	}, []string{"destinationId", "mode", "tableName"})
	ProducerMessagesDelivered = func(destinationId, mode, tableName string) prometheus.Counter {
		return producerMessagesDelivered.WithLabelValues(destinationId, mode, tableName)
	}

	producerProduceErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "producer",
		Name:      "produce_error",
	}, []string{"destinationId", "mode", "tableName", "errorType"})
	ProducerProduceErrors = func(destinationId, mode, tableName, errorType string) prometheus.Counter {
		return producerProduceErrors.WithLabelValues(destinationId, mode, tableName, errorType)
	}

	producerDeliveryErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "producer",
		Name:      "delivery_error",
	}, []string{"destinationId", "mode", "tableName", "errorType"})
	ProducerDeliveryErrors = func(destinationId, mode, tableName, errorType string) prometheus.Counter {
		return producerDeliveryErrors.WithLabelValues(destinationId, mode, tableName, errorType)
	}

	ProducerQueueLength = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "bulkerapp",
		Subsystem: "producer",
		Name:      "queue_length",
	})

	streamConsumerMessageConsumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "stream_consumer",
		Name:      "message_consumed",
	}, []string{"destinationId", "tableName"})
	StreamConsumerMessageConsumed = func(destinationId, tableName string) prometheus.Counter {
		return streamConsumerMessageConsumed.WithLabelValues(destinationId, tableName)
	}

	streamConsumerMessageErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "stream_consumer",
		Name:      "message_error",
	}, []string{"destinationId", "tableName", "errorType"})
	StreamConsumerMessageErrors = func(destinationId, tableName, errorType string) prometheus.Counter {
		return streamConsumerMessageErrors.WithLabelValues(destinationId, tableName, errorType)
	}

	streamConsumerErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "stream_consumer",
		Name:      "error",
	}, []string{"destinationId", "tableName", "errorType"})
	StreamConsumerErrors = func(destinationId, tableName, errorType string) prometheus.Counter {
		return streamConsumerErrors.WithLabelValues(destinationId, tableName, errorType)
	}

	batchConsumerMessageConsumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "batch_consumer",
		Name:      "message_consumed",
	}, []string{"destinationId", "tableName"})
	BatchConsumerMessageConsumed = func(destinationId, tableName string) prometheus.Counter {
		return batchConsumerMessageConsumed.WithLabelValues(destinationId, tableName)
	}

	batchConsumerMessageErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "batch_consumer",
		Name:      "message_error",
	}, []string{"destinationId", "tableName", "errorType"})
	BatchConsumerMessageErrors = func(destinationId, tableName, errorType string) prometheus.Counter {
		return batchConsumerMessageErrors.WithLabelValues(destinationId, tableName, errorType)
	}

	batchConsumerErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "batch_consumer",
		Name:      "error",
	}, []string{"destinationId", "tableName", "errorType"})
	BatchConsumerErrors = func(destinationId, tableName, errorType string) prometheus.Counter {
		return batchConsumerErrors.WithLabelValues(destinationId, tableName, errorType)
	}

	batchConsumerBatchSuccesses = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "batch_consumer",
		Name:      "batch_success",
	}, []string{"destinationId", "tableName"})
	BatchConsumerBatchSuccesses = func(destinationId, tableName string) prometheus.Counter {
		return batchConsumerBatchSuccesses.WithLabelValues(destinationId, tableName)
	}

	batchConsumerBatchRuns = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "batch_consumer",
		Name:      "run",
	}, []string{"destinationId", "tableName"})
	BatchConsumerBatchRuns = func(destinationId, tableName string) prometheus.Counter {
		return batchConsumerBatchRuns.WithLabelValues(destinationId, tableName)
	}

	batchConsumerBatchMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "batch_consumer",
		Name:      "messages",
	}, []string{"destinationId", "tableName"})
	BatchConsumerBatchMessages = func(destinationId, tableName string) prometheus.Counter {
		return batchConsumerBatchMessages.WithLabelValues(destinationId, tableName)
	}

	batchConsumerBatchFailedMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "batch_consumer",
		Name:      "failed_messages",
	}, []string{"destinationId", "tableName"})
	BatchConsumerBatchFailedMessages = func(destinationId, tableName string) prometheus.Counter {
		return batchConsumerBatchFailedMessages.WithLabelValues(destinationId, tableName)
	}

	batchConsumerBatchFailedMessagesRelocated = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "batch_consumer",
		Name:      "failed_messages_relocated",
	}, []string{"destinationId", "tableName"})
	BatchConsumerBatchFailedMessagesRelocated = func(destinationId, tableName string) prometheus.Counter {
		return batchConsumerBatchFailedMessagesRelocated.WithLabelValues(destinationId, tableName)
	}

	batchConsumerBatchFails = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "batch_consumer",
		Name:      "batch_fail",
	}, []string{"destinationId", "tableName"})
	BatchConsumerBatchFails = func(destinationId, tableName string) prometheus.Counter {
		return batchConsumerBatchFails.WithLabelValues(destinationId, tableName)
	}

	redisConfigurationSourceError = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "redis_configuration",
		Name:      "error",
	}, []string{"errorType"})
	RedisConfigurationSourceError = func(errorType string) prometheus.Counter {
		return redisConfigurationSourceError.WithLabelValues(errorType)
	}

	RedisConfigurationSourceDestinations = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "bulkerapp",
		Subsystem: "redis_configuration",
		Name:      "destinations",
	})

	RepositoryAddedDestinations = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "repository",
		Name:      "destination_added",
	})
	RepositoryChangedDestinations = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "repository",
		Name:      "destination_changed",
	})
	RepositoryRemovedDestinations = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "repository",
		Name:      "destination_removed",
	})
	repositoryDestinationInitError = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "repository",
		Name:      "destination_init_error",
	}, []string{"destinationId"})
	RepositoryDestinationInitError = func(destinationId string) prometheus.Counter {
		return repositoryDestinationInitError.WithLabelValues(destinationId)
	}

	eventsLogError = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "event_log",
		Name:      "error",
	}, []string{"errorType"})
	EventsLogError = func(errorType string) prometheus.Counter {
		return eventsLogError.WithLabelValues(errorType)
	}
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
