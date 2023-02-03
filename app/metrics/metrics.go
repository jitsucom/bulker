package metrics

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ingestHandlerRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "handler",
		Name:      "ingest",
		Help:      "Ingest handler errors by destination Id",
	}, []string{"slug", "status", "errorType"})
	IngestHandlerRequests = func(slug, status, errorType string) prometheus.Counter {
		return ingestHandlerRequests.WithLabelValues(slug, status, errorType)
	}

	ingestedMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "ingest",
		Name:      "messages",
		Help:      "Messages ingested by destination Id",
	}, []string{"destinationId", "status", "errorType"})
	IngestedMessages = func(destinationId, status, errorType string) prometheus.Counter {
		return ingestedMessages.WithLabelValues(destinationId, status, errorType)
	}

	eventsHandlerRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "handler",
		Name:      "events",
		Help:      "Events handler errors by destination Id",
	}, []string{"destinationId", "mode", "tableName", "status", "errorType"})
	EventsHandlerRequests = func(destinationId, mode, tableName, status, errorType string) prometheus.Counter {
		return eventsHandlerRequests.WithLabelValues(destinationId, mode, tableName, status, errorType)
	}

	topicManagerCreate = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "topic_manager",
		Name:      "create",
	}, []string{"topicId", "destinationId", "mode", "tableName", "status", "errorType"})
	TopicManagerCreate = func(topicId, destinationId, mode, tableName, status, errorType string) prometheus.Counter {
		return topicManagerCreate.WithLabelValues(topicId, destinationId, mode, tableName, status, errorType)
	}

	topicManagerError = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "topic_manager",
		Name:      "error",
	}, []string{"errorType"})
	TopicManagerError = func(errorType string) prometheus.Counter {
		return topicManagerError.WithLabelValues(errorType)
	}

	topicManagerDestinations = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bulkerapp",
		Subsystem: "topic_manager",
		Name:      "destinations",
		Help:      "Number of destination topics with errors by destination mode",
	}, []string{"mode", "status"})
	TopicManagerDestinations = func(mode, status string) prometheus.Gauge {
		return topicManagerDestinations.WithLabelValues(mode, status)
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

	consumerErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "consumer",
		Name:      "error",
	}, []string{"topicId", "mode", "destinationId", "tableName", "errorType"})
	ConsumerErrors = func(topicId, mode, destinationId, tableName, errorType string) prometheus.Counter {
		return consumerErrors.WithLabelValues(topicId, mode, destinationId, tableName, errorType)
	}

	consumerMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "consumer",
		Name:      "messages",
	}, []string{"topicId", "mode", "destinationId", "tableName", "status"})
	ConsumerMessages = func(topicId, mode, destinationId, tableName, status string) prometheus.Counter {
		return consumerMessages.WithLabelValues(topicId, mode, destinationId, tableName, status)
	}

	consumerRuns = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "consumer",
		Name:      "run",
	}, []string{"topicId", "mode", "destinationId", "tableName", "status"})
	ConsumerRuns = func(topicId, mode, destinationId, tableName, status string) prometheus.Counter {
		return consumerRuns.WithLabelValues(topicId, mode, destinationId, tableName, status)
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

	repositoryDestinations = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "repository",
		Name:      "destinations",
	}, []string{"status"})
	RepositoryDestinations = func(status string) prometheus.Counter {
		return repositoryDestinations.WithLabelValues(status)
	}

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
		return fmt.Sprintf("kafka error: %s", kafkaError.Code().String())
	}

	return "kafka_error"
}
