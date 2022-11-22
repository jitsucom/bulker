package metrics

import (
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
)
