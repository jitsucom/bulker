package eventslog

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	eventsLogError = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "event_log",
		Name:      "error",
	}, []string{"errorType"})
	EventsLogError = func(errorType string) prometheus.Counter {
		return eventsLogError.WithLabelValues(errorType)
	}
)
