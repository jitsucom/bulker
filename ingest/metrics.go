package main

import (
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

	eventsLogError = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "event_log",
		Name:      "error",
	}, []string{"errorType"})
	EventsLogError = func(errorType string) prometheus.Counter {
		return eventsLogError.WithLabelValues(errorType)
	}

	panics = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "safego",
		Name:      "panic",
	})
	Panics = func() prometheus.Counter {
		return panics
	}
)
