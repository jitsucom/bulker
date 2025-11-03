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

	ingestedMessagesReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "ingest",
		Name:      "messages_received",
		Help:      "Messages ingested by stream Id",
	}, []string{"streamId", "status"})
	IngestedMessagesReceived = func(streamId, status string) prometheus.Counter {
		return ingestedMessagesReceived.WithLabelValues(streamId, status)
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

	deviceFunctions = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bulkerapp",
		Subsystem: "ingest",
		Name:      "device_functions",
		Help:      "Device Functions enrichment status by destination Id",
	}, []string{"destinationId", "status"})
	DeviceFunctions = func(destinationId, status string) prometheus.Counter {
		return deviceFunctions.WithLabelValues(destinationId, status)
	}

	repositoryErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "ingest",
		Subsystem: "repository",
		Name:      "error",
	})
	RepositoryErrors = func() prometheus.Counter {
		return repositoryErrors
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
