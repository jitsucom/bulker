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
