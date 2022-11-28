# Bulker Queue

And HTTP interface that allows to throw objects to Bulker, while Bulker processes queue in background. Queue should rely on an external messaging service
(Kafka).

## Design requirements

* Bulker Queue should rely on Kafka, but it should be functional for a while (minutes hours, not days) event when Kafka is down
* Each Bulker agent should be stateless. We should expect that it could be stopped at any time
* All configuration should be done via env variables only

## Configuration

See `BULKER_HTTP_PORT`, `BULKER_CONFIG_*` and `AUTH_TOKENS` in [./http.md](HTTP Spec). Queue should share this settings and use same HTTP server

Queue specific parameters are:

* `BULKER_QUEUE_MODE` should be either `producer`, `consumer`, `producer-consumer` or `none`. If not set, the value is infered from presense of `BULKER_BROKER`
  variable; if the var is defined, `BULKER_QUEUE_MODE` should default to `producer-consumer`. Otherwise it should be `none`
* `BULKER_BROKER` should be an URL of message broker. So far we intend to support Kafka and Kafka-compatible platforms (such as Redpanda)
* `BULKER_KAFKA_*` Kafka specific settings if message broker is Kafka

## Destination Batching Configuration

## HTTP API

### `POST /post/{destinationId}?tableName=`

Body is JSON object representing single event. The response is either `{"success": true}` or `{"success": false, "error": ""}`

## Internal Implementation

Topic name pattern: `incoming.destinationId.{}.mode.{stream|batch}.tableName.{}` (query parametes should be always sorted by name).

