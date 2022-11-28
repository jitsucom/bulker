# 🚚 Bulker-server configuration

Bulker is configured by environment variables. The doc

## Common parameters

### `BULKER_INSTANCE_ID`

*Optional, default value: `random uuid`*

ID of bulker instance. It is used for identifying Kafka consumers. If is not set,
instance id will be generated and persisted to disk (`/var/bulker/instance_id`) and
reused on next restart.

### `BULKER_HTTP_PORT`

*Optional, default value: `3042`*

### `BULKER_AUTH_TOKENS`

*Optional, default value: `''`*

TODO:

### `BULKER_TOKEN_SECRET`

*Optional, default value: ``*

TODO:

## Connection to Kafka

### `BULKER_KAFKA_BROKERS`

**Required**

### `BULKER_KAFKA_SSL`

### `BULKER_KAFKA_SASL`

## Kafka topic management

Per each destination

### `KAFKA_TOPIC_RETENTION_HOURS`

### `KAFKA_FAILED_TOPIC_RETENTION_HOURS`

## Connection to Redis

### `BULKER_REDIS_URL`

**Required**

Url for connecting to Redis: `redis[s]://[[username :]password@]host[:port][/database]`

Example: `redis://default:secret@localhost:6379`

> **Note**
> If username is not set use `default`

### `BULKER_REDIS_URL`


## Defining destinations

Bulker operates with destinations. Each destination is a connectio to database or storage services (GCS, S3, etc).

Each destination is a JSON-object 



