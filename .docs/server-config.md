# 🚚 Bulker-server configuration

* [Running Bulker](#running-bulker)
* [Common Parameters](#common-parameters)
* [Kafka Connection](#connection-to-kafka) 
* [Batching](#batching)
* [Streaming](#streaming)
* [Error Handling and Retries](#error-handling-and-retries)
* [Advanced Kafka Tuning](#kafka-topic-management--advanced-)
* [Redis Connection](#connection-to-redis--optional-) *(optional)*
* [Defining Destination](#defining-destinations)
  * [Postgres / MySQL / Redshift / Snowflake credentials](#postgres--mysql--redshift--snowflake-credentials)
  * [Clickhouse](#clickhouse)
  * [BigQuery](#bigquery)

> **See also**
> [HTTP API](./http-api.md)

## Running Bulker

The best way to run Bulker is to use [docker image](https://hub.docker.com/r/jitsucom/bulker).

 * Use `jitsucom/bulker:latest` for the last stable version
 * Use `jitsucom/bulker:canary` for the last build

Alternatively, you can build your own binary by running `go mod download && go build -o bulker`

Bulker is configured via environment variables. All variables are prefixed with
`BULKER_`. See the list of available variables below.

## Common parameters

### `BULKER_INSTANCE_ID`

*Optional, default value: `random uuid`*

ID of bulker instance. It is used for identifying Kafka consumers and metrics. If is not set,
instance id will be generated and persisted to disk (`~/.bulkerapp/instance_id`) and
reused on next restart.

### `BULKER_HTTP_PORT`

*Optional, default value: `3042`*


### `BULKER_AUTH_TOKENS`

*Optional, default value: `''`*

A list of auth tokens that authorizes user in HTTP interface separated by comma. Each token can be either:
 * `${token}` un-encrypted token value
 * `${salt}.${hash}` hashed token. `${salt}` should be random string. Hash is `base64(sha512($token + $salt + $BULKER_TOKEN_SECRET)`.
 * Token is `[0-9a-zA-Z_\-]` (only letters, digits, underscore and dash)

### `BULKER_TOKEN_SECRET`

*Optional, default value: empty string*

See above. A secret that is used for hashing tokens.

### `BULKER_MODE`

>**Note:** 
> Not available yet. At the moment Bulker is always running in `two-way` mode.

*Optional, default: `two-way`*

How this particular instance of Bulker should work. Possible values:

* `producer` - this instance will only listen to [HTTP requests](./http-api.md) and send data to Kafka. It won't consume from Kafka
* `consumer` - this instance will only consume from Kafka. It won't listen to [HTTP requests](./http-api.md), except for `/ready` and `/metrics` endpoints
* `two-way` - this instance will both listen produce messages from [HTTP requests](./http-api.md) and consume from Kafka


## Connection to Kafka

### `BULKER_KAFKA_BOOTSTRAP_SERVERS`

**Required**

List of Kafka brokers separated by comma. Each broker should be in format `host:port`.

### `BULKER_KAFKA_SSL`

If SSL should be enabled

### `BULKER_KAFKA_SSL_SKIP_VERIFY`

Skip SSL verification of kafka server certificate.

### `BULKER_KAFKA_SASL` (aka Kafka auth)

Kafka authorization as JSON object `{"mechanism": "SCRAM-SHA-256|PLAIN", "username": "user", "password": "password"}`


## Batching

Bulker buffers events and sends them to destination in batches if mode=`batch`. The batch is sent when
either one of the following is true:

* `batchSize` events are buffered
* `frequency` minutes passed since the first event in the batch was buffered. (float))

Batch settings that are default for all destinations may be set with following variables:

### `BULKER_BATCH_RUNNER_DEFAULT_PERIOD_SEC`

*Optional, default value: `300` (5 min)*

Default period for batch processing for destinations where `frequency` is not set explicitly.
Read more about batch processing configuration [below](#defining-destinations)

### `BULKER_BATCH_RUNNER_DEFAULT_BATCH_SIZE`

*Optional, default value: `10000`*

Default batch size for destinations where `batchSize` is not set explicitly.
Read more about batch processing configuration [below](#defining-destinations)

>**See also**
> [DB Feature Matrix](./db-feature-matrix.md)


## Streaming

If mode is `stream`, Bulker will send events to destination as soon as they are received.

## Error Handling and Retries

If Bulker fails to send events to destination, it can retry sending them with exponential backoff.
When error occurs, Bulker move events to Kafka topic dedicated to  Retry Consumer.
In streaming mode single failed event is moved to `retry` topic while in batch mode whole batch is moved to `retry` topic.

Retry Consumer is responsible for requeuing events from `retry` topic. It runs periodically and 
relocate events from `retry` topic to the original topic while incrementing retries attempt counter. 

If stream or batch consumer reaches max retry attempts for specific event, that event is moved to `dead` topic.

Parameters:

### `BULKER_MESSAGES_RETRY_COUNT`

*Optional, default value: `5`*

Max number of retry attempts.

### `BULKER_MESSAGES_RETRY_BACKOFF_BASE`

*Optional, default value: `5`*

Defines base for exponential backoff in minutes for retry attempts.
For example, if retry count is 3 and base is 5, then retry delays will be 5, 25, 125 minutes.

### `BULKER_MESSAGES_RETRY_BACKOFF_MAX_DELAY`

*Optional, default value: `1440`*

Defines maximum possible retry delay in minutes. Default: 1440 minutes = 24 hours

### `BULKER_BATCH_RUNNER_DEFAULT_RETRY_PERIOD_SEC`

*Optional, default value: `300` (5 min)*

Default period of running Retry Consumer for destinations where `retryPeriodSec` is not set explicitly.
Read more about batch processing configuration [below](#defining-destinations)

### `BULKER_BATCH_RUNNER_DEFAULT_RETRY_BATCH_SIZE`

*Optional, default value: `100`*

Default batch size for destination's Retry Consumer where `retryBatchSize` is not set explicitly.
Read more about batch processing configuration [below](#defining-destinations)

## Kafka topic management (advanced)

Bulker automatically creates 3 topics per each table in destination. One topic is for main processing, one is for failed events that should be retried and the last one for failed events that won't be retried - dead. The topic names has format `in.id.{destiantionId}.m.{mode}.t.{tableName}`.

Mode can be: `batch` or `stream`, `retry`, `dead`.

Parameters above define how topics are created

### `BULKER_KAFKA_TOPIC_RETENTION_HOURS`

*Optional, default value: `168` (7 days)*

Main topic retention time in hours.

### `BULKER_KAFKA_RETRY_TOPIC_RETENTION_HOURS`

*Optional, default value: `168` (7 days)*

Topic for retried events retention time in hours.

### `BULKER_KAFKA_DEAD_TOPIC_RETENTION_HOURS`

*Optional, default value: `168` (7 days)*

Topic for dead events retention time in hours.

### `BULKER_KAFKA_TOPIC_REPLICATION_FACTOR`

*Optional, default value: `1`*

Replication factor for topics.

> **Note**
> For production, it should be set to at least 2.

## Connection to Redis (optional)

If `BULKER_REDIS_URL` is set, Bulker will use Redis for storing a history of processed events
in following format:

 * `events_log:bulker_stream.all:<destination-id>` - all events that have been sent to `<destination-id>` in streaming mode. Includes failed events too
 * `events_log:bulker_stream.error:<destination-id>` - all events that have been sent to `<destination-id>` in streaming mode and failed

 * `events_log:bulker_batch.all:<destination-id>` - all processed batches including failed ones
 * `events_log:bulker_batch.error:<destination-id>` - all failed batches

Each key is a [redis stream](https://redis.io/docs/data-types/streams/)
 
### `BULKER_REDIS_URL`

**Optional**

Url for connecting to Redis: `redis[s]://[[username :]password@]host[:port][/database]`

Example: `redis://default:secret@localhost:6379`

> **Note**
> If username is not set use `default`

### `BULKER_EVENTS_LOG_MAX_SIZE`

*Optional, default value: `100000`*

Maximum number of events in each `events_log:*` stream


## Defining destinations

Bulker operates with destinations. Each destination is a connection to database or storage services (GCS, S3, etc).

Each destination is a JSON-object 


There are two ways how to define list of destinations:

### With `BULKER_DESTINATION_*` environment variables

Each environment variable `BULKER_DESTINATION_*` defines a destination. The value of the variable is a JSON object. Example:

```shell
BULKER_DESTINATION_POSTGRES="{id: 'postgres', }"
```

### With Redis

Set `BULKER_CONFIG_SOURCE` to `redis://...` or `rediss://...` and Bulker will read destinations from Redis `enrichedConnections` key.


### Destination parameters

Each destination is a JSON object:

```json5
{
  //unique id of destination. The id is referenced in HTTP-api
  id: "string", // unique destination id
  //"clickhouse", "postgres", "mysql", "snowflake", "redshift" or "bigquery"
  //"s3" and "gcs" are coming soom
  type: "string", // destination type, see below
  //optional (time in ISO8601 format) when destination has been updated
  updatedAt: "2020-01-01T00:00:00Z",
  //how to connect to destination. Values are destination specific. See 
  credentials: {},
  options: {
    mode: "string", // "stream" or "batch"
    //maximum batch size. If not set, value of BULKER_BATCH_RUNNER_DEFAULT_BATCH_SIZE is used
    //see "Batching" section above
    //default value: 10000
    batchSize: 10000,
    //period of running batch consumer in minutes (float). If not set, value of BULKER_BATCH_RUNNER_DEFAULT_PERIOD_SEC is used
    //see "Batching" section above
    //default value: 5
    frequency: 5, 
    //name of the field that contains unique event id.
    //optional
    primaryKey: "id", 
    //whether bulker should deduplicate events by primary key. See db-feature-matrix.md Requires primaryKey to be set. 
    //default value: false
    deduplicate: false, 
    //field that contains timestamp of an event. If set bulker will create destination tables optimized for range queries and sorting by provided column
    //optional
    timestamp: "timestamp",
    //batch size of retry consumer. If not set, value of BULKER_BATCH_RUNNER_DEFAULT_RETRY_BATCH_SIZE is used
    //see "Error Handling and Retries" section above
    //default value: 100
    retryBatchSize: 100, 
    //period of running retry consumer in minutes (float). If not set batchPeriodSec is used or BULKER_BATCH_RUNNER_DEFAULT_RETRY_PERIOD_SEC if batchPeriodSec is not set too.
    //see "Error Handling and Retries" section above
    //default value: 5
    retryFrequency: 5, 
  },
}
```

### Postgres / MySQL / Redshift / Snowflake credentials

Postrgres, MySQL, Redshift and Snowflake `credentials` shares same configuration structure

```json5
{
  host: "string",
  port: 5432,
  database: "string",
  defaultSchema: "",
  username: "string",
  password: "string",
  //custom SQL connection parameters
  parameters: {},
  //Only for Redshift. Intermediate S3 bucket for uploading data
  s3Config: {
    //bucket name
    bucket: "string",
    //bucker region. Seehttps://docs.aws.amazon.com/general/latest/gr/s3.html
    region: "string",
    //access credentials
    accessKeyId: "string",
    secretAccessKey: "string",
    //(optional) Folder inside bucker
    folder: "",
  }
}
```

### Clickhouse

```json5
{
  //Clickhouse protocol: clickhouse, clickhouse-secure, http or https
  protocol: "string",
  //list of clickhouse servers as host:port. If port is not specified, default port for respective protocol will be used. http → 8123, https → 8443, clickhouse → 9000, clickhouse-secure → 9440  
  hosts: ["string"],
  //map of parameters. See https://clickhouse.com/docs/en/integrations/go/clickhouse-go/database-sql-api/#connection-settings 
  parameters: {},
  username: "string",
  password: "string",
  //name of the database
  database: "string",
  //cluster name
  cluster: "string",
  //clickhouse engine settings. Defines how new tables are created in clickhouse
  engine: {
    //todo
  }
}
```


### BigQuery


```json5
{
  //service account credentials. See https://cloud.google.com/docs/authentication/production
  //Google Cloud project ID
  project: "string",
  //key file. Either JSON object or path to local file
  keyFile: "string",
  //BigQuery dataset name
  bqDataset: "string",
}
```






