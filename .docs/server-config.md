# 🚚 Bulker-server configuration

* [Running Bulker](#running-bulker)
* [Common Parameters](#common-parameters)
* [Kafka Connection](#connection-to-kafka) 
  * [Advanced Kafka Tuning](#connection-to-kafka)
* [Redis Connection](#connection-to-redis) *(optional)*
* [Defining Destination](#connection-to-redis)
  * [Batching Strategy](#batching-strategy)] 
  * [Postgres / MySQL / Redshift / Snowflake credentials](#postgres--mysql--redshift--snowflake-credentials)
  * [Clickhouse](#clickhouse)
  * [BigQuery](#bigquery)

> **See also**
> [HTTP API](./http-api.md)

## Running Bulker

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

A list of auth tokens that authorizes user in HTTP interface separated by comma. Each token can be either:
 * `${token}` un-encrypted token value
 * `${salt}.${hash}` hashed token. `${salt}` should be random string. Hash is `base64(sha512($token + $salt + $BULKER_TOKEN_SECRET)`.
 * Token is `[0-9a-zA-Z_\-]` (only letters, digits, underscore and dash)
 

### `BULKER_TOKEN_SECRET`

*Optional, default value: empty string*

See above. A secret that is used for hashing tokens.

## Connection to Kafka


### `BULKER_KAFKA_BROKERS`

**Required**

List of Kafka brokers separated by comma. Each broker should be in format `host:port`.

### `BULKER_KAFKA_SSL`

If SSL should be enabled

> **Note**: 
> At the moment, Bulker do not verify SSL certificates 

### `BULKER_KAFKA_SASL` (aka Kafka auth)

Kafka authorization as JSON object `{"mechanism": "SCRAM-SHA-256|SCRAM-SHA-256|PLAIN", "username": "user", "password": "password"}`

## Kafka topic management (advanced)

Bulker automatically creates 2 topics per each table in destination. One topic is for main processing and the second
one is for failed events that should be retried. The topic names start with `in.id` prefix.

Parameters above define how topics are created

### `KAFKA_TOPIC_RETENTION_HOURS`

*Optional, default value: `168` (7 days)*

Main topic retention time in hours.

### `KAFKA_FAILED_TOPIC_RETENTION_HOURS`

*Optional, default value: `168` (7 days)*

Topic for failed events retention time in hours.

### `KAFKA_TOPIC_REPLICATION_FACTOR`

*Optional, default value: `1`*

Replication factor for topics.

> **Note**
> For production, it should be set to at least 2.

### `PRODUCER_WAIT_FOR_DELIVERY_MS`

*Optional, default value: `1000`*

Wait for delivery confirmation

### `BATCH_RUNNER_DEFAULT_PERIOD_SEC`

*Optional, default value: `300` (5 min)*

Default period for batch processing for destinations where `batch_period_sec` is not set explicitly.
Read more about batch processing configuration [below](#defining-destinations)

### `BATCH_RUNNER_DEFAULT_BATCH_SIZE`

*Optional, default value: `10000`*

Default period for batch processing for destinations where `batch_size` is not set explicitly.
Read more about batch processing configuration [below](#defining-destinations)

## Connection to Redis (optional)

If BULKER_REDIS_URL is set, Bulker will use Redis for storing a history of processed events
in following format:

 * `events_log:processed.all:<destination-id>` - all events that have been sent to `<destination-id>` in streaming mode. Includes failed events too
 * `events_log:processed.error:<destination-id>` - all events that have been sent to `<destination-id>` in streaming mode and failed
   * `processed.error` is a subset of `processed.all`
* `events_log:batch.all:<destination-id>` - all processed batches
* `events_log:batch.error:<destination-id>` - all failed batches
    * `batch.error` is a subset of `batch.all`

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

Bulker operates with destinations. Each destination is a connectio to database or storage services (GCS, S3, etc).

Each destination is a JSON-object 


There are two ways how to define list of destinations:

### With `BULKER_DESTINATION_*` environment variables

Each environment variable `BULKER_DESTINATION_*` defines a destination. The value of the variable is a JSON object. Example:

```shell
BULKER_DESTINATION_POSTGRES="{id: 'postgres', }"
```

### With Redis

Set `CONFIG_SOURCE_SOURCE` to `redis://...` or `rediss://...` and Bulker will read destinations from Redis `bulkerExportDestinations` key.


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
  mode: "string", // "stream" or "batch"
  updated_at: "2020-01-01T00:00:00Z",
  //maximum batch size. If not set, value of BATCH_RUNNER_DEFAULT_BATCH_SIZE is used
  //see "Batching strategy" section below
  batch_size: 10000,
  //maximum batch period in seconds. If not set, value of BATCH_RUNNER_DEFAULT_PERIOD_SEC is used
  //see "Batching strategy" section below
  batch_period_sec: 300, // optional, default value: 300,
  //how to connect to destination. Values are destination specific. See 
  credentials: {},
  options: {
    //(optional) mame of the field that contains unique event id. "id" by default
    "primary_key": "id",
    //field that contains timestamp of an event. If not set, bulker won't treat
    //events as time series
    "timestamp_field": "timestamp",
  },
}
```

### Batching strategy

Bulker buffers events and sends them to destination in batches if mode=`batch`. The batch is sent when
either one of the following is true:

 * `batch_size` events are buffered
 * `batch_period_sec` seconds passed since the first event in the batch was buffered


>**See also**
> [DB Feature Matrix](./db-feature-matrix.md)


### Streaming

If mode is `stream`, Bulker will send events to destination as soon as they are received. 


### Postgres / MySQL / Redshift / Snowflake credentials

Postrgres, MySQL, Redshift and Snowflake `credentials` shares same configuration structure

```json5
{
  host: "string",
  port: 5432,
  database: "string",
  default_schema: "",
  username: "string",
  password: "string",
  //custom SQL connection parameters
  parameters: {},
  //Only for Redshift. Intermediate S3 bucket for uploading data
  s3_config: {
    //bucket name
    bucket: "string",
    //bucker region. Seehttps://docs.aws.amazon.com/general/latest/gr/s3.html
    region: "string",
    //access credentials
    access_key_id: "string",
    secret_access_key: "string",
    //(optional) Folder inside bucker
    folder: "",
  }
}
```

### Clickhouse

```json5
{
  //list of Clickhouse nodes. See https://github.com/ClickHouse/clickhouse-go#dsn  
  dsns: ["string"],
  //name of the database
  db: "string",
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
  key_file: "string",
  //BigQuery dataset name
  bq_dataset: "string",
}
```






