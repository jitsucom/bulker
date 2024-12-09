# 🚚 Bulker

Bulker is a tool for streaming and batching large amounts of semi-structured data into data warehouses. It uses Kafka internally.

## How it works

<p align="center">
<img src="./.docs/assets/bulker-summary.excalidraw.png" width="600" />
</p>

Send a JSON object to the Bulker HTTP endpoint, and it will ensure it is saved to the data warehouse:
 
 * **JSON flattening**. The JSON object will be flattened - `{a: {b: 1}}` becomes `{a_b: 1}`.
 * **Schema management** for **semi-structured** data. For each field, Bulker will ensure that a corresponding column exists in the destination table. If not, Bulker will create it. The type will be best-guessed by value, or it can be explicitly set via a type hint as in `{"a": "test", "__sql_type_a": "varchar(4)"}`.
 * **Reliability**. Bulker will place the object in a Kafka queue immediately, so if the data warehouse is down, data won't be lost.
 * **Streaming** or **Batching**. Bulker will send data to the data warehouse either as soon as it becomes available in Kafka (streaming) or after some time (batching). Most data warehouses won't tolerate a large number of inserts, which is why we implemented batching.

Bulker is at the 💜 of [Jitsu](https://github.com/jitsucom/jitsu), an open-source data integration platform.

See the full list of features below.

Bulker is also available as a Go library if you want to embed it into your application instead of using an HTTP server.

## Features

* 🛢️ **Batching** - Bulker sends data in batches in the most efficient way for each particular database. For example, for Postgres, it uses the COPY command; for BigQuery, it uses batch files.
* 🚿 **Streaming** - Alternatively, Bulker can stream data to the database. This is useful when the number of records is low, up to 10 records per second for most databases.
* 🐫 **Deduplication** - If configured, Bulker will deduplicate records by primary key.
* 📋 **Schema management** - Bulker creates tables and columns on the fly. It also flattens nested JSON objects. For example, if you send `{"a": {"b": 1}}` to Bulker, it will ensure that there is a column `a_b` in the table (and will create it if needed).
* 🦾 **Implicit typing** - Bulker infers column types from JSON data.
* 📌 **Explicit typing** - Explicit types can be set by type hints placed in the JSON. For example, for the event `{"a": "test", "__sql_type_a": "varchar(4)"}`, Bulker will ensure that there is a column `a`, and its type is `varchar(4)`.
* 📈 **Horizontal Scaling** - Bulker scales horizontally. Too much data? No problem, just add more Bulker instances!
* 📦 **Dockerized** - Bulker is containerized and can be deployed to any cloud provider and Kubernetes.
* ☁️ **Cloud Native** - Each Bulker instance is stateless and is configured with only a few environment variables.

## Supported databases

Bulker supports the following databases:

 * ✅ PostgreSQL <br/>
 * ✅ Redshift <br/>
 * ✅ Snowflake <br/>
 * ✅ ClickHouse <br/>
 * ✅ BigQuery <br/>
 * ✅ MySQL <br/>
 * ✅ S3 <br/>
 * ✅ GCS <br/>

Please see the [Compatibility Matrix](./.docs/db-feature-matrix.md) to learn which Bulker features are supported by each database.

## Documentation Links

> **Note**
> We highly recommend reading the [Core Concepts](#core-concepts) below before diving into the details.

* [How to use Bulker as HTTP Service](./.docs/server-config.md)
  * [Server Configuration](./.docs/server-config.md)  
  * [HTTP API](./.docs/http-api.md)
* How to use Bulker as a Go library *(coming soon)*

## Core Concepts

### Destinations

Bulker operates with destinations. A destination is a database or storage service (e.g., S3, GCS). Each destination has an ID and configuration represented by a JSON object.

Bulker exposes an HTTP API to load data into destinations, where those destinations are referenced by their IDs.

If the destination is a database, you'll need to provide a destination table name.

### Event

The main unit of data in Bulker is an *event*. An event is represented as a JSON object.

### Batching and Streaming (aka Destination Mode)

Bulker can send data to databases in two ways:
 * **Streaming**. Bulker sends events to the destination one by one. This is useful when the number of events is low (less than 10 events per second for most databases).
 * **Batching**. Bulker accumulates events in batches and sends them periodically once the batch is full or a timeout is reached. Batching is more efficient for large amounts of events, especially for cloud data warehouses (e.g., PostgreSQL, ClickHouse, BigQuery).

<p align="center">
<img src="./.docs/assets/stream-batch.excalidraw.png" width="600" />
</p>

### Primary Keys and Deduplication

Optionally, Bulker can deduplicate events by primary key. This is useful when the same event can be sent to Bulker multiple times. If available, Bulker uses primary keys, but for some data warehouses, alternative strategies are used.

>[Read more about deduplication »](./.docs/db-feature-matrix.md)
