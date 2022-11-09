# 🚚 Bulker

Bulker is a HTTP server and go library that simplifies loading large amounts of data into databases. It is designed to be 
used as a part of ETL pipelines. 

Bulker is a heart 💜 of [Jitsu](https://github.com/jitsucom/jitsu), an open-source data integration platform.

HTTP-server relies on Kafka for message delivery.   

<p align="center">
<img src="https://github.com/jitsucom/bulker/raw/main/.docs/assets/bulker-summary.excalidraw.png" width="600" />
</p>

## Features

* 🛢️ **Batching** - Bulker can send data in batches in most efficient way for particular database. For example, for Postgres it uses 
COPY command, for BigQuery it uses files
* 🚿 **Streaming** - alternatively, Bulker can stream data to database. It is useful when number of records is low, up to 10 records
per second for most databases
* 🐫 **Deduplication** - Bulker can optionally deduplicate data by primary key. 
* 📋 **Schema management** - Bulker can create tables and columns on the fly. It also flattens nested JSON-objects. Example if you send `{"a": {"b": 1}}` to 
bulker, it will make sure that there is a column `a_b` in the table (and will create it)
* 📌 **Implicit typing** - Bulker can infer types of columns from JSON-data.
* 📌 **Explicit typing** - Explicit types can be specified per column via StreamOptions. Bulker will use them to create tables and columns.  
TODO:
- [ ] Use explicit types from type hints, that can be placed right in the JSON as `{"a": "test", "__sql_type_a": "varchar(4)"}`.
Bulker, it will make sure that there is a column `a_b` in the table (and will create it)
* 📈 **Horizontal Scaling**. Bulker scales horrizontally. Too much data? No problem, just add more Bulker instances!
* 📦 **Dockerized** - Bulker is dockerized and can be deployed to any cloud provider and k8s. 
* ☁️ **Cloud Native** - each Bulker instance is stateless and is configured by only few environment variables. 


## Supported databases

<p align="center"><b>
Postgres • Redshit • Snowflake • BigQuery • Clickhouse • MySQL
</p></b>

<p align="center">
Coming soon
</p>

<p align="center"><b>
S3 • GCS
</p></b>


Not all features supported by all databases. See [DB Feature Matrix](.docs/db-feature-matrix.md) for details.

## Documentation

* [How to use Bulker as HTTP Service](./docs/server-howto.md)
* [How to use bulker as Go-lib](./docs/golib-howto.md)

## How It Works

Bulker App relies on Kafka server for routing incoming messages, managing queues, batches
