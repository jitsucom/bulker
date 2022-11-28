# 🚚 Bulker

Bulker is a HTTP server and go-lib that simplifies streaming large amounts of data into databases. It is designed to be 
used as a part of ETL pipelines. 

Bulker is a heart 💜 of [Jitsu](https://github.com/jitsucom/jitsu), an open-source data integration platform.

HTTP-server relies on Kafka for message delivery.   

<p align="center">
<img src="https://github.com/jitsucom/bulker/raw/main/.docs/assets/bulker-summary.excalidraw.png" width="600" />
</p>

## Features

* 🛢️ **Batching** - Bulker sends data in batches in most efficient way for particular database. For example, for Postgres it uses 
COPY command, for BigQuery it uses files
* 🚿 **Streaming** - alternatively, Bulker can stream data to database. It is useful when number of records is low. Up to 10 records
per second for most databases
* 🐫 **Deduplication** - if configured, Bulker will deduplicate records by primary key 
* 📋 **Schema management** - Bulker creates tables and columns on the fly. It also flattens nested JSON-objects. Example if you send `{"a": {"b": 1}}` to 
bulker, it will make sure that there is a column `a_b` in the table (and will create it)
* 📌 **Implicit typing** - Bulker infers types of columns from JSON-data.
* 📌 **Explicit typing** - Explicit types can be by type hints that are placed in JSON. Example: for event `{"a": "test", "__sql_type_a": "varchar(4)"}`
Bulker wukk make sure that there is a column `a_b`, and it's type is `varchar(4)`.
* 📈 **Horizontal Scaling**. Bulker scales horizontally. Too much data? No problem, just add Bulker instances!
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
</b></p>


Not all features supported by all databases. See [DB Feature Matrix](.docs/db-feature-matrix.md) for details.

## Documentation

* [How to use Bulker as HTTP Service](./.docs/server-config.md)
  * [Server Configuration](./.docs/server-config.md)  
  * [HTTP API](./.docs/http-api.md)
* How to use bulker as Go-lib *(coming soon)*


## Dependencies

Bulker depends on Kafka for messaging. Optionally it uses Redis for logging progressed events.
