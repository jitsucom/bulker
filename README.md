# 🚚 Bulker

Bulker is a go-library and http server that simplifies loading large amounts of data into databases. It is designed to be used as a part of ETL pipelines.
Bulker is a heart 💜 of [Jitsu](https://github.com/jitsucom/jitsu), an open-source data integration platform.

The interface of Bulker is very simple, it accepts a set (stream) of JSON objects (or JSON-like go structures) and pushes them into a database. It takes care of batching, schema management, deduplication, and other optimizations.

## Features

* 🛢️ **Batching** - Bulker can send data in batches in most efficient way for particular database. For example, for Postgres it uses COPY command, for BigQuery it uses files
* 🚿 **Streaming** - alternatively, Bulker can stream data to database. It is useful for small amounts of data
* 🐫 **Deduplication** - Bulker can optionally deduplicate data by primary key. 
* 📋 **Schema management** - Bulker can create tables and columns on the fly. It also flattens nested JSON-objects. Example if you send `{"a": {"b": 1}}` to 
builker, it will make sure that there is a column `a_b` in the table (and will create it)
* 📌 **Implicit typing** - Bulker can infer types of columns from JSON-data.
* 📌 **Explicit typing** - Explicit types can be specified per column via StreamOptions. Bulker will use them to create tables and columns.  
TODO:
- [ ] Use explicit types from type hints, that can be placed right in the JSON as `{"a": "test", "__sql_type_a": "varchar(4)"}`.
* 🚀 **HTTP** - Bulker provides a simple HTTP server on top of Bulker Go intefarces. The server is stateless and can read configuration from Redis.
(if Redis is not available, it can use static configuration from yaml file)


## Supported databases

Bulker supports the following databases: Postgres, Redshit, Snowflake, BigQuery, Clickhouse, MySQL.

## Usage

### Go library

### HTTP Server

Http 

### Configuration

## How It Works

Bulker App relies on Kafka server for routing incoming messages, managing queues, batches 
