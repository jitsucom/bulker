# Database Feature Matrix

## Features

* 🚿**Stream** — a mode when Bulker inserts data to a destination on per record basis. Usually,
databases don't like when a large amount of data is streamed. Don't use at production scale (more than 10-100 records per minute, 
depending on database).
* 🛢️**Batch** — a mode when Bulker inserts data to a destination in batches. Preferred mode for large amounts of data.
* 🔑**Primary Key** - a primary key is the column or columns that contain values that uniquely identify each row in a table. Enabled via stream options. Required for 'deduplication' option.
* 🐫**Deduplication** — a mode that avoid duplication of data rows with the equal values of key columns (primary key). It means that if Bulker receives
a record with the same primary key values, the old one will be replaced. Bulker maintains uniqueness of rows based on primary key columns even for warehouses that doesn't enforce uniqueness natively. Enabled via stream options. Require primary key option.
May comes with performance tradeoffs.
* 🗓️**Timestamp Column** - timestamp column option helps Bulker to create tables optimized for range queries and sorting by time, e.g. event creation time.


|                  | &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Redshift&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;BigQuery&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;ClickHouse&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | &nbsp;&nbsp;&nbsp;Snowflake&nbsp;&nbsp;&nbsp;   | &nbsp;&nbsp;&nbsp;&nbsp;Postgres&nbsp;&nbsp;&nbsp;&nbsp; | &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;MySQL&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | S3 (coming soon) |
|------------------|----------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------|-------------------------------------------------|----------------------------------------------------------|-------------------------------------------------------------------------------|------------------|
| Stream           | ✅&nbsp;[Supported](#redshift-stream)<br/>⚠️&nbsp;Slow                | ❌&nbsp;[Not supported](#bigquery-stream)                                                                 | ✅&nbsp;[Supported](#clickhouse-stream)                                                         | ✅&nbsp;[Supported](#snowflake-stream)           | ✅&nbsp;[Supported](#postgres-stream)                     | ✅&nbsp;[Supported](#mysql-stream)                                             |                  |
| Batch            | ✅&nbsp;[Supported](#redshift-batch)                                  | ✅&nbsp;[Supported](#bigquery-batch)                                                                      | ✅&nbsp;[Supported](#clickhouse-batch)                                                          | ✅&nbsp;[Supported](#snowflake-batch)            | ✅&nbsp;[Supported](#postgres-batch)                      | ✅&nbsp;[Supported](#mysql-batch)                                              |                  |
| Primary key      | ✅&nbsp;[Supported](#redshift-primary-key)                            | ℹ️&nbsp;[Emulated](#bigquery-primary-key)                                                                | ✅️&nbsp;[Supported](#clickhouse-primary-key)                                                   | ✅️ [Supported](#snowflake-primary-key)          | ✅️&nbsp;[Supported](#postgres-primary-key)               | ✅️&nbsp;[Supported](#mysql-primary-key)                                       |                  |
| Deduplication    | ✅&nbsp;[Supported](#redshift-deduplication)                          | ✅&nbsp;[Supported](#bigquery-deduplication)                                                              | ✅&nbsp;[Supported](#clickhouse-deduplication)<br/>⚠️&nbsp;Eventual&nbsp;dedup                  | ✅&nbsp;[Supported](#snowflake-deduplication)    | ✅&nbsp;[Supported](#postgres-deduplication)              | ✅&nbsp;[Supported](#mysql-deduplication)                                      |                  |
| Timestamp Column | ✅&nbsp;[Supported](#redshift-timestamp-column)                       | ✅&nbsp;[Supported](#bigquery-timestamp-column)                                                           | ✅&nbsp;[Supported](#clickhouse-timestamp-column)                                               | ✅&nbsp;[Supported](#snowflake-timestamp-column) | ✅&nbsp;[Supported](#postgres-timestamp-column)           | ✅&nbsp;[Supported](#mysql-timestamp-column)                                   |                  |

## Advanced features

Those features are not exposed as HTTP API and supported only on Go-lib API level.

* **Replace Table** - a special version of batch mode that assumes that a single batch contains all data for a table. Depending on database implementation bulker tries to atomically replace old table with a new one.
* **Replace Partition** - a special version of batch mode that replaces a part of target table. Part of table to replace is defined by 'partition' stream option. Each batch loads data for virtual partition identified by 'partition' option value. If table already contains data for provided 'partition', this data will be deleted and replaced with new data from current batch. Enabled via stream options.


|                        | &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Redshift&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;BigQuery&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;ClickHouse&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | &nbsp;&nbsp;&nbsp;Snowflake&nbsp;&nbsp;&nbsp;    | &nbsp;&nbsp;&nbsp;&nbsp;Postgres&nbsp;&nbsp;&nbsp;&nbsp; | &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;MySQL&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | S3 (coming soon) |
|------------------------|----------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------|--------------------------------------------------|----------------------------------------------------------|-------------------------------------------------------------------------------|------------------|
| Replace&nbsp;Table     | ✅&nbsp;[Supported](#redshift-replace-table)                          | ✅&nbsp;[Supported](#bigquery-replace-table)                                                              | ✅&nbsp;[Supported](#clickhouse-replace-table)                                                  | ✅&nbsp;[Supported](#snowflake-replace-table)     | ✅&nbsp;[Supported](#postgres-replace-table)              | ✅&nbsp;[Supported](#mysql-replace-table)                                      |                  |
| Replace&nbsp;Partition | ✅&nbsp;[Supported](#redshift-replace-partition)                      | ✅&nbsp;[Supported](#bigquery-replace-partition)<br/>⚠️&nbsp;Not atomic                                   | ✅&nbsp;[Supported](#clickhouse-replace-partition)                                              | ✅&nbsp;[Supported](#snowflake-replace-partition) | ✅&nbsp;[Supported](#postgres-replace-partition)          | ✅&nbsp;[Supported](#mysql-replace-partition)                                  |                  |



## Redshift

### Redshift Stream

> ✅ Supported

> ⚠️Performance considerations

Supported as plain insert statements. Don't use at production scale (more than 10 records per minute)

### Redshift Batch

> ✅ Supported

Algorithm:

- Write to tmp file
- Load tmp file to s3
- `BEGIN TRANSACTION`
- `COPY from s3 to tmp_table`
- `INSERT into target_table select from tmp_table`
- `COMMIT`

### Redshift Deduplication

> ✅ Supported

For batch mode the following algorithm is used:

- Write to tmp file
- Deduplicate rows in tmp file
- Load tmp file to s3
- `BEGIN TRANSACTION`
- `COPY from s3 to tmp_table`
- `DELETE from target_table using tmp_table` where primary key matches
- `INSERT into target_table select from tmp_table`
- `COMMIT`

For stream mode:

`SELECT` by primary key. Then either `INSERT` or `UPDATE` depending on result. Don't use at production scale (more than 10 records per minute)

### Redshift Primary Key

> ✅ Supported

In Redshift primary keys doesn’t enforce uniqueness.
Bulker performs deduplication itself when deduplication option is enabled and primary key is specified.

If primary key consists of a single column, that column will also be selected as the `DIST KEY`.

### Redshift Timestamp Column

> ✅ Supported

Selected timestamp column will be used as [sort key](https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html) for target table.

### Redshift Replace Table

> ✅ Supported

Algorithm:
- Write to tmp file
- Load tmp file to s3
- `BEGIN TRANSACTION`
- `COPY from s3 to tmp_table`
- `RENAME target_table to deprecated_target_table_20060101_150405`
- `RENAME tmp_table to target_table`
- `DROP TABLE deprecated_target_table_20060101_150405`
- `COMMIT`

### Redshift Replace Partition

> ✅ Supported

Algorithm:
- Write to tmp file
- Load tmp file to s3
- `BEGIN TRANSACTION`
- `DELETE from target_table where partition_id=partiton option value`
- `COPY from s3 to target_table`
- `COMMIT`

## BigQuery

### BigQuery Stream

> ❌ Not supported 

It's possible to implement, but without deduplication, so we decided not to. 

### BigQuery Batch

> ✅ Supported

- Write to tmp file
- Use Loader API to load to tmp_table from tmp file
- Use Copier API to copy from tmp_table to target_table

### BigQuery Deduplication

> ✅ Supported

Algorithm for batch mode:
- Write to tmp file
- Dedup tmp file
- Use Loader API to load to tmp_table from tmp file
- `MERGE into target_table on tmp_table when matched then UPDATE when not matched them INSERT`

### BigQuery Primary Key

Emulated - bulker fully handles uniqueness.
Primary keys columns meta information stored in table labels.

### BigQuery Timestamp Column

> ✅ Supported

Bulker creates [time-unit column-partitioned](https://cloud.google.com/bigquery/docs/partitioned-tables#date_timestamp_partitioned_tables) table with specified timestamp column and monthly partitioning.

### BigQuery Replace Table

> ✅ Supported

Algorithm:
- Write to tmp file
- Use Loader API to load to tmp_table from tmp file
- Use Copier API to copy from tmp_table to target_table with WriteTruncate mode
- Drop tmp_table

### BigQuery Replace Partition

> ✅ Supported
> 
> ⚠️ Not atomic – during completion of bulker stream it is possible that target table will be missing some data for specified 'partiton' for a short period of time.

Algorithm:
- `DELETE from target_table where partition_id=` partition option value
- Write to tmp file
- Use Loader API to load to target_table from tmp file

## ClickHouse

### ClickHouse Stream

> ✅ Supported

For single node instance:

`INSERT INTO target_table (...) VALUES (..)`

For cluster bulker insert into distributed table so data evenly distributed across cluster nodes:

`INSERT INTO dist_target_table (...) VALUES (...)`

### ClickHouse Batch

> ✅ Supported

Algorithm:
- Write to tmp file
- `INSERT INTO tmp_table (...) VALUES (...)` - bulk load data from tmp file into tmp_table using bulk insert
- `INSERT INTO target_table(...) SELECT ... FROM tmp_table`

### ClickHouse Deduplication

> ✅ Supported
> ⚠️ Eventual deduplication

Bulker relies on underlying table engine. Unless table created prior to Bulker,
[ReplacingMergeTree](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replacingmergetree/) will be used.

Primary key column configured for destination will be used both as `PRIMARY KEY` and `ORDER BY`.

ReplacingMergeTree engine performs deduplication in background during some time after insertion
So it's still possible to get rows with duplicated primary key columns using ordinary `SELECT`.

To make sure that no duplicates are present in query results use [FINAL](https://clickhouse.com/docs/en/sql-reference/statements/select/from/#final-modifier) modifier, e.g:

`SELECT * FROM target_table FINAL`.

> Note**
> `ReplacingMergeTree` is not only way to deduplicate data in ClickHouse. There other [approaches too](https://kb.altinity.com/altinity-kb-schema-design/row-level-deduplication/).
> To implement them, create destination table before Bulker starts inserting the data. In this case Bulker will respect table engine and primary key columns you specified.

### ClickHouse Primary Key

> ✅ Supported

Primary keys columns also used as sorting key for ReplacingMergeTree engine.

### ClickHouse Timestamp Column

> ✅ Supported

Bulker creates tables [partitioned](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/custom-partitioning-key/) by specified timestamp column and monthly partitioning, e.g. `PARTITION BY toYYYYMM(_timestamp)`

### ClickHouse Replace Table

> ✅ Supported

Algorithm:
- Write to tmp file
- `INSERT INTO tmp_table (...) VALUES (...)` - bulk load data from tmp file into tmp_table using bulk insert
- `EXCHANGE TABLES target_table tmp_table`

### ClickHouse Replace Partition

> ✅ Supported

Algorithm:
- Write to tmp file
- `INSERT INTO tmp_table(...) VALUES (...)` - bulk load data from tmp file into tmp_table using bulk insert
- `INSERT INTO target_table(...) SELECT ... FROM tmp_table`


## Snowflake

### Snowflake Stream

> ✅ Supported

`INSERT INTO target_table (...) VALUES (..)`

### Snowflake Batch

> ✅ Supported

Algorithm:

- Write to tmp file
- Load tmp file to `stage`
- `BEGIN TRANSACTION`
- `COPY from stage to tmp_table`
- `INSERT into target_table select from tmp_table`
- `COMMIT`

### Snowflake Deduplication

> ✅ Supported

For batch mode the following algorithm is used:

- Write to tmp file
- Deduplicate rows in tmp file
- Load tmp file to s3
- `BEGIN TRANSACTION`
- `COPY from stage to tmp_table`
- `MERGE into target_table using (select from tmp_table) ...`
- `COMMIT`

For stream mode:

`SELECT` by primary key. Then either `INSERT` or `UPDATE` depending on result.

### Snowflake Primary Key

> ✅ Supported

In Snowflake primary keys doesn’t enforce uniqueness.
Bulker performs deduplication itself when deduplication option is enabled and primary key is specified.

### Snowflake Timestamp Column

> ✅ Supported

Bulker sets [clustering key](https://docs.snowflake.com/en/user-guide/tables-clustering-keys.html#what-is-a-clustering-key) to the month part of specified timestamp column values, e.g. `CLUSTER BY (DATE_TRUNC('MONTH', _timestamp))`

### Snowflake Replace Table

> ✅ Supported

Algorithm:
- Write to tmp file
- Load tmp file to `stage`
- `BEGIN TRANSACTION`
- `COPY from stage to tmp_table`
- `CREATE OR REPLACE TABLE target_table CLONE tmp_table`
- `DROP TABLE tmp_table`
- `COMMIT`

### Snowflake Replace Partition

> ✅ Supported

Algorithm:
- Write to tmp file
- Load tmp file to `stage`
- `BEGIN TRANSACTION`
- `DELETE from target_table where partition_id=partiton option value`
- `COPY from stage to target_table`
- `COMMIT`

## Postgres

### Postgres Stream

> ✅ Supported

`INSERT INTO target_table (...) VALUES (..)`

### Postgres Batch

> ✅ Supported

Algorithm:

- Write to tmp file
- `BEGIN TRANSACTION`
- `COPY from STDIN to tmp_table` - load tmp file into tmp_table
- `INSERT into target_table select from tmp_table`
- `COMMIT`

### Postgres Deduplication

> ✅ Supported

For batch mode the following algorithm is used:

- Write to tmp file
- Deduplicate rows in tmp file
- `BEGIN TRANSACTION`
- `COPY from STDIN to tmp_table` - load tmp file into tmp_table
- `INSERT into target_table select from tmp_table ON CONFLICT UPDATE ...`
- `COMMIT`

For stream mode:

`INSERT INTO target_table (...) VALUES (..) ON CONFLICT UPDATE ...`

### Postgres Primary Key

> ✅ Supported

### Postgres Timestamp Column

> ✅ Supported

Regular index is created on specified timestamp column.

### Postgres Replace Table

> ✅ Supported

Algorithm:
- Write to tmp file
- `BEGIN TRANSACTION`
- `COPY from STDIN to tmp_table` - load tmp file into tmp_table
- `RENAME target_table to deprecated_target_table_20060101_150405`
- `RENAME tmp_table to target_table`
- `DROP TABLE deprecated_target_table_20060101_150405`
- `COMMIT`

### Postgres Replace Partition

> ✅ Supported

Algorithm:
- Write to tmp file
- `BEGIN TRANSACTION`
- `DELETE from target_table where partition_id=partiton option value`
- `COPY from STDIN to target_table` - load tmp file into tmp_table
- `COMMIT`

## MySQL

### MySQL Stream

> ✅ Supported

`INSERT INTO target_table (...) VALUES (..)`

### MySQL Batch

> ✅ Supported

Algorithm:

- `BEGIN TRANSACTION`
- `INSERT into tmp_table`
- `INSERT into target_table select from tmp_table`
- `COMMIT`

### MySQL Deduplication

> ✅ Supported

For batch mode the following algorithm is used:

- `BEGIN TRANSACTION`
- `INSERT into tmp_table ... ON DUPLICATE KEY UPDATE ...`
- `INSERT into target_table select from tmp_table ... ON DUPLICATE KEY UPDATE ...`
- `COMMIT`

For stream mode:

`INSERT INTO target_table ... ON DUPLICATE KEY UPDATE ...`

### MySQL Primary Key

> ✅ Supported

### MySQL Timestamp Column

> ✅ Supported

Regular index is created on specified timestamp column.

### MySQL Replace Table

> ✅ Supported

Algorithm:
- `BEGIN TRANSACTION`
- `INSERT into tmp_table`
- `RENAME target_table to deprecated_target_table_20060101_150405`
- `RENAME tmp_table to target_table`
- `DROP TABLE deprecated_target_table_20060101_150405`
- `COMMIT`

### MySQL Replace Partition

> ✅ Supported

Algorithm:
- `BEGIN TRANSACTION`
- `DELETE from target_table where partition_id=partiton option value`
- `INSERT into target_table ... ON DUPLICATE KEY UPDATE ...`
- `COMMIT`
