# Database Feature Matrix

## Glossary

* 🚿**Stream** — a mode when Bulker inserts data to a destination on per record basis. Usually,
databases don't like when a large amount of data is streamed. Don't use at production scale (more than 10-100 records per minute, 
depending on database).
* 🛢️**Batch** — a mode when Bulker inserts data to a destination in batches. Preferred mode for large amounts of data.
* 🔑**Primary Key** - a primary key is the column or columns that contain values that uniquely identify each row in a table. Enabled via stream options. Required for 'deduplication' option.
* 🐫**Deduplication** — a mode that avoid duplication of data rows with the equal values of key columns (primary key). It means that if Bulker receives
a record with the same primary key values, the old one will be replaced. Bulker maintains uniqueness of rows based on primary key columns even for warehouses that doesn't enforce uniqueness natively. Enabled via stream options. Require primary key option.
May comes with performance tradeoffs.


### Advanced features

Those features are not exposed as HTTP API and supported only on Go-lib API level.

* **Replace Table** - a special version of batch mode that assumes that a single batch contains all data for a table. Depending on database implementation bulker tries to atomically replace old table with a new one.
* **Replace Partition** - a special version of batch mode that replaces a part of target table. Part of table to replace is defined by 'partition' stream option. Each batch loads data for virtual partition identified by 'partition' option value. If table already contains data for provided 'partition', this data will be deleted and replaced with new data from current batch. Enabled via stream options.

|                        | Redshift                                   | BigQuery                                                    | ClickHouse                                                            | Snowflake                                   | Postgres                                   | MySQL                                   | S3 (coming soon) |     |
|------------------------|--------------------------------------------|-------------------------------------------------------------|-----------------------------------------------------------------------|---------------------------------------------|--------------------------------------------|-----------------------------------------|------------------|-----|
| Stream                 | ✅ [Supported](#redshift-stream)<br/>⚠️Slow | ❌ [Not supported](#bigquery-stream)                         | ✅ [Supported](#clickhouse-stream)                                     | ✅ [Supported](#snowflake-stream)            | ✅ [Supported](#postgres-stream)            | ✅ [Supported](#mysql-stream)            |                  |     |
| Batch                  | ✅ [Supported](#redshift-batch)             | ✅ [Supported](#bigquery-batch)                              | ✅ [Supported](#clickhouse-batch)                                      | ✅ [Supported](#snowflake-batch)             | ✅ [Supported](#postgres-batch)             | ✅ [Supported](#mysql-batch)             |                  |     |
| Deduplication          | ✅ [Supported](#redshift-deduplication)     | ✅ [Supported](#bigquery-deduplication)                      | ✅ [Supported](#clickhouse-deduplication)<br/>⚠️Eventual deduplication | ✅ [Supported](#snowflake-deduplication)     | ✅ [Supported](#postgres-deduplication)     | ✅ [Supported](#mysql-deduplication)     |                  |     |
| Primary key            | ✅ [Supported](#redshift-primary-key)       | ℹ️ [Emulated](#bigquery-primary-key)                        | ✅️ [Supported](#clickhouse-primary-key)                               | ✅️ [Supported](#snowflake-primary-key)      | ✅️ [Supported](#postgres-primary-key)      | ✅️ [Supported](#mysql-primary-key)      |                  |     |
| **Advanced features:** |                                            |                                                             |                                                                       |                                             |                                            |                                         |                  |     |
| Replace Table          | ✅ [Supported](#redshift-replace-table)     | ✅ [Supported](#bigquery-replace-table)                      | ✅ [Supported](#clickhouse-replace-table)                              | ✅ [Supported](#snowflake-replace-table)     | ✅ [Supported](#postgres-replace-table)     | ✅ [Supported](#mysql-replace-table)     |                  |     |
| Replace Partition      | ✅ [Supported](#redshift-replace-partition) | ✅ [Supported](#bigquery-replace-partition)<br/>⚠️Not atomic | ✅ [Supported](#clickhouse-replace-partition)                          | ✅ [Supported](#snowflake-replace-partition) | ✅ [Supported](#postgres-replace-partition) | ✅ [Supported](#mysql-replace-partition) |                  |     |



## Redshift

### Redshift Stream

✅Supported

⚠️Performance considerations

Supported as plain insert statements. Don't use at production scale (more than 10 records per minute)

### Redshift Batch

✅Supported

Algorithm:

- Write to tmp file
- Load tmp file to s3
- `BEGIN TRANSACTION`
- `COPY from s3 to tmp_table`
- `INSERT into target_table select from tmp_table`
- `COMMIT`

### Redshift Deduplication

✅Supported

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

✅Supported

In Redshift primary keys doesn’t enforce uniqueness.
Bulker performs deduplication itself when deduplication option is enabled and primary key is specified.

If primary key consists of a single column, that column will also be selected as the `DIST KEY`.

### Redshift Replace Table

✅Supported

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

✅Supported

Algorithm:
- Write to tmp file
- Load tmp file to s3
- `BEGIN TRANSACTION`
- `DELETE from target_table where partition_id=partiton option value`
- `COPY from s3 to target_table`
- `COMMIT`

## BigQuery

### BigQuery Stream

❌ Not supported, though it's possible to implement.

### BigQuery Batch

✅Supported

- Write to tmp file
- Use Loader API to load to tmp_table from tmp file
- Use Copier API to copy from tmp_table to target_table

### BigQuery Deduplication

✅Supported

Algorithm for batch mode:
- Write to tmp file
- Dedup tmp file
- Use Loader API to load to tmp_table from tmp file
- `MERGE into target_table on tmp_table when matched then UPDATE when not matched them INSERT`

### BigQuery Primary Key

Emulated - bulker fully handles uniqueness.
Primary keys columns meta information stored in table labels.

### BigQuery Replace Table

✅Supported

Algorithm:
- Write to tmp file
- Use Loader API to load to tmp_table from tmp file
- Use Copier API to copy from tmp_table to target_table with WriteTruncate mode
- Drop tmp_table

### BigQuery Replace Partition

✅Supported
⚠️Not atomic – during completion of bulker stream it is possible that target table will be missing some data for specified 'partiton' for a short period of time.

Algorithm:
- `DELETE from target_table where partition_id=` partition option value
- Write to tmp file
- Use Loader API to load to target_table from tmp file

## ClickHouse

### ClickHouse Stream

✅Supported

For single node instance:

`INSERT INTO target_table (...) VALUES (..)`

For cluster bulker insert into distributed table so data evenly distributed across cluster nodes:

`INSERT INTO dist_target_table (...) VALUES (...)`

### ClickHouse Batch

✅Supported

Algorithm:
- Write to tmp file
- `INSERT INTO tmp_table (...) VALUES (...)` - bulk load data from tmp file into tmp_table using prepared statement in transaction
- `INSERT INTO target_table(...) SELECT ... FROM tmp_table`

### ClickHouse Deduplication

✅Supported 

Bulker clickhouse implementation relies on clickhouse [ReplacingMergeTree](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replacingmergetree/)
engine to perform deduplication.
Primary key columns are used as primary key as well as sorting keys (`ORDER BY`) for ReplacingMergeTree engine.

⚠️Eventual deduplication

ReplacingMergeTree engine performs deduplication in background during some time after insertion
So it's still possible to get rows with duplicated primary key columns using ordinary `SELECT`.

To make sure that no duplicates are present in query results use [FINAL](https://clickhouse.com/docs/en/sql-reference/statements/select/from/#final-modifier) modifier, e.g:

`SELECT * FROM target_table FINAL`.

### ClickHouse Primary Key

✅Supported

Primary keys columns also used as sorting key for ReplacingMergeTree engine.

### ClickHouse Replace Table

✅Supported

Algorithm:
- Write to tmp file
- `INSERT INTO tmp_table (...) VALUES (...)` - bulk load data from tmp file into tmp_table using prepared statement in transaction
- `EXCHANGE TABLES target_table tmp_table`

### ClickHouse Replace Partition

✅Supported

Algorithm:
- Write to tmp file
- `INSERT INTO tmp_table(...) VALUES (...)` - bulk load data from tmp file into tmp_table using prepared statement in transaction
- `INSERT INTO target_table(...) SELECT ... FROM tmp_table`


## Snowflake

### Snowflake Stream

✅Supported

`INSERT INTO target_table (...) VALUES (..)`

### Snowflake Batch

✅Supported

Algorithm:

- Write to tmp file
- Load tmp file to `stage`
- `BEGIN TRANSACTION`
- `COPY from stage to tmp_table`
- `INSERT into target_table select from tmp_table`
- `COMMIT`

### Snowflake Deduplication

✅Supported

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

✅Supported

In Snowflake primary keys doesn’t enforce uniqueness.
Bulker performs deduplication itself when deduplication option is enabled and primary key is specified.

### Snowflake Replace Table

✅Supported

Algorithm:
- Write to tmp file
- Load tmp file to `stage`
- `BEGIN TRANSACTION`
- `COPY from stage to tmp_table`
- `RENAME target_table to deprecated_target_table_20060101_150405`
- `RENAME tmp_table to target_table`
- `DROP TABLE deprecated_target_table_20060101_150405`
- `COMMIT`

### Snowflake Replace Partition

✅Supported

Algorithm:
- Write to tmp file
- Load tmp file to `stage`
- `BEGIN TRANSACTION`
- `DELETE from target_table where partition_id=partiton option value`
- `COPY from stage to target_table`
- `COMMIT`

## Postgres

### Postgres Stream

✅Supported

`INSERT INTO target_table (...) VALUES (..)`

### Postgres Batch

✅Supported

Algorithm:

- Write to tmp file
- `BEGIN TRANSACTION`
- `COPY from STDIN to tmp_table` - load tmp file into tmp_table
- `INSERT into target_table select from tmp_table`
- `COMMIT`

### Postgres Deduplication

✅Supported

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

✅Supported

### Postgres Replace Table

✅Supported

Algorithm:
- Write to tmp file
- `BEGIN TRANSACTION`
- `COPY from STDIN to tmp_table` - load tmp file into tmp_table
- `RENAME target_table to deprecated_target_table_20060101_150405`
- `RENAME tmp_table to target_table`
- `DROP TABLE deprecated_target_table_20060101_150405`
- `COMMIT`

### Postgres Replace Partition

✅Supported

Algorithm:
- Write to tmp file
- `BEGIN TRANSACTION`
- `DELETE from target_table where partition_id=partiton option value`
- `COPY from STDIN to target_table` - load tmp file into tmp_table
- `COMMIT`

## MySQL

### MySQL Stream

✅Supported

`INSERT INTO target_table (...) VALUES (..)`

### MySQL Batch

✅Supported

Algorithm:

- `BEGIN TRANSACTION`
- `INSERT into tmp_table`
- `INSERT into target_table select from tmp_table`
- `COMMIT`

### MySQL Deduplication

✅Supported

For batch mode the following algorithm is used:

- `BEGIN TRANSACTION`
- `INSERT into tmp_table ... ON DUPLICATE KEY UPDATE ...`
- `INSERT into target_table select from tmp_table ... ON DUPLICATE KEY UPDATE ...`
- `COMMIT`

For stream mode:

`INSERT INTO target_table ... ON DUPLICATE KEY UPDATE ...`

### MySQL Primary Key

✅Supported


### MySQL Replace Table

✅Supported

Algorithm:
- `BEGIN TRANSACTION`
- `INSERT into tmp_table`
- `RENAME target_table to deprecated_target_table_20060101_150405`
- `RENAME tmp_table to target_table`
- `DROP TABLE deprecated_target_table_20060101_150405`
- `COMMIT`

### MySQL Replace Partition

✅Supported

Algorithm:
- `BEGIN TRANSACTION`
- `DELETE from target_table where partition_id=partiton option value`
- `INSERT into target_table ... ON DUPLICATE KEY UPDATE ...`
- `COMMIT`