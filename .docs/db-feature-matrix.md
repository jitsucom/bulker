# Database Feature Matrix

## Glossary

* **Stream** — a mode when bulker inserts data to a destination on per record basis. Usually,
databases don't like when a large amount of data is streamed
* **Batch** — a mode when bulker inserts data to a destination in batches. Usually, databases
* **Primary Keys **
* **Dedup**

### Advanced feature

Those features are not exposed as HTTP API and supported only on Go-lib API level

* **Replace Table**
* **Replace Partition**




<table>
    <thead>
        <tr>
            <th></th>
            <th>Redshift</th>
            <th>BigQuery</th>
            <th>Clickhouse</th>
            <th>Snowflake</th>
            <th>Postgres</th>
            <th>MySQL</th>  
            <th>S3 <i>(coming soon)</i></th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><b>Stream</b></td>
            <td>
                <!--Redshift-->
                <a href="#stream-redshift">✅Supported</a>
            </td>
            <td>
                <!--BigQuery-->
                <a href="#stream-bigquery">❌Not supported</a>, but possible
            </td>
            <td>
                <!--Clickhouse-->
                <a href="#stream-clickhouse">✅Supported</a>
            </td>
            <td>
                <!--Snowflake-->
                <a href="#stream-snowflake">✅Supported</a>
            </td>
            <td>
                <!--Postgres-->
                <a href="#stream-postgres">✅Supported</a>
            </td>
            <td>
                <!--MySQL-->
                <a href="#stream-mysql">✅Supported</a>
            </td>
            <td>
                <!--S3-->
            </td>
        </tr>
        <tr>
            <td><b>Stream with dedup</b></td>
            <td>
                <!--Redshift-->
            </td>
            <td>
                <!--BigQuery-->
            </td>
            <td>
                <!--Clickhouse-->
            </td>
            <td>
                <!--Snowflake-->
            </td>
            <td>
                <!--Postgres-->
            </td>
            <td>
                <!--MySQL-->
            </td>
            <td>
                <!--S3-->
            </td>
        </tr>
        <tr>
            <td><b>Batch</b></td>
            <td>
                <!--Redshift-->
            </td>
            <td>
                <!--BigQuery-->
            </td>
            <td>
                <!--Clickhouse-->
            </td>
            <td>
                <!--Snowflake-->
            </td>
            <td>
                <!--Postgres-->
            </td>
            <td>
                <!--MySQL-->
            </td>
            <td>
                <!--S3-->
            </td>
        </tr>
        <tr>
            <td><b>Batch with dedup</b></td>
            <td>
                <!--Redshift-->
            </td>
            <td>
                <!--BigQuery-->
            </td>
            <td>
                <!--Clickhouse-->
            </td>
            <td>
                <!--Snowflake-->
            </td>
            <td>
                <!--Postgres-->
            </td>
            <td>
                <!--MySQL-->
            </td>
            <td>
                <!--S3-->
            </td>
        </tr>
        <tr>
            <td><b>Primary Keys</b></td>
            <td>
                <!--Redshift-->
            </td>
            <td>
                <!--BigQuery-->
            </td>
            <td>
                <!--Clickhouse-->
            </td>
            <td>
                <!--Snowflake-->
            </td>
            <td>
                <!--Postgres-->
            </td>
            <td>
                <!--MySQL-->
            </td>
            <td>
                <!--S3-->
            </td>
        </tr>        
        <tr>
            <td><b>Replace Table</b></td>
            <td>
                <!--Redshift-->
            </td>
            <td>
                <!--BigQuery-->
            </td>
            <td>
                <!--Clickhouse-->
            </td>
            <td>
                <!--Snowflake-->
            </td>
            <td>
                <!--Postgres-->
            </td>
            <td>
                <!--MySQL-->
            </td>
            <td>
                <!--S3-->
            </td>
        </tr>
        <tr>
            <td><b>Replace Table + Dedup</b></td>
            <td>
                <!--Redshift-->
            </td>
            <td>
                <!--BigQuery-->
            </td>
            <td>
                <!--Clickhouse-->
            </td>
            <td>
                <!--Snowflake-->
            </td>
            <td>
                <!--Postgres-->
            </td>
            <td>
                <!--MySQL-->
            </td>
            <td>
                <!--S3-->
            </td>
        </tr>
    </tbody>
</table>

##Stream

### Stream: Redshift

Supported as plain insert statements. Don't use at production scale (more than 10 records per minute)

### Stream: BigQuery

Not supported, though it's possible to implement.

### Stream: Clickhouse

TODO

### Stream: Snowflake

Supported as plain insert statements

### Stream: Postgres

Supported as plain insert statements

### Stream: MySQL

Supported as plain insert statements

### Stream: S3

Coming soon

## Stream with dedup: Redshift