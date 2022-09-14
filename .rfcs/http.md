# Bulker HTTP Interface

And HTTP interface on top of Bulker Go intefarce with [persistent configuration](https://github.com/jitsucom/bulker/blob/main/bulker/bulker.go) storage



## Configuration

The service should be started with a single `./bulker` command and all configuration should be done with envinroment variables. List of available config options:

#### `BULKER_HTTP_PORT`

(or just `PORT` for comapatibility); default value `3042`

#### `BULKER_CONFIG`

Points bulker server to a configuration source. So far it should recognize only URL `redis://` (or `rediss://` for secure connection). The string should be a redis Connection URL

#### `BULKER_REDIS_CONFIG_KEY`

Default value `bulker_connections`

A key where Bulker should read configuration. The key should contain redis hashset where the key is a unique id of destination (see below).

#### `AUTH_TOKENS` (and `BULKER_TOKEN_SECRET`)

Comma-separated a list of auth tokens. Token can have either of those formats:

* `${token}` un-encripted token value (toke can't contain `.` or white space)
* `${salt}.${base64(sha512(token + salt + (process.env.BULKER_TOKEN_SECRET || ''))}` hashed token. `${salt}` should be random string
  * Example of hashing: `21a2ae36-32994870a9fbf2f61ea6f6c8`→ `bt6ghq4tpqr.WMMKlCNvcwpCkHFwFDLDaTGTuBT37yTioDFsMXRAXrY` (without `BULKER_TOKEN_SECRET` )
  * `BULKER_TOKEN_SECRET` can be a comma-separated list of secret. In this case hash should be checked against each secret.



## HTTP API

### Authorization

All requests should contain `Application: Bearer <token>` where token is unencrypted token

#### `POST /load/{destinationId}`

GET parameters should mirror [options](https://github.com/jitsucom/bulker/blob/main/implementations/sql/options.go) - `WithPartition` →  `partition`, `MergeRows` → `mergeRows`, etc

Body should be either one JSON object (for autocommit mode), or a stream of JSON objects for batch mode.


## Config storage

Bulker should make best effort to be able to work without responsive configuration storage. For Redis:

* During start it should read all destinations config and keep them in RAM
* It should periodically reload configs from Redis
* It should subsribe to changes in Redis and 









