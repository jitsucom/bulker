# Sync Controller

Sync Controller is a component that is responsible for running sync tasks in kubernetes cluster.
Sync Controller supports source connectors that use Airbyte protocol. All methods are supported: `spec`, `check`, `discover`, `read`.

Sync Controller run all tasks in separate **Pods**. It runs pod and monitor its status, updating task status information in database tables.
When task is finished, Sync Controller deletes Pod and frees resources.
K8s **Secret** used to pass source credentials to Source connector in Pod.

Each Pod contains of two main containers: Source connector and [Sync Sidecar](../sync-sidecar/README.md).

Sync Sidecar captures data rows, logs, state and results of spec, discover and check command from Source connector:

 - Data rows are sent to the target destination in Bulker instance,
 - Logs are sent to preconfigured Bulkers destinations.
 - `spec`, `discover` and `check` results goes to Postgres database tables.

## Authorization

All request should have `Authorization` header with value `Bearer ${token}`.

Token should be one of the tokens defined in `SYNCCTL_AUTH_TOKENS` env variable,
[see configuration section](#configuration).

If `SYNCCTL_AUTH_TOKENS` is not configured, then no authorization is required.

## Endpoints

### `GET /spec`

The endpoint is async. Run task to obtain Source connector specification. Resulting specification is stored in database table `source_spec`. 

Query parameters:

 - `package`* - source package image (for airbyte it's docker image)
 - `version`* - source package version
 - `connectorType` (optional) - `airbyte` by default 

Result table: `source_spec`. It writes a last JSON schema with update time by `[connectorType, package, version]`.

It returns either `{"status": "ok"}` or `{"error": ...}`

### `POST /check`

The endpoint is async. Run task to check Source connector credentials.
Check result is stored in database table `source_check`.

Query parameters:

 - `cacheKey` - id of source credentials entity
 - `package` - source package image
 - `version` - source package version
 
Request body:

```json
{
  "config": {
    // credentials json object
  }
}
```

Result table: `source_check`

### `POST /discover`

Run task to obtain Source connector streams catalog.
Catalog is stored in database table `source_catalog`.

Query parameters:

 - `cacheKey` - id of source credentials entity
 - `package` - source package image
 - `version` - source package version

Request body:

```json
{
  "config": {
    // credentials json object
  }
}
```

Result table: `source_catalog`

### `POST /read`

Run task to pull data from Source connector.

- Pulled data is sent to Bulker instance to `syncId` connection.
- Task logs are sent to Bulker instance to `SYNCCTL_BULKER_LOGS_CONNECTION_ID` connection.
- Task status is kept updated in `source_task` database table.
- Saved state of task if any is stored in `source_state` database table.

Query parameters:

 - `package` - source package image
 - `version` - source package version
 - `taskId` - id this task. Should be unique for each task
 - `syncId` - id of sync entity (bulker destination id) where pulled events should be sent

Request body:

```json
{
  "config": {
    // credentials json object
  },
  "catalog": {
    // catalog json object with selected streams and their configurations
  },
  "state": {
    // json object with state from previous task run
  }
}
```

## Configuration

| Environment variable                     | Required | Description                                                                                                                                                                                                                                                                                                                                                                | Default value           |
|------------------------------------------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------|
| `SYNCCTL_DATABASE_URL`                   | yes      | Postgres database URL. E.g.: `postgresql://user:pass@localhost:5432/postgres?sslmode=disable&search_path=bulker`                                                                                                                                                                                                                                                           |                         |
| `SYNCCTL_HTTP_PORT`                      | no       | HTTP port                                                                                                                                                                                                                                                                                                                                                                  | `3043`                  |
| `SYNCCTL_AUTH_TOKENS`                    | no       | A list of auth tokens that authorizes user in HTTP interface separated by comma.<br/>Each token can be either:<br/>`${token}` un-encrypted token value<br/>`${salt}.${hash}` hashed token.<br/><br/>`salt` should be random string<br/>`hash` = `base64(sha512(token + salt + SYNCCTL_TOKEN_SECRET))`<br/>`token` may consist only of letters, digits, underscore and dash |                         |
| `SYNCCTL_TOKEN_SECRET`                   | no       | See above. A secret that is used for hashing tokens.                                                                                                                                                                                                                                                                                                                       |                         |
| `SYNCCTL_SIDECAR_IMAGE`                  | yes      | Sync Sidecar docker image. E.g.: `jitsucom/sidecar:latest`                                                                                                                                                                                                                                                                                                                 |                         |
| `SYNCCTL_BULKER_URL`                     | yes      | Bulker instance URL. Passed to Sync Sidecar and must be reachable from k8s environment                                                                                                                                                                                                                                                                                     |                         |
| `SYNCCTL_BULKER_AUTH_TOKEN`              | yes      | Bulker instance auth token                                                                                                                                                                                                                                                                                                                                                 |                         |
| `SYNCCTL_BULKER_LOGS_CONNECTION_ID`      | yes      | id of bulker destination where task logs should be sent                                                                                                                                                                                                                                                                                                                    |                         |
| `SYNCCTL_KUBERNETES_CLIENT_CONFIG`       | yes      | kubernetes client config in yaml format                                                                                                                                                                                                                                                                                                                                    |                         |
| `SYNCCTL_KUBERNETES_NAMESPACE`           | no       | kubernetes namespace where tasks pod should be run                                                                                                                                                                                                                                                                                                                         | `default`               |
| `SYNCCTL_CONTAINER_STATUS_CHECK_SECONDS` | no       | frequency of running Pods status check                                                                                                                                                                                                                                                                                                                                     | `10`                    |
| `SYNCCTL_CONTAINER_INIT_TIMEOUT_SECONDS` | no       | timeout for Pod initialization                                                                                                                                                                                                                                                                                                                                             | `180`                   |
| `SYNCCTL_SIDECAR_DATABASE_URL`           | no       | Must lead to the same db instance as `SYNCCTL_DATABASE_URL`. Use it when in k8s environment database is reachable through a different hostname or IP.                                                                                                                                                                                                                      | =`SYNCCTL_DATABASE_URL` |
| `SYNCCTL_INSTANCE_ID`                    | no       | ID of syncctl instance. It is used for metrics. If is not set, instance id will be generated and persisted to disk (`~/.syncctl/instance_id`) and reused on next restart.                                                                                                                                                                                                  | random uuid             |


## Database schema

| Table name       | Description                                                                                          |
|------------------|------------------------------------------------------------------------------------------------------|
| `source_spec`    | Result of running `spec` command. Source connector specification that may be used to configure sync. |
| `source_check`   | Result of running `check` command. Source connector credentials check result.                        |
| `source_catalog` | Result of running `discover` command. Source connector streams catalog.                              |
| `source_task`    | Status of running `read` command.                                                                    |
| `source_state`   | State objects obtained while running `read` command.                                                 |
