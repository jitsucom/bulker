# 🏍️ Sync Sidecar

Sync Sidecar is meant to be used as sidecar container to Airbyte protocol compatible Source connectors in Kubernetes Pod or Docker Compose.

Sync Sidecar captures data rows, logs, state and results of spec, discover and check command from Source connector:

- Data rows are sent to the target destination in Bulker instance,
- Logs are sent to preconfigured Bulkers destinations.
- `spec`, `discover` and `check` results goes to Postgres database tables.

## Named Pipes

Bulker-Sidecar uses named pipes to communicate with Source connector.
In k8s environment all containers from the same Pod are running on the same machine, and it is possible to use named pipes.
Source connectors entrypoint must be changed to direct their stderr and stdout output to named pipes.

Volume with named pipes should be mounted to both Bulker-Sidecar and Source connector containers.
InitContainer can be used to create named pipes in advance.

## Configuration

 - `STDOUT_PIPE_FILE` - path of named pipe for stdout of Source connector
 - `STDERR_PIPE_FILE` - path of named pipe for stderr of Source connector
 - `COMMAND` - Command that is used to run Source connector. Should be one of `spec`, `discover`, `check` or `read`
 - `STARTED_AT` - Timestamp when task was triggered
 - `BULKER_URL` - Bulker instance URL
 - `BULKER_AUTH_TOKEN` - Bulker instance auth token
 - `DATABASE_URL` - URL of Postgres database where spec, discover, check results and read task statuses should be stored
 - `PACKAGE` - Name of Source connector package
 - `PACKAGE_VERSION` - Version of Source connector package
 - `SOURCE_ID` - id of credentials entity. For `check` and `discover` commands.
 - `CONFIG_HASH` - Hash of Source connector credentials.  For `check` and `discover` commands.
 - `SYNC_ID` - id of sync entity (bulker destination id) where pulled events should be sent. For `read` command.
 - `TASK_ID` - id of current running task
 - `LOGS_CONNECTION_ID` - id of bulker internal destination where task logs should be sent
