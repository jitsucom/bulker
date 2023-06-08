# 🏍️ Bulker-Sidecar

Bulker-Sidecar is meant to be used as sidecar container to Source connectors (Airbyte or Jitsu) in Kubernetes Pod or Docker Compose.
Bulker-Sidecar is responsible for collecting pulled events, logs and states from Source connector and sending them to Bulker instance.


## Named Pipes

Bulker-Sidecar uses named pipes to communicate with Source connector.
Even in k8s environment all containers from the same Pod are running on the same machine, and it is possible to use named pipes.
Source connectors entrypoint should be changed to direct their stderr and stdout output to named pipes.

Volume with named pipes should be mounted to both Bulker-Sidecar and Source connector containers.
InitContainer can be used to create named pipes in advance.

## Configuration

 - `STDOUT_PIPE_FILE` - path of named pipe for stdout
 - `STDERR_PIPE_FILE` - path of named pipe for stderr
 - `BULKER_URL` - Bulker instance URL
 - `BULKER_AUTH_TOKEN` - Bulker instance auth token
 - `SOURCE_ID` - id of Source entity that current running task belongs to
 - `TASK_ID` - id of current running task
 - `CONNECTION_ID` - id of bulker destination(connection) where pulled events should be sent
 - `TASKS_CONNECTION_ID` - id of bulker internal destination where task logs and status should be sent
 - `STATE_CONNECTION_ID` - id of bulker internal destination where task state should be sent

## Example

See [github_example_pod](./k8s/github_example_pod.yaml) file for example of using Bulker-Sidecar with Airbyte Github Source
in kubernetes Pod.