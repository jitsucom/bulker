# Prerequisites

- `go: 1.21`
- `docker: >= 19.03.0`
- `node: 18.x`
- `npx`

# Releasing Docker packages

We use [build-scripts](https://github.com/jitsucom/jitsu/tree/newjitsu/cli/build-scripts) along with `all.Dockerfile` to publish releases to Docker.

## Packages

- `jitsucom/bulker` (./bulkerapp) - Bulker is a tool for streaming and batching large amount of semi-structured data into data warehouses
- `jitsucom/ingest` (./ingest) - Ingestion API for Jitsu
- `jitsucom/syncctl` (./sync-controller) - Controller for Jitsu Connectors
- `jitsucom/sidecar` (./sync-sidecar) - Sidecar component that runs in the same pod as connector and responsible for running syncs

To avoid confusion, always release all packages together, even if only one of them has changes.

## Common steps

- Make sure that you are logged to your docker account `docker login`
- All changes should be committed (check with `git status`). It's ok to release canary from branches!

## Beta releases

- `./release.sh --dryRun` - to **dry-run** publishing.
- `./release.sh` - to actually **publish** beta.

## Stable releases

- `./release.sh --release latest --dryRun` - to **dry-run** publishing.
- `./release.sh --release latest ` - to actually **publish** latest image.

## Bumping versions

For initial release or to bump major/minor version pass `--version` argument to `./release.sh` script.

- `./release.sh --version 2.5.0`
- `./release.sh --release latest --version 2.5.0`



  
