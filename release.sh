#!/usr/bin/env bash

npx jitsu-build-scripts docker -t bulker,ingest,syncctl,sidecar,ingmgr,cfgkpr --platform linux/amd64,linux/arm64 --push $@

