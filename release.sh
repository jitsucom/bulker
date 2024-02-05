#!/usr/bin/env bash

npx jitsu-build-scripts docker -t bulker,ingest,syncctl,sidecar,ingmgr --push $@

