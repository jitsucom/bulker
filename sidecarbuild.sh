#!/usr/bin/env bash

docker buildx build --platform linux/arm64 -f sidecar.Dockerfile -t jitsucom/sidecar:dev2 --push .