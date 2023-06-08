#!/usr/bin/env bash

docker buildx build --platform linux/arm64 -t jitsucom/sidecar:latest --load .