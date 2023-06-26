#!/usr/bin/env bash

docker buildx build --platform linux/amd64 -f bulker.Dockerfile -t jitsucom/bulker:latest --push .