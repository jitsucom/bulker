#!/usr/bin/env bash

docker buildx build --platform linux/amd64 -f syncctl.Dockerfile -t jitsucom/syncctl:latest --push .