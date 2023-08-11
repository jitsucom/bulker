#!/usr/bin/env bash

docker buildx build --platform linux/amd64 -f firebase.Dockerfile -t jitsucom/source-firebase:0.0.2 --push .