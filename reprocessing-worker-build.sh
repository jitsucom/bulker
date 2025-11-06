#!/usr/bin/env bash

VERSION=$(git log -1 --pretty=%h)
BUILD_TIMESTAMP=$( date '+%F_%H:%M:%S' )

docker buildx build --build-arg VERSION="$VERSION" --build-arg BUILD_TIMESTAMP="$BUILD_TIMESTAMP" --platform linux/amd64,linux/arm64 -f reprocessing-worker.Dockerfile -t jitsucom/reprocessing-worker:"$VERSION" -t jitsucom/reprocessing-worker:latest --push .
