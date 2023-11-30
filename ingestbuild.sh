#!/usr/bin/env bash

VERSION=$(git log -1 --pretty=%h)
TAG="$REPO$VERSION"
LATEST="${REPO}latest"
BUILD_TIMESTAMP=$( date '+%F_%H:%M:%S' )

docker buildx build --build-arg VERSION="$VERSION" --build-arg BUILD_TIMESTAMP="$BUILD_TIMESTAMP" --platform linux/amd64 -f ingest.Dockerfile -t jitsucom/ingest:"$VERSION" -t jitsucom/ingest:latest --push .