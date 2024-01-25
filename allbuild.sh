#!/usr/bin/env bash

VERSION=$(git log -1 --pretty=%h)
BUILD_TIMESTAMP=$( date '+%F_%H:%M:%S' )

docker buildx build --build-arg VERSION="$VERSION" --build-arg BUILD_TIMESTAMP="$BUILD_TIMESTAMP" --platform linux/amd64 -f all.Dockerfile --target bulker -t jitsucom/bulker:"$VERSION" -t jitsucom/bulker:beta --push .
docker buildx build --build-arg VERSION="$VERSION" --build-arg BUILD_TIMESTAMP="$BUILD_TIMESTAMP" --platform linux/amd64 -f all.Dockerfile --target ingest -t jitsucom/ingest:"$VERSION" -t jitsucom/ingest:beta --push .
docker buildx build --build-arg VERSION="$VERSION" --build-arg BUILD_TIMESTAMP="$BUILD_TIMESTAMP" --platform linux/amd64 -f all.Dockerfile --target sidecar -t jitsucom/sidecar:"$VERSION" -t jitsucom/sidecar:beta --push .
docker buildx build --build-arg VERSION="$VERSION" --build-arg BUILD_TIMESTAMP="$BUILD_TIMESTAMP" --platform linux/amd64 -f all.Dockerfile --target syncctl -t jitsucom/syncctl:"$VERSION" -t jitsucom/syncctl:beta --push .