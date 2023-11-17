#!/usr/bin/env bash

VERSION=$(git log -1 --pretty=%h)
TAG="$REPO$VERSION"
LATEST="${REPO}latest"
BUILD_TIMESTAMP=$( date '+%F_%H:%M:%S' )

docker buildx build --build-arg VERSION="$VERSION" --build-arg BUILD_TIMESTAMP="$BUILD_TIMESTAMP" --platform linux/amd64 -f bulker.Dockerfile -t jitsucom/bulker:"$VERSION" -t jitsucom/bulker:latest --push .