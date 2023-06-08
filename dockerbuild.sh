#!/usr/bin/env bash

cd bulkerapp

docker buildx build --platform linux/amd64 -t jitsucom/bulker:latest --load .