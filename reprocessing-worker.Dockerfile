FROM debian:bullseye-slim as main

RUN apt-get update -y
RUN apt-get install -y ca-certificates curl

ENV TZ=UTC

FROM golang:1.24-bullseye as build

ARG VERSION
ENV VERSION $VERSION
ARG BUILD_TIMESTAMP
ENV BUILD_TIMESTAMP $BUILD_TIMESTAMP

RUN apt-get install gcc libc6-dev

RUN mkdir /app
WORKDIR /app

# Copy go.mod and go.sum for all modules
RUN mkdir jitsubase kafkabase reprocessing-worker

COPY jitsubase/go.* ./jitsubase/
COPY kafkabase/go.* ./kafkabase/
COPY reprocessing-worker/go.* ./reprocessing-worker/

# Initialize workspace
RUN go work init jitsubase kafkabase reprocessing-worker

WORKDIR /app/reprocessing-worker

RUN go mod download

WORKDIR /app

# Copy all source code
COPY . .

# Build reprocessing worker
RUN go build -ldflags="-X main.Commit=$VERSION -X main.Timestamp=$BUILD_TIMESTAMP" -o reprocessing-worker ./reprocessing-worker/

#######################################
# FINAL STAGE
FROM main as final

RUN mkdir /app
WORKDIR /app

# Copy worker binary
COPY --from=build /app/reprocessing-worker ./

CMD ["/app/reprocessing-worker"]
