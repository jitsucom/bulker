FROM debian:bookworm-slim as base

RUN apt-get update -y
RUN apt-get install -y ca-certificates curl

ENV TZ=UTC

WORKDIR /app

FROM golang:1.21.6-bookworm as builder

ARG VERSION
ENV VERSION $VERSION
ARG BUILD_TIMESTAMP
ENV BUILD_TIMESTAMP $BUILD_TIMESTAMP

RUN apt-get install gcc libc6-dev

WORKDIR /app

RUN mkdir jitsubase kafkabase eventslog bulkerlib bulkerapp ingest sync-sidecar sync-controller
RUN mkdir connectors connectors/airbytecdk connectors/firebase

COPY jitsubase/go.* ./jitsubase/
COPY kafkabase/go.* ./kafkabase/
COPY eventslog/go.* ./eventslog/
COPY bulkerlib/go.* ./bulkerlib/
COPY bulkerapp/go.* ./bulkerapp/
COPY ingest/go.* ./ingest/
COPY sync-sidecar/go.* ./sync-sidecar/
COPY sync-controller/go.* ./sync-controller/

COPY admin/go.* ./admin/
COPY connectors/airbytecdk/go.* ./connectors/airbytecdk/
COPY connectors/firebase/go.* ./connectors/firebase/
COPY go.work ./go.work
COPY go.work.sum ./go.work.sum

#RUN go work init jitsubase kafkabase eventslog bulkerlib bulkerapp ingest sync-sidecar sync-controller
RUN --mount=type=cache,id=go_mod,mode=0755,target=/go/pkg/mod go mod download

WORKDIR /app

COPY . .

RUN --mount=type=cache,id=go_mod,mode=0755,target=/go/pkg/mod go build -ldflags="-X main.Commit=$VERSION -X main.Timestamp=$BUILD_TIMESTAMP" -o bulker ./bulkerapp
RUN --mount=type=cache,id=go_mod,mode=0755,target=/go/pkg/mod go build -ldflags="-X main.Commit=$VERSION -X main.Timestamp=$BUILD_TIMESTAMP" -o ingest ./ingest
RUN --mount=type=cache,id=go_mod,mode=0755,target=/go/pkg/mod go build -ldflags="-X main.Commit=$VERSION -X main.Timestamp=$BUILD_TIMESTAMP" -o sidecar ./sync-sidecar
RUN --mount=type=cache,id=go_mod,mode=0755,target=/go/pkg/mod go build -ldflags="-X main.Commit=$VERSION -X main.Timestamp=$BUILD_TIMESTAMP" -o syncctl ./sync-controller


FROM base as bulker

COPY --from=builder /app/bulker ./
CMD ["/app/bulker"]

FROM base as ingest

COPY --from=builder /app/ingest/ingest ./
CMD ["/app/ingest"]

FROM base as sidecar

COPY --from=builder /app/sidecar ./
CMD ["/app/sidecar"]

FROM base as syncctl

COPY --from=builder /app/syncctl ./
CMD ["/app/syncctl"]