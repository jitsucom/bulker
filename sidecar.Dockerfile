FROM golang:1.21.4-alpine as build


RUN mkdir /app
WORKDIR /app

COPY sync-sidecar/go.mod .
COPY sync-sidecar/go.sum .

RUN go mod download

# go mod download applied changes to go.sum that we don't want to override
RUN mv go.sum go.sum_

COPY sync-sidecar/. .

# restore modified go.sum
RUN mv go.sum_ go.sum

# Build bulker
RUN go build -o sidecar

#######################################
# FINAL STAGE
FROM alpine as final

ENV TZ=UTC

RUN mkdir /app
WORKDIR /app

# Copy bulkerapp
COPY --from=build /app/sidecar ./sidecar

CMD ["/app/sidecar"]