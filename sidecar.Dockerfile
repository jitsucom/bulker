FROM golang:1.22.2-alpine as build


RUN mkdir /app
WORKDIR /app

RUN mkdir jitsubase sync-sidecar

COPY jitsubase/go.* ./jitsubase/
COPY sync-sidecar/go.* ./sync-sidecar/

RUN go work init jitsubase sync-sidecar

WORKDIR /app/sync-sidecar

RUN go mod download

WORKDIR /app

COPY . .

# Build bulker
RUN go build -o sidecar ./sync-sidecar

#######################################
# FINAL STAGE
FROM alpine as final

ENV TZ=UTC

RUN mkdir /app
WORKDIR /app

# Copy bulkerapp
COPY --from=build /app/sidecar ./

CMD ["/app/sidecar"]