FROM golang:1.20.2-alpine as build


RUN mkdir /app
WORKDIR /app

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

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