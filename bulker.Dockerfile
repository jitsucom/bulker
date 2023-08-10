FROM debian:bullseye-slim as main

RUN apt-get update -y
RUN apt-get install -y ca-certificates curl

ENV TZ=UTC

FROM golang:1.21.0-bullseye as build

RUN apt-get install gcc libc6-dev

#RUN wget -qO - https://packages.confluent.io/deb/7.2/archive.key | apt-key add -
#RUN echo "deb https://packages.confluent.io/deb/7.2 stable main"  > /etc/apt/sources.list.d/backports.list
#RUN echo "deb https://packages.confluent.io/clients/deb buster main" > /etc/apt/sources.list.d/backports.list
#RUN apt-get update
#RUN apt-get install -y librdkafka1 librdkafka-dev

RUN mkdir /app
WORKDIR /app

RUN mkdir jitsubase bulkerlib bulkerapp

COPY jitsubase/go.* ./jitsubase/
COPY bulkerlib/go.* ./bulkerlib/
COPY bulkerapp/go.* ./bulkerapp/

RUN go work init jitsubase bulkerlib bulkerapp

WORKDIR /app/bulkerapp

RUN go mod download

WORKDIR /app

COPY . .

# Build bulker
RUN go build -o bulker ./bulkerapp

#######################################
# FINAL STAGE
FROM main as final

RUN mkdir /app
WORKDIR /app

# Copy bulkerapp
COPY --from=build /app/bulker ./
#COPY ./config.yaml ./

CMD ["/app/bulker"]