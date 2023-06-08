package app

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jitsucom/bulker/jitsubase/timestamp"
	"math"
	"strconv"
	"time"
)

func GetKafkaHeader(message *kafka.Message, key string) string {
	for _, h := range message.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func GetKafkaIntHeader(message *kafka.Message, name string) (int, error) {
	v := GetKafkaHeader(message, name)
	if len(v) > 0 {
		return strconv.Atoi(v)
	} else {
		return 0, nil
	}
}

func GetKafkaTimeHeader(message *kafka.Message, name string) (time.Time, error) {
	v := GetKafkaHeader(message, name)
	if len(v) > 0 {
		return timestamp.ParseISOFormat(v)
	} else {
		return time.Time{}, nil
	}
}

func RetryBackOffTime(config *AppConfig, attempt int) time.Time {
	backOffDelay := time.Duration(math.Min(math.Pow(config.MessagesRetryBackoffBase, float64(attempt)), config.MessagesRetryBackoffMaxDelay)) * time.Minute
	return time.Now().Add(backOffDelay)
}
