package kafkabase

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jitsucom/bulker/jitsubase/timestamp"
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

func PutKafkaHeader(headers *[]kafka.Header, key string, value string) {
	for i, h := range *headers {
		if h.Key == key {
			(*headers)[i] = kafka.Header{Key: key, Value: []byte(value)}
			return
		}
	}
	*headers = append(*headers, kafka.Header{
		Key:   key,
		Value: []byte(value),
	})
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
