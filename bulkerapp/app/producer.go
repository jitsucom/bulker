package app

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jitsucom/bulker/kafkabase"
)

type Producer struct {
	*kafkabase.Producer
}

// NewProducer creates new Producer
func NewProducer(config *kafkabase.KafkaConfig, kafkaConfig *kafka.ConfigMap, reportQueueLength bool) (*Producer, error) {
	base, err := kafkabase.NewProducer(config, kafkaConfig, reportQueueLength, ProducerMessageLabels)
	if err != nil {
		return nil, err
	}
	return &Producer{
		Producer: base,
	}, nil
}

func ProducerMessageLabels(topicId string, status, errText string) (topic, destinationId, mode, tableName, st string, err string) {
	destinationId, mode, tableName, topicErr := ParseTopicId(topicId)
	if topicErr != nil {
		return topicId, "", "", "", status, errText
	} else {
		return topicId, destinationId, mode, tableName, status, errText
	}
}
