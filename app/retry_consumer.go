package app

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"strconv"
	"time"
)

type RetryConsumer struct {
	AbstractBatchConsumer
}

func NewRetryConsumer(repository *Repository, destinationId string, batchPeriodSec int, topicId string, config *AppConfig, kafkaConfig *kafka.ConfigMap) (*RetryConsumer, error) {
	base, err := NewAbstractBatchConsumer(repository, destinationId, batchPeriodSec, topicId, "retry", config, kafkaConfig)
	if err != nil {
		return nil, err
	}
	rc := RetryConsumer{
		AbstractBatchConsumer: *base,
	}
	rc.batchFunc = rc.processBatchImpl
	rc.pause()
	return &rc, nil
}

func (rc *RetryConsumer) processBatchImpl(destination *Destination, batchSize, retryBatchSize int) (counters BatchCounters, nextBatch bool, err error) {
	var firstPosition *kafka.TopicPartition
	var lastPosition *kafka.TopicPartition

	txOpened := false
	defer func() {
		if err != nil {
			//cleanup
			if firstPosition != nil {
				_ = rc.consumer.Seek(*firstPosition, 10_000)
			}
			if txOpened {
				_ = rc.producer.AbortTransaction(context.Background())
			}
			nextBatch = false
		}
	}()
	nextBatch = true
	// we collect batchSize of messages but no longer than for 1/10 of batchPeriodSec
	timeEnd := time.Now().Add(time.Duration(rc.batchPeriodSec) * time.Second)
	for i := 0; i < retryBatchSize; i++ {
		if rc.retired.Load() {
			return
		}
		wait := timeEnd.Sub(time.Now())
		if wait <= 0 {
			break
		}
		message, err := rc.consumer.ReadMessage(rc.waitForMessages)
		if err != nil {
			kafkaErr := err.(kafka.Error)
			if kafkaErr.Code() == kafka.ErrTimedOut {
				nextBatch = false
				// waitForMessages period is over. it's ok. considering batch as full
				break
			}
			return BatchCounters{}, false, rc.NewError("Failed to consume event from topic. Retryable: %t: %w", kafkaErr.IsRetriable(), kafkaErr)
		}
		counters.consumed++
		lastPosition = &message.TopicPartition
		if counters.consumed == 1 {
			firstPosition = &message.TopicPartition
			err = rc.producer.BeginTransaction()
			if err != nil {
				return BatchCounters{}, false, fmt.Errorf("failed to begin kafka transaction: %w", err)
			}
			txOpened = true
		}
		singleCount := BatchCounters{}
		originalTopic := GetKafkaHeader(message, originalTopicHeader)
		topic := originalTopic
		if topic == "" {
			singleCount.skipped++
			rc.Errorf("Failed to get original topic from message headers. Skipping message")
			continue
		}
		rc.Debugf("message %s header: %v", message.TopicPartition.Offset, message.Headers)
		retries, err := GetKafkaIntHeader(message, retriesCountHeader)
		if err != nil {
			singleCount.skipped++
			rc.Errorf("Failed to get retries count from message headers. Skipping message")
			continue
		}
		headers := make([]kafka.Header, 0, len(message.Headers))
		if retries >= rc.config.MessagesRetryCount {
			singleCount.deadLettered++
			//no attempts left - send to dead-letter topic
			topic, _ = MakeTopicId(rc.destinationId, deadTopicMode, allTablesToken, false)
		} else if !rc.isTimeToRetry(message) {
			singleCount.notReadyReadded++
			// retry time is not yet come. requeueing message
			topic = rc.topicId
			headers = append(headers, kafka.Header{Key: retryTimeHeader, Value: []byte(GetKafkaHeader(message, retryTimeHeader))})
		} else {
			retries++
			singleCount.retryScheduled++
		}
		headers = append(headers, kafka.Header{Key: originalTopicHeader, Value: []byte(originalTopic)})
		headers = append(headers, kafka.Header{Key: retriesCountHeader, Value: []byte(strconv.Itoa(retries))})
		err = rc.producer.Produce(&kafka.Message{
			Key:            message.Key,
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Headers:        headers,
			Value:          message.Value,
		}, nil)
		if err != nil {
			return counters, false, fmt.Errorf("failed to put message to producer: %w", err)
		}
		counters.accumulate(singleCount)

	}
	if !txOpened {
		return
	}
	groupMetadata, err := rc.consumer.GetConsumerGroupMetadata()
	if err != nil {
		return BatchCounters{}, false, fmt.Errorf("failed to get consumer group metadata: %w", err)
	}
	offset := *lastPosition
	offset.Offset++
	//set consumer offset to the next message after failure. that happens atomically with whole producer transaction
	err = rc.producer.SendOffsetsToTransaction(context.Background(), []kafka.TopicPartition{offset}, groupMetadata)
	if err != nil {
		return BatchCounters{}, false, fmt.Errorf("failed to send consumer offset to producer transaction: %w", err)
	}
	err = rc.producer.CommitTransaction(context.Background())
	if err != nil {
		return BatchCounters{}, false, fmt.Errorf("failed to commit kafka transaction for producer: %w", err)
	}
	return
}

func (rc *RetryConsumer) isTimeToRetry(message *kafka.Message) bool {
	retryTime, err := GetKafkaTimeHeader(message, retryTimeHeader)
	if err != nil {
		rc.Errorf("failed to parse retry_time: %v", err)
		return true
	}
	if retryTime.IsZero() || time.Now().After(retryTime) {
		return true
	}
	return false
}
