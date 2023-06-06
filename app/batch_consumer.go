package app

import (
	"bytes"
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jitsucom/bulker/app/metrics"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/timestamp"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/types"
	jsoniter "github.com/json-iterator/go"
	"strconv"
	"time"
)

type BatchConsumerImpl struct {
	*AbstractBatchConsumer
	eventsLogService EventsLogService
}

func NewBatchConsumer(repository *Repository, destinationId string, batchPeriodSec int, topicId string, config *AppConfig, kafkaConfig *kafka.ConfigMap, eventsLogService EventsLogService) (*BatchConsumerImpl, error) {

	base, err := NewAbstractBatchConsumer(repository, destinationId, batchPeriodSec, topicId, "batch", config, kafkaConfig)
	if err != nil {
		return nil, err
	}
	bc := BatchConsumerImpl{
		AbstractBatchConsumer: base,
		eventsLogService:      eventsLogService,
	}
	bc.batchFunc = bc.processBatchImpl
	bc.pause()
	return &bc, nil
}

func (bc *BatchConsumerImpl) processBatchImpl(destination *Destination, batchNum, batchSize, retryBatchSize int) (counters BatchCounters, nextBatch bool, err error) {
	bulkerStream, err := destination.bulker.CreateStream(bc.topicId, bc.tableName, bulker.Batch, destination.streamOptions.Options...)
	if err != nil {
		bc.errorMetric("failed to create bulker stream")
		err = bc.NewError("Failed to create bulker stream: %w", err)
		return
	}
	ctx := context.WithValue(context.Background(), bulker.BatchNumberCtxKey, batchNum)

	//position of last message in batch in case of failed. Needed for processFailed
	var failedPosition *kafka.TopicPartition
	var firstPosition *kafka.TopicPartition
	defer func() {
		if err != nil {
			counters.failed = counters.consumed - counters.processed
			if failedPosition != nil {
				cnts, err2 := bc.processFailed(firstPosition, failedPosition)
				cnts.failed = counters.failed
				counters = cnts
				if err2 != nil {
					bc.errorMetric("PROCESS_FAILED_ERROR")
					logging.SystemError(err2)
					nextBatch = false
				} else {
					nextBatch = true
				}
			}
		}
	}()
	// we collect batchSize of messages but no longer than for 1/10 of batchPeriodSec
	_, highOffset, err := bc.consumer.QueryWatermarkOffsets(bc.topicId, 0, 10_000)
	if err != nil {
		bc.errorMetric("query_watermark_failed")
		err = bc.NewError("Failed to query watermark offsets: %w", err)
		return
	}
	var latestMessage *kafka.Message
	var processedObjectsSample []types.Object
	processed := 0
	for i := 0; i < batchSize; i++ {
		if bc.retired.Load() {
			_, _ = bulkerStream.Abort(ctx)
			return
		}
		if latestMessage != nil && int64(latestMessage.TopicPartition.Offset) == highOffset-1 {
			nextBatch = false
			bc.Debugf("Reached watermark offset %d. Stopping batch", highOffset-1)
			// we reached the end of the topic
			break
		}
		message, err := bc.consumer.ReadMessage(bc.waitForMessages)
		if err != nil {
			kafkaErr := err.(kafka.Error)
			if kafkaErr.Code() == kafka.ErrTimedOut {
				// waitForMessages period is over. it's ok. considering batch as full
				break
			}
			bc.errorMetric("consumer_error:" + metrics.KafkaErrorCode(kafkaErr))
			_, _ = bulkerStream.Abort(ctx)
			return counters, false, bc.NewError("Failed to consume event from topic. Retryable: %t: %w", kafkaErr.IsRetriable(), kafkaErr)
		}
		counters.consumed++
		retriesHeader := GetKafkaHeader(message, retriesCountHeader)
		if retriesHeader != "" {
			// we perform retries in smaller batches
			batchSize = retryBatchSize
		}
		latestMessage = message
		if firstPosition == nil {
			firstPosition = &message.TopicPartition
		}
		obj := types.Object{}
		dec := jsoniter.NewDecoder(bytes.NewReader(message.Value))
		dec.UseNumber()
		err = dec.Decode(&obj)
		if err == nil {
			bc.Debugf("%d. Consumed Message ID: %s Offset: %s (Retries: %s) for: %s", i, obj.Id(), message.TopicPartition.Offset.String(), GetKafkaHeader(message, retriesCountHeader), destination.config.BulkerType)
			_, processedObjectsSample, err = bulkerStream.Consume(ctx, obj)
			if err != nil {
				bc.errorMetric("bulker_stream_error")
			}
		} else {
			bc.errorMetric("parse_event_error")
		}
		if err != nil {
			failedPosition = &latestMessage.TopicPartition
			state, _ := bulkerStream.Abort(ctx)
			bc.postEventsLog(state, processedObjectsSample, err)
			return counters, false, bc.NewError("Failed to process event to bulker stream: %w", err)
		} else {
			processed++
		}
	}
	//we've processed some messages. it is time to commit them
	if processed > 0 {
		if processed == batchSize {
			nextBatch = true
		}
		// we need to pause consumer to avoid kafka session timeout while loading huge batches to slow destinations
		bc.pause()

		bc.Infof("Committing %d events to %s", processed, destination.config.BulkerType)
		var state bulker.State
		//TODO: do we need to interrupt commit if consumer is retired?
		state, err = bulkerStream.Complete(ctx)
		bc.postEventsLog(state, processedObjectsSample, err)
		if err != nil {
			failedPosition = &latestMessage.TopicPartition
			return counters, false, bc.NewError("Failed to commit bulker stream to %s: %w", destination.config.BulkerType, err)
		}
		counters.processed = processed
		_, err = bc.consumer.Commit()
		if err != nil {
			bc.errorMetric("KAFKA_COMMIT_ERR:" + metrics.KafkaErrorCode(err))
			bc.SystemErrorf("Failed to commit kafka consumer after batch was successfully committed to the destination: %w", err)
			err = bc.NewError("Failed to commit kafka consumer: %w", err)
			return
		}
	} else {
		_, _ = bulkerStream.Abort(ctx)
	}
	return
}

// processFailed consumes the latest failed batch of messages and sends them to the 'failed' topic
func (bc *BatchConsumerImpl) processFailed(firstPosition *kafka.TopicPartition, failedPosition *kafka.TopicPartition) (counters BatchCounters, err error) {
	defer func() {
		if err != nil {
			err = bc.NewError("Failed to put unsuccessful batch to 'failed' producer: %w", err)
		}
	}()
	err = bc.resume()
	if err != nil {
		return BatchCounters{}, fmt.Errorf("failed to resume kafka consumer: %w", err)
	}

	bc.Infof("Rolling back to first offset %d (failed at %d)", firstPosition.Offset, failedPosition.Offset)
	//Rollback consumer to committed offset
	_, err = bc.consumer.SeekPartitions([]kafka.TopicPartition{*firstPosition})
	if err != nil {
		bc.errorMetric("SEEK_ERROR")
		return BatchCounters{}, fmt.Errorf("failed to rollback kafka consumer offset: %w", err)
	}
	err = bc.producer.BeginTransaction()
	if err != nil {
		return BatchCounters{}, fmt.Errorf("failed to begin kafka transaction: %w", err)
	}
	defer func() {
		if err != nil {
			//cleanup
			_ = bc.producer.AbortTransaction(context.Background())
			_, err = bc.consumer.SeekPartitions([]kafka.TopicPartition{*firstPosition})
			if err != nil {
				bc.errorMetric("SEEK_ERROR")
			}
		}
	}()
	for {
		var message *kafka.Message
		message, err = bc.consumer.ReadMessage(pauseHeartBeatInterval)
		if err != nil {
			kafkaErr := err.(kafka.Error)
			if kafkaErr.Code() == kafka.ErrTimedOut {
				err = fmt.Errorf("failed to consume message: %w", err)
				return
			}
			if kafkaErr.IsRetriable() {
				time.Sleep(pauseHeartBeatInterval)
				continue
			} else {
				bc.restartConsumer()
				err = fmt.Errorf("failed to consume message: %w", err)
				return
			}
		}
		counters.consumed++
		deadLettered := false
		failedTopic, _ := MakeTopicId(bc.destinationId, retryTopicMode, allTablesToken, false)
		retries, err := GetKafkaIntHeader(message, retriesCountHeader)
		if err != nil {
			bc.Errorf("failed to read retry header: %w", err)
		}
		if retries >= bc.config.MessagesRetryCount {
			//no attempts left - send to dead-letter topic
			deadLettered = true
			failedTopic, _ = MakeTopicId(bc.destinationId, deadTopicMode, allTablesToken, false)
		}
		err = bc.producer.Produce(&kafka.Message{
			Key:            message.Key,
			TopicPartition: kafka.TopicPartition{Topic: &failedTopic, Partition: kafka.PartitionAny},
			Headers: []kafka.Header{
				{Key: retriesCountHeader, Value: []byte(strconv.Itoa(retries))},
				{Key: originalTopicHeader, Value: []byte(bc.topicId)},
				{Key: retryTimeHeader, Value: []byte(timestamp.ToISOFormat(RetryBackOffTime(bc.config, retries+1).UTC()))}},
			Value: message.Value,
		}, nil)
		if err != nil {
			return counters, fmt.Errorf("failed to put message to producer: %w", err)
		}
		if deadLettered {
			counters.deadLettered++
		} else {
			counters.retryScheduled++
		}
		//stop consuming on the latest message before failure
		if message.TopicPartition.Offset == failedPosition.Offset {
			break
		}
	}
	groupMetadata, err := bc.consumer.GetConsumerGroupMetadata()
	if err != nil {
		err = fmt.Errorf("failed to get consumer group metadata: %w", err)
		return
	}
	offset := *failedPosition
	offset.Offset++
	//set consumer offset to the next message after failure. that happens atomically with whole producer transaction
	err = bc.producer.SendOffsetsToTransaction(context.Background(), []kafka.TopicPartition{offset}, groupMetadata)
	if err != nil {
		err = fmt.Errorf("failed to send consumer offset to producer transaction: %w", err)
		return
	}
	err = bc.producer.CommitTransaction(context.Background())
	if err != nil {
		err = fmt.Errorf("failed to commit kafka transaction for producer: %w", err)
		return
	}
	return
}

func (bc *BatchConsumerImpl) postEventsLog(state bulker.State, processedObjectsSample []types.Object, batchErr error) {
	if batchErr != nil && state.LastError == nil {
		state.SetError(batchErr)
	}
	batchState := BatchState{State: state, LastMappedRow: processedObjectsSample}
	_, err2 := bc.eventsLogService.PostEvent(EventTypeBatchAll, bc.destinationId, batchState)
	if err2 != nil {
		bc.Errorf("Failed to post event to events log service: %w", err2)
	}
	if batchErr != nil {
		_, err2 = bc.eventsLogService.PostEvent(EventTypeBatchError, bc.destinationId, batchState)
		if err2 != nil {
			bc.Errorf("Failed to post event to events log service: %w", err2)
		}
	}
}

type BatchState struct {
	bulker.State  `json:",inline"`
	LastMappedRow []types.Object `json:"lastMappedRow"`
}
