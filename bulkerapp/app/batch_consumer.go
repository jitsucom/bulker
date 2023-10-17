package app

import (
	"bytes"
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jitsucom/bulker/bulkerapp/metrics"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/jitsucom/bulker/jitsubase/timestamp"
	jsoniter "github.com/json-iterator/go"
	"strconv"
	"time"
)

type BatchConsumerImpl struct {
	*AbstractBatchConsumer
	eventsLogService EventsLogService
}

func NewBatchConsumer(repository *Repository, destinationId string, batchPeriodSec int, topicId string, config *Config, kafkaConfig *kafka.ConfigMap, eventsLogService EventsLogService) (*BatchConsumerImpl, error) {

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
	var bulkerStream bulker.BulkerStream
	ctx := context.WithValue(context.Background(), bulker.BatchNumberCtxKey, batchNum)

	//position of last message in batch in case of failed. Needed for processFailed
	var failedPosition *kafka.TopicPartition
	var firstPosition *kafka.TopicPartition
	defer func() {
		if err != nil {
			nextBatch = false
			counters.failed = counters.consumed - counters.processed
			if failedPosition != nil {
				cnts, err2 := bc.processFailed(firstPosition, failedPosition)
				cnts.failed = counters.failed
				counters = cnts
				if err2 != nil {
					bc.errorMetric("PROCESS_FAILED_ERROR")
					logging.SystemError(err2)
				} else if cnts.failed > 1 {
					// if we fail right on the first message - that probably means connection problems. No need to move further.
					// otherwise we can try to consume next batch
					nextBatch = true
				}
			}
		}
	}()
	// we collect batchSize of messages but no longer than for 1/10 of batchPeriodSec
	_, highOffset, err := bc.consumer.Load().QueryWatermarkOffsets(bc.topicId, 0, 10_000)
	if err != nil {
		bc.errorMetric("query_watermark_failed")
		err = bc.NewError("Failed to query watermark offsets: %v", err)
		return
	}
	var latestMessage *kafka.Message
	var processedObjectSample types.Object
	processed := 0
	for i := 0; i < batchSize; i++ {
		if bc.retired.Load() {
			if bulkerStream != nil {
				_, _ = bulkerStream.Abort(ctx)
			}
			return
		}
		if latestMessage != nil && int64(latestMessage.TopicPartition.Offset) == highOffset-1 {
			nextBatch = false
			bc.Debugf("Reached watermark offset %d. Stopping batch", highOffset-1)
			// we reached the end of the topic
			break
		}
		message, err := bc.consumer.Load().ReadMessage(bc.waitForMessages)
		if err != nil {
			kafkaErr := err.(kafka.Error)
			if kafkaErr.Code() == kafka.ErrTimedOut {
				// waitForMessages period is over. it's ok. considering batch as full
				break
			}
			bc.errorMetric("consumer_error:" + metrics.KafkaErrorCode(kafkaErr))
			if bulkerStream != nil {
				_, _ = bulkerStream.Abort(ctx)
			}
			return counters, false, bc.NewError("Failed to consume event from topic. Retryable: %t: %v", kafkaErr.IsRetriable(), kafkaErr)
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
			if bulkerStream == nil {
				destination.InitBulkerInstance()
				bulkerStream, err = destination.bulker.CreateStream(bc.topicId, bc.tableName, bulker.Batch, destination.streamOptions.Options...)
				if err != nil {
					bc.errorMetric("failed to create bulker stream")
					err = bc.NewError("Failed to create bulker stream: %v", err)
				}
			}
			if err == nil {
				bc.Debugf("%d. Consumed Message ID: %s Offset: %s (Retries: %s) for: %s", i, obj.Id(), message.TopicPartition.Offset.String(), GetKafkaHeader(message, retriesCountHeader), destination.config.BulkerType)
				_, processedObjectSample, err = bulkerStream.Consume(ctx, obj)
				if err != nil {
					bc.errorMetric("bulker_stream_error")
				}
			}
		} else {
			bc.errorMetric("parse_event_error")
		}
		if err != nil {
			failedPosition = &latestMessage.TopicPartition
			state := bulker.State{}
			if bulkerStream != nil {
				state, _ = bulkerStream.Abort(ctx)
			}
			bc.postEventsLog(state, processedObjectSample, err)
			return counters, false, bc.NewError("Failed to process event to bulker stream: %v", err)
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
		bc.postEventsLog(state, processedObjectSample, err)
		if err != nil {
			failedPosition = &latestMessage.TopicPartition
			return counters, false, bc.NewError("Failed to commit bulker stream to %s: %v", destination.config.BulkerType, err)
		}
		counters.processed = processed
		_, err = bc.consumer.Load().CommitMessage(latestMessage)
		if err != nil {
			bc.errorMetric("KAFKA_COMMIT_ERR:" + metrics.KafkaErrorCode(err))
			bc.SystemErrorf("Failed to commit kafka consumer after batch was successfully committed to the destination: %v", err)
			err = bc.NewError("Failed to commit kafka consumer: %v", err)
			return
		}
	} else if bulkerStream != nil {
		_, _ = bulkerStream.Abort(ctx)
	}
	return
}

// processFailed consumes the latest failed batch of messages and sends them to the 'failed' topic
func (bc *BatchConsumerImpl) processFailed(firstPosition *kafka.TopicPartition, failedPosition *kafka.TopicPartition) (counters BatchCounters, err error) {
	defer func() {
		//recover
		if r := recover(); r != nil {
			err = bc.NewError("Recovered from panic: %v", r)
			bc.SystemErrorf("Recovered from panic: %v", r)
		}
		if err != nil {
			err = bc.NewError("Failed to put unsuccessful batch to 'failed' producer: %v", err)
		}
	}()
	producer := bc.initProducer()

	bc.resume()

	bc.Infof("Rolling back to first offset %d (failed at %d)", firstPosition.Offset, failedPosition.Offset)
	//Rollback consumer to committed offset
	_, err = bc.consumer.Load().SeekPartitions([]kafka.TopicPartition{*firstPosition})
	if err != nil {
		bc.errorMetric("SEEK_ERROR")
		return BatchCounters{}, fmt.Errorf("failed to rollback kafka consumer offset: %v", err)
	}
	err = producer.BeginTransaction()
	if err != nil {
		return BatchCounters{}, fmt.Errorf("failed to begin kafka transaction: %v", err)
	}
	defer func() {
		if err != nil {
			//cleanup
			_ = producer.AbortTransaction(context.Background())
			_, err = bc.consumer.Load().SeekPartitions([]kafka.TopicPartition{*firstPosition})
			if err != nil {
				bc.errorMetric("SEEK_ERROR")
			}
		}
	}()
	for {
		var message *kafka.Message
		message, err = bc.consumer.Load().ReadMessage(bc.waitForMessages)
		if err != nil {
			kafkaErr := err.(kafka.Error)
			if kafkaErr.Code() == kafka.ErrTimedOut {
				err = fmt.Errorf("failed to consume message: %v", err)
				return
			}
			if kafkaErr.IsRetriable() {
				time.Sleep(10 * time.Second)
				continue
			} else {
				bc.restartConsumer()
				err = fmt.Errorf("failed to consume message: %v", err)
				return
			}
		}
		counters.consumed++
		deadLettered := false
		failedTopic, _ := MakeTopicId(bc.destinationId, retryTopicMode, allTablesToken, false)
		retries, err := GetKafkaIntHeader(message, retriesCountHeader)
		if err != nil {
			bc.Errorf("failed to read retry header: %v", err)
		}
		if retries >= bc.config.MessagesRetryCount {
			//no attempts left - send to dead-letter topic
			deadLettered = true
			failedTopic, _ = MakeTopicId(bc.destinationId, deadTopicMode, allTablesToken, false)
		}
		err = producer.Produce(&kafka.Message{
			Key:            message.Key,
			TopicPartition: kafka.TopicPartition{Topic: &failedTopic, Partition: kafka.PartitionAny},
			Headers: []kafka.Header{
				{Key: retriesCountHeader, Value: []byte(strconv.Itoa(retries))},
				{Key: originalTopicHeader, Value: []byte(bc.topicId)},
				{Key: retryTimeHeader, Value: []byte(timestamp.ToISOFormat(RetryBackOffTime(bc.config, retries+1).UTC()))}},
			Value: message.Value,
		}, nil)
		if err != nil {
			return counters, fmt.Errorf("failed to put message to producer: %v", err)
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
	groupMetadata, err := bc.consumer.Load().GetConsumerGroupMetadata()
	if err != nil {
		err = fmt.Errorf("failed to get consumer group metadata: %v", err)
		return
	}
	offset := *failedPosition
	offset.Offset++
	//set consumer offset to the next message after failure. that happens atomically with whole producer transaction
	err = producer.SendOffsetsToTransaction(context.Background(), []kafka.TopicPartition{offset}, groupMetadata)
	if err != nil {
		err = fmt.Errorf("failed to send consumer offset to producer transaction: %v", err)
		return
	}
	err = producer.CommitTransaction(context.Background())
	if err != nil {
		err = fmt.Errorf("failed to commit kafka transaction for producer: %v", err)
		return
	}
	return
}

func (bc *BatchConsumerImpl) postEventsLog(state bulker.State, processedObjectSample types.Object, batchErr error) {
	if batchErr != nil && state.LastError == nil {
		state.SetError(batchErr)
	}
	batchState := BatchState{State: state, LastMappedRow: processedObjectSample}
	bc.eventsLogService.PostAsync(&ActorEvent{EventTypeBatchAll, bc.destinationId, batchState})
	if batchErr != nil {
		bc.eventsLogService.PostAsync(&ActorEvent{EventTypeBatchError, bc.destinationId, batchState})
	}
}

type BatchState struct {
	bulker.State  `json:",inline"`
	LastMappedRow types.Object `json:"lastMappedRow"`
}
