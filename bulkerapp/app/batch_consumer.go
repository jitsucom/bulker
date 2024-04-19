package app

import (
	"bytes"
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jitsucom/bulker/bulkerapp/metrics"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/eventslog"
	"github.com/jitsucom/bulker/jitsubase/timestamp"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/jitsucom/bulker/kafkabase"
	jsoniter "github.com/json-iterator/go"
	"strconv"
	"time"
)

type BatchConsumerImpl struct {
	*AbstractBatchConsumer
	eventsLogService eventslog.EventsLogService
}

func NewBatchConsumer(repository *Repository, destinationId string, batchPeriodSec int, topicId string, config *Config, kafkaConfig *kafka.ConfigMap, bulkerProducer *Producer, eventsLogService eventslog.EventsLogService) (*BatchConsumerImpl, error) {

	base, err := NewAbstractBatchConsumer(repository, destinationId, batchPeriodSec, topicId, "batch", config, kafkaConfig, bulkerProducer)
	if err != nil {
		return nil, err
	}
	bc := BatchConsumerImpl{
		AbstractBatchConsumer: base,
		eventsLogService:      eventsLogService,
	}
	bc.batchFunc = bc.processBatchImpl
	bc.pause(false)
	return &bc, nil
}

func (bc *BatchConsumerImpl) processBatchImpl(destination *Destination, batchNum, batchSize, retryBatchSize int, highOffset int64) (counters BatchCounters, nextBatch bool, err error) {
	bc.Debugf("Starting batch #%d", batchNum)
	counters.firstOffset = int64(kafka.OffsetBeginning)
	startTime := time.Now()
	var bulkerStream bulker.BulkerStream
	ctx := context.WithValue(context.Background(), bulker.BatchNumberCtxKey, batchNum)

	//position of last message in batch in case of failed. Needed for processFailed
	var failedPosition *kafka.TopicPartition
	var firstPosition *kafka.TopicPartition
	var latestMessage *kafka.Message
	defer func() {
		if err != nil {
			nextBatch = false
			counters.failed = counters.consumed - counters.processed
			if counters.failed > 0 {
				// we separate original errors from retry errors
				bc.SendMetrics(kafkabase.GetKafkaHeader(latestMessage, MetricsMetaHeader), "error", counters.failed-counters.retried)
				bc.SendMetrics(kafkabase.GetKafkaHeader(latestMessage, MetricsMetaHeader), "retry_error", counters.retried)
			}
			if failedPosition != nil {
				cnts, err2 := bc.processFailed(firstPosition, failedPosition, err)
				counters.deadLettered = cnts.deadLettered
				counters.retryScheduled = cnts.retryScheduled
				if err2 != nil {
					bc.errorMetric("PROCESS_FAILED_ERROR")
					bc.SystemErrorf(err2.Error())
				} else if counters.failed > 1 && int64(latestMessage.TopicPartition.Offset) < highOffset-1 {
					// if we fail right on the first message - that probably means connection problems. No need to move further.
					// otherwise we can try to consume next batch
					nextBatch = true
				}
			}
		} else if counters.processed > 0 {
			bc.SendMetrics(kafkabase.GetKafkaHeader(latestMessage, MetricsMetaHeader), "success", counters.processed)
		}
	}()
	var processedObjectSample types.Object
	processed := 0
	for i := 0; i < batchSize; i++ {
		if bc.retired.Load() {
			if bulkerStream != nil {
				_, _ = bulkerStream.Abort(ctx)
			}
			return
		}
		if latestMessage != nil && int64(latestMessage.TopicPartition.Offset) >= highOffset-1 {
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
		retriesHeader := kafkabase.GetKafkaHeader(message, retriesCountHeader)
		if retriesHeader != "" {
			// we perform retries in smaller batches
			//batchSize = retryBatchSize
			counters.retried++
		}
		latestMessage = message
		if firstPosition == nil {
			firstPosition = &message.TopicPartition
			counters.firstOffset = int64(message.TopicPartition.Offset)
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
				bc.Debugf("%d. Consumed Message ID: %s Offset: %s (Retries: %s) for: %s", i, obj.Id(), message.TopicPartition.Offset.String(), kafkabase.GetKafkaHeader(message, retriesCountHeader), destination.config.BulkerType)
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
			//treat failed message as processed
			state.ProcessedRows++
			state.ProcessingTimeSec = time.Since(startTime).Seconds()
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
		pauseTimer := time.AfterFunc(time.Duration(bc.config.KafkaMaxPollIntervalMs)*time.Millisecond/2, func() {
			// we need to pause consumer to avoid kafka session timeout while loading huge batches to slow destinations
			bc.pause(true)
		})

		bc.Infof("Batch #%d Committing %d events to %s", batchNum, processed, destination.config.BulkerType)
		var state bulker.State
		//TODO: do we need to interrupt commit if consumer is retired?
		state, err = bulkerStream.Complete(ctx)
		pauseTimer.Stop()
		state.ProcessingTimeSec = time.Since(startTime).Seconds()
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
func (bc *BatchConsumerImpl) processFailed(firstPosition *kafka.TopicPartition, failedPosition *kafka.TopicPartition, originalErr error) (counters BatchCounters, err error) {
	var producer *kafka.Producer
	defer func() {
		//recover
		if r := recover(); r != nil {
			err = bc.NewError("Recovered from panic: %v", r)
			bc.SystemErrorf("Recovered from panic: %v", r)
		}
		if err != nil {
			err = bc.NewError("Failed to put unsuccessful batch to 'failed' producer: %v", err)
		}
		if producer != nil {
			producer.Close()
		}
	}()
	producer, err = bc.initTransactionalProducer()
	if err != nil {
		return
	}

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
			_, err2 := bc.consumer.Load().SeekPartitions([]kafka.TopicPartition{*firstPosition})
			if err2 != nil {
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
		retries, err := kafkabase.GetKafkaIntHeader(message, retriesCountHeader)
		if err != nil {
			bc.Errorf("failed to read retry header: %v", err)
		}
		if retries >= bc.config.MessagesRetryCount {
			//no attempts left - send to dead-letter topic
			deadLettered = true
			failedTopic, _ = MakeTopicId(bc.destinationId, deadTopicMode, allTablesToken, false)
		}
		headers := message.Headers
		kafkabase.PutKafkaHeader(&headers, errorHeader, utils.ShortenStringWithEllipsis(originalErr.Error(), 256))
		kafkabase.PutKafkaHeader(&headers, originalTopicHeader, bc.topicId)
		kafkabase.PutKafkaHeader(&headers, retriesCountHeader, strconv.Itoa(retries))
		kafkabase.PutKafkaHeader(&headers, retryTimeHeader, timestamp.ToISOFormat(RetryBackOffTime(bc.config, retries+1).UTC()))
		err = producer.Produce(&kafka.Message{
			Key:            message.Key,
			TopicPartition: kafka.TopicPartition{Topic: &failedTopic, Partition: kafka.PartitionAny},
			Headers:        headers,
			Value:          message.Value,
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
	level := eventslog.LevelInfo
	if batchErr != nil {
		level = eventslog.LevelError
	}
	bc.eventsLogService.PostAsync(&eventslog.ActorEvent{EventType: eventslog.EventTypeBatch, Level: level, ActorId: bc.destinationId, Event: batchState})
}

type BatchState struct {
	bulker.State  `json:",inline"`
	LastMappedRow types.Object `json:"lastMappedRow"`
}
