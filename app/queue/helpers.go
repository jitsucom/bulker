package queue

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/utils"
	"time"
)

/**
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Transaction helper methods

// createTransactionalProducer creates a transactional producer for the given
// input partition.
func createTransactionalProducer(kafkaConfig *kafka.ConfigMap, toppar kafka.TopicPartition, producers map[int32]*kafka.Producer) error {
	producerConfig := kafka.ConfigMap(utils.MapPutAll(kafka.ConfigMap{
		"transactional.id": fmt.Sprintf("bulker-transactional-producer-p%d", int(toppar.Partition)),
	}, *kafkaConfig))

	producer, err := kafka.NewProducer(&producerConfig)
	if err != nil {
		return err
	}

	maxDuration, err := time.ParseDuration("10s")
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), maxDuration)
	defer cancel()

	err = producer.InitTransactions(ctx)
	if err != nil {
		return err
	}

	err = producer.BeginTransaction()
	if err != nil {
		return err
	}

	producers[toppar.Partition] = producer
	logging.Infof("Processor: created producer %s for partition %v",
		producers[toppar.Partition], toppar.Partition)
	return nil
}

// destroyTransactionalProducer aborts the current transaction and destroys the producer.
func destroyTransactionalProducer(producer *kafka.Producer) error {
	maxDuration, err := time.ParseDuration("10s")
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), maxDuration)
	defer cancel()

	err = producer.AbortTransaction(ctx)
	if err != nil {
		if err.(kafka.Error).Code() == kafka.ErrState {
			// No transaction in progress, ignore the error.
			err = nil
		} else {
			logging.Infof("Failed to abort transaction for %s: %s",
				producer, err)
		}
	}

	producer.Close()

	return err
}

// groupRebalance is triggered on consumer rebalance.
//
// For each assigned partition a transactional producer is created, this is
// required to guarantee per-partition offset commit state prior to
// KIP-447 being supported.
func groupRebalance(kafkaConfig *kafka.ConfigMap, consumer *kafka.Consumer, event kafka.Event, producers map[int32]*kafka.Producer) error {
	logging.Infof("Processor: rebalance event %v", event)

	switch e := event.(type) {
	case kafka.AssignedPartitions:
		// Create a producer per input partition.
		for _, tp := range e.Partitions {
			err := createTransactionalProducer(kafkaConfig, tp, producers)
			if err != nil {
				return err
			}
		}

		err := consumer.Assign(e.Partitions)
		if err != nil {
			return err
		}

	case kafka.RevokedPartitions:
		// Abort any current transactions and close the
		// per-partition producers.
		for _, producer := range producers {
			err := destroyTransactionalProducer(producer)
			if err != nil {
				return err
			}
		}

		// Clear producer and intersection states
		utils.MapClear(producers)
		//intersectionStates = make(map[string]*intersectionState)

		err := consumer.Unassign()
		if err != nil {
			return err
		}
	}

	return nil
}

// rewindConsumerPosition rewinds the consumer to the last committed offset or
// the beginning of the partition if there is no committed offset.
// This is to be used when the current transaction is aborted.
func rewindConsumerPosition(consumer *kafka.Consumer, topic string, partition int32) error {
	committed, err := consumer.Committed([]kafka.TopicPartition{{Topic: &topic, Partition: partition}}, 10*1000 /* 10s */)
	if err != nil {
		return err
	}

	for _, tp := range committed {
		if tp.Offset < 0 {
			// No committed offset, reset to earliest
			tp.Offset = kafka.OffsetBeginning
		}

		logging.Infof("Processor: rewinding input partition %v to offset %v",
			tp.Partition, tp.Offset)

		err = consumer.Seek(tp, -1)
		if err != nil {
			return err
		}
	}
	return nil
}

// getConsumerPosition gets the current position (next offset) for a given input partition.
func getConsumerPosition(consumer *kafka.Consumer, topic string, partition int32) ([]kafka.TopicPartition, error) {
	position, err := consumer.Position([]kafka.TopicPartition{{Topic: &topic, Partition: partition}})
	if err != nil {
		return nil, err
	}

	return position, nil
}

// commitTransactionForInputPartition sends the consumer offsets for
// the given input partition and commits the current transaction.
// A new transaction will be started when done.
func commitTransactionForInputPartition(consumer *kafka.Consumer, topic string, producers map[int32]*kafka.Producer, partition int32) error {
	producer, found := producers[partition]
	if !found || producer == nil {
		return fmt.Errorf("BUG: No producer for input partition %v", partition)
	}

	position, err := getConsumerPosition(consumer, topic, partition)
	if err != nil {
		return fmt.Errorf("failed to get consumer group metadata: %v", err)
	}
	consumerMetadata, err := consumer.GetConsumerGroupMetadata()
	if err != nil {
		return fmt.Errorf("failed to get consumer group metadata: %v", err)
	}

	err = producer.SendOffsetsToTransaction(nil, position, consumerMetadata)
	if err != nil {
		logging.Errorf(
			"Processor: Failed to send offsets to transaction for input partition %v: %s: aborting transaction",
			partition, err)

		err = producer.AbortTransaction(nil)
		if err != nil {
			return err
		}

		// Rewind this input partition to the last committed offset.
		rewindConsumerPosition(consumer, topic, partition)
	} else {
		err = producer.CommitTransaction(nil)
		if err != nil {
			logging.Errorf(
				"Processor: Failed to commit transaction for input partition %v: %s",
				partition, err)

			err = producer.AbortTransaction(nil)
			if err != nil {
				return err
			}

			// Rewind this input partition to the last committed offset.
			rewindConsumerPosition(consumer, topic, partition)
		}
	}

	// Start a new transaction
	err = producer.BeginTransaction()
	if err != nil {
		return err
	}
	return nil
}
