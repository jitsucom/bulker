package main

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/safego"
	"github.com/mroth/weightedrand/v2"
	"sync"
	"time"
)

type ConsumerMonitor struct {
	sync.RWMutex
	appbase.Service
	adminClient       *kafka.AdminClient
	topic             string
	consumerGroupName string
	threshold         int64
	weightedRandom    *weightedrand.Chooser[int32, int64]
	closed            chan struct{}
}

func NewConsumerMonitor(appContext *Context, topic, consumerGroupName string) (*ConsumerMonitor, error) {
	base := appbase.NewServiceBase("topic-monitor")

	adminClient, err := kafka.NewAdminClient(appContext.kafkaConfig)
	if err != nil {
		return nil, err
	}
	c := &ConsumerMonitor{
		Service:           base,
		adminClient:       adminClient,
		topic:             topic,
		threshold:         appContext.config.WeightedPartitionSelectorLagThreshold,
		consumerGroupName: consumerGroupName,
		closed:            make(chan struct{}),
	}
	c.Start()
	return c, nil
}

func (tm *ConsumerMonitor) Start() {
	safego.RunWithRestart(func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-tm.closed:
				return
			case <-ticker.C:
				res, err := tm.adminClient.ListConsumerGroupOffsets(context.Background(),
					[]kafka.ConsumerGroupTopicPartitions{{Group: tm.consumerGroupName}})
				if err != nil {
					tm.Errorf("Error getting consumer group offsets: %v", err)
					continue
				}
				topicPartitionOffsets := make(map[kafka.TopicPartition]kafka.OffsetSpec)
				lags := make(map[int32]int64)
				for _, group := range res.ConsumerGroupsTopicPartitions {
					for _, partition := range group.Partitions {
						lags[partition.Partition] = int64(partition.Offset)
						topicPartitionOffsets[kafka.TopicPartition{Topic: partition.Topic, Partition: partition.Partition}] = kafka.LatestOffsetSpec
					}
				}
				latestOffsets, err := tm.adminClient.ListOffsets(context.Background(), topicPartitionOffsets)
				if err != nil {
					tm.Errorf("Error getting latest offsets: %v", err)
					continue
				}
				var maxLag int64
				for tp, offset := range latestOffsets.ResultInfos {
					lag := int64(offset.Offset) - lags[tp.Partition]
					if lag > maxLag {
						maxLag = lag
					}
					lags[tp.Partition] = lag
				}
				if maxLag > tm.threshold {
					choices := make([]weightedrand.Choice[int32, int64], 0, len(lags))
					for part, lag := range lags {
						choices = append(choices, weightedrand.NewChoice(part, maxLag-lag+1))
					}
					chooser, err := weightedrand.NewChooser(choices...)
					if err != nil {
						tm.Errorf("Error getting latest offsets: %v", err)
						continue
					}
					tm.Lock()
					if tm.weightedRandom == nil {
						tm.Infof("Enabling weighed random partition selector. Lags: %v", lags)
					}
					tm.weightedRandom = chooser
					tm.Unlock()
				} else {
					tm.Lock()
					if tm.weightedRandom != nil {
						tm.weightedRandom = nil
						tm.Infof("Disabling weighed random partition selector. Max lag is low: %d", maxLag)
					}
					tm.Unlock()
				}
			}
		}
	})
}

func (tm *ConsumerMonitor) SelectPartition() int32 {
	tm.RLock()
	defer tm.RUnlock()
	if tm.weightedRandom == nil {
		return kafka.PartitionAny
	}
	return tm.weightedRandom.Pick()
}

func (tm *ConsumerMonitor) Close() error {
	tm.adminClient.Close()
	return nil
}
