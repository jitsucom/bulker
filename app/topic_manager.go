package app

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/utils"
	"regexp"
	"sync"
	"time"
)

var TopicPattern = regexp.MustCompile(`^incoming[.]destinationId[.](.*)[.]mode[.](.*)[.]tableName[.](.*)$`)

type TopicManager struct {
	sync.Mutex
	config *AppConfig

	kafkaBootstrapServer string
	//consumer         *kafka.Consumer
	kaftaAdminClient *kafka.AdminClient

	//topics by destinationId
	topics map[string]utils.Set[string]
	closed chan struct{}
}

// NewTopicManager returns TopicManager
func NewTopicManager(config *AppConfig, kafkaConfig *kafka.ConfigMap) (*TopicManager, error) {
	admin, err := kafka.NewAdminClient(kafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("[topic-manager] Error creating kafka admin client: %w", err)
	}
	return &TopicManager{
		config:               config,
		kaftaAdminClient:     admin,
		kafkaBootstrapServer: config.KafkaBootstrapServers,
		topics:               make(map[string]utils.Set[string]),
		closed:               make(chan struct{}),
	}, nil
}

// Start starts TopicManager
func (tm *TopicManager) Start() error {
	metadata, err := tm.kaftaAdminClient.GetMetadata(nil, true, tm.config.KafkaAdminMetadataTimeoutMs)
	if err != nil {
		return fmt.Errorf("[topic-manager] Error getting metadata: %w", err)
	}
	// refresh metadata every 10 seconds
	go func() {
		ticker := time.NewTicker(time.Duration(tm.config.TopicManagerRefreshPeriodSec) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-tm.closed:
				return
			case <-ticker.C:
				//start := time.Now()
				metadata, err = tm.kaftaAdminClient.GetMetadata(nil, true, tm.config.KafkaAdminMetadataTimeoutMs)
				if err != nil {
					logging.Errorf("[topic-manager] Error getting metadata: %v", err)
					continue
				}
				tm.Lock()
				for topic, _ := range metadata.Topics {
					topics := TopicPattern.FindStringSubmatch(topic)
					if len(topics) == 4 {
						destinationId := topics[1]
						tableName := topics[3]
						var set utils.Set[string]
						ok := false
						if set, ok = tm.topics[destinationId]; !ok {
							set = utils.NewSet[string]()
							tm.topics[destinationId] = set
						}
						if !set.Contains(topic) {
							logging.Infof("[topic-manager] Found topic %s for destination %s and table %s", topic, destinationId, tableName)
							set.Put(topic)
						}
					}
				}
				tm.Unlock()
				//logging.Infof("[topic-manager] Refreshed metadata in %v", time.Since(start))
			}
		}
	}()
	return nil
}

// GetTopicsSlice returns topics for destinationId
func (tm *TopicManager) GetTopicsSlice(destinationId string) []string {
	tm.Lock()
	defer tm.Unlock()
	if set, ok := tm.topics[destinationId]; ok {
		return set.ToSlice()
	}
	return nil
}

// GetTopics returns topics for destinationId
func (tm *TopicManager) GetTopics(destinationId string) utils.Set[string] {
	tm.Lock()
	defer tm.Unlock()
	if set, ok := tm.topics[destinationId]; ok {
		return set.Clone()
	}
	return nil
}

//// Remove all topics for destination
//func (tm *TopicManager) RemoveTopics(destinationId string) {
//	tm.Lock()
//	defer tm.Unlock()
//	if set, ok := tm.topics[destinationId]; ok {
//		for topic, _ := range set {
//			_, err := tm.kaftaAdminClient.DeleteTopics([]string{topic})
//			if err != nil {
//				logging.Errorf("[topic-manager] Error deleting topic %s: %v", topic, err)
//			}
//		}
//		delete(tm.topics, destinationId)
//	}
//}

// CreateTopic creates topic for destinationId and tableName
func (tm *TopicManager) CreateTopic(destination *Destination, tableName string) error {
	topic := destination.TopicId(tableName)
	tm.Lock()
	defer tm.Unlock()
	var set utils.Set[string]
	ok := false
	if set, ok = tm.topics[destination.Id()]; !ok {
		set = utils.NewSet[string]()
		tm.topics[destination.Id()] = set
	}
	topicRes, err := tm.kaftaAdminClient.CreateTopics(context.Background(), []kafka.TopicSpecification{
		{
			Topic:         topic,
			NumPartitions: tm.config.KafkaTopicPartitionsCount,
			//TODO: get broker count from admin
			ReplicationFactor: tm.config.KafkaTopicReplicationFactor,
			Config: map[string]string{
				"retention.ms": fmt.Sprint(tm.config.KafkaTopicRetentionMs),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("[topic-manager] Error creating topic %s: %w", topic, err)
	}
	for _, res := range topicRes {
		if res.Error.Code() != kafka.ErrNoError {
			return fmt.Errorf("[topic-manager] Error creating topic %s: %v", topic, res.Error)
		}
	}
	set.Put(topic)
	return nil
}

func (tm *TopicManager) Close() error {
	close(tm.closed)
	tm.kaftaAdminClient.Close()
	return nil
}
