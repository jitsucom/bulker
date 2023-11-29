package app

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/timestamp"
	"math"
	"time"
)

const MetricsMetaHeader = "metrics_meta"

type AbstractConsumer struct {
	appbase.Service
	config         *Config
	topicId        string
	bulkerProducer *Producer
	repository     *Repository
}

func NewAbstractConsumer(config *Config, repository *Repository, topicId string, bulkerProducer *Producer) *AbstractConsumer {
	return &AbstractConsumer{
		Service:        appbase.NewServiceBase(topicId),
		config:         config,
		topicId:        topicId,
		bulkerProducer: bulkerProducer,
		repository:     repository,
	}
}

func (ac *AbstractConsumer) GetInstanceId() string {
	// range partitioner assigner distributes partitions between consumers in alphabetical order
	// since bulker topics mostly have only 1 partition – instance with the lowest instanceId will be assigned for all topic.
	// we use first letters of hash of 'topicId + instanceId' as a beginning of 'group.instance.id'
	// so for each topic the first instance will be different
	// while keeping consistency between restarts (if instanceId is the same)
	firstByte := md5.Sum([]byte(ac.topicId + ac.config.InstanceId))[0]
	return fmt.Sprintf("%x-%s", firstByte, ac.config.InstanceId)
}

func (ac *AbstractConsumer) SendMetrics(metricsMeta string, status string, events int) {
	if metricsMeta == "" || events <= 0 {
		return
	}
	metricsDst := ac.repository.GetDestination("metrics")
	if metricsDst == nil {
		return
	}
	topicId, err := metricsDst.TopicId("metrics")
	if err != nil {
		ac.Errorf("Error getting topicId for metrics destination: %v", err)
		return
	}
	meta := make(map[string]any)
	err = json.Unmarshal([]byte(metricsMeta), &meta)
	if err != nil {
		ac.Errorf("Failed to unmarshal metrics meta: %v", err)
		return
	}
	meta["status"] = status
	meta["events"] = events
	meta["timestamp"] = timestamp.ToISOFormat(time.Now().Truncate(time.Minute))
	payload, _ := json.Marshal(meta)
	//ac.Infof("Sending metrics to topic %s: %+v", topicId, meta)
	err = ac.bulkerProducer.ProduceAsync(topicId, "", payload, nil)
	if err != nil {
		ac.Errorf("Error producing metrics to metrics destination: %v", err)
		return
	}
}

func RetryBackOffTime(config *Config, attempt int) time.Time {
	backOffDelay := time.Duration(math.Min(math.Pow(config.MessagesRetryBackoffBase, float64(attempt)), config.MessagesRetryBackoffMaxDelay)) * time.Minute
	return time.Now().Add(backOffDelay)
}
