package app

import (
	"encoding/json"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/timestamp"
	"time"
)

type AbstractConsumer struct {
	appbase.Service
	bulkerProducer *Producer
	repository     *Repository
}

func NewAbstractConsumer(repository *Repository, topicId string, bulkerProducer *Producer) *AbstractConsumer {
	return &AbstractConsumer{
		Service:        appbase.NewServiceBase(topicId),
		bulkerProducer: bulkerProducer,
		repository:     repository,
	}
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
