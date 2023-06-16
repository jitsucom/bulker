package app

import (
	"context"
	"fmt"
	"github.com/hjson/hjson-go/v4"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/timestamp"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/prometheus/client_golang/prometheus"
	prom "github.com/prometheus/client_model/go"
	"strings"
	"time"
)

type MetricsRelay struct {
	appbase.Service
	instanceId  string
	relayBulker bulker.Bulker
	ticker      *utils.Ticker
	counters    map[string]float64
}

func NewMetricsRelay(appConfig *Config) (*MetricsRelay, error) {
	base := appbase.NewServiceBase("metrics_relay")
	if appConfig.MetricsRelayDestination == "" || appConfig.MetricsRelayPeriodSec == 0 {
		return nil, nil
	}
	cfg := &DestinationConfig{}
	err := hjson.Unmarshal([]byte(appConfig.MetricsRelayDestination), &cfg)
	if err != nil {
		return nil, fmt.Errorf("error parsing metrics relay destination: %v", err)
	}
	bulkerInstance, err := bulker.CreateBulker(cfg.Config)
	if bulkerInstance == nil {
		return nil, fmt.Errorf("error creating metrics relay bulker: %v", err)
	}
	period := time.Duration(appConfig.MetricsRelayPeriodSec) * time.Second
	m := &MetricsRelay{
		Service:     base,
		instanceId:  appConfig.InstanceId,
		relayBulker: bulkerInstance,
		ticker:      utils.NewTicker(period, period),
		counters:    map[string]float64{},
	}
	m.start()
	return m, nil

}

// start metrics relay
func (m *MetricsRelay) start() {
	m.Infof("Starting metrics relay")
	go func() {
		for {
			select {
			case <-m.ticker.C:
				err := m.Push()
				if err != nil {
					m.Errorf("Error pushing metrics: %v", err)
				}
			case <-m.ticker.Closed:
				m.Infof("Metrics relay stopped.")
				return
			}
		}
	}()
}

// Push metrics to destination
func (m *MetricsRelay) Push() (err error) {
	stream, err := m.relayBulker.CreateStream("metrics_relays", "bulker_metrics", bulker.Batch,
		bulker.WithTimestamp("timestamp"))
	if err != nil {
		return fmt.Errorf("error creating bulker stream: %v", err)
	}
	defer func() {
		if err != nil {
			_, _ = stream.Abort(context.Background())
		}
	}()
	ts := timestamp.Now().Truncate(m.ticker.Period())
	gatheredData, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return fmt.Errorf("failed to gather metrics: %v", err)
	}

	for _, metricFamily := range gatheredData {
		if metricFamily.Type == nil || *metricFamily.Type != prom.MetricType_COUNTER {
			continue
		}
		if !strings.HasPrefix(metricFamily.GetName(), "bulkerapp_") {
			continue
		}
		for _, metric := range metricFamily.Metric {
			if metric.Counter != nil {
				metricEvent := types.Object{}
				metricEvent["timestamp"] = ts
				metricEvent["instance"] = m.instanceId
				metricEvent["metric_type"] = "counter"
				metricEvent["metric_name"] = metricFamily.GetName()
				key := strings.Builder{}
				key.WriteString(metricFamily.GetName())
				key.WriteString("{")
				prevLabel := false
				for _, label := range metric.Label {
					if label.Name == nil || label.Value == nil || *label.Value == "" {
						continue
					}
					if prevLabel {
						key.WriteString(",")
					}
					metricEvent[*label.Name] = *label.Value
					key.WriteString(*label.Name)
					key.WriteString("=")
					key.WriteString(*label.Value)
					prevLabel = true
				}
				key.WriteString("}")
				metricKey := key.String()
				//metricEvent["metric_key"] = metricKey
				prevValue := m.counters[metricKey]
				delta := metric.Counter.GetValue() - prevValue
				if delta > 0 {
					metricEvent["value"] = delta
					m.counters[metricKey] = metric.Counter.GetValue()
					//m.Infof("Pushing metric: %+v", metricEvent)
					_, _, err = stream.Consume(context.Background(), metricEvent)
					if err != nil {
						return fmt.Errorf("error pushing metric: %v", err)
					}
				}
			}

		}
	}
	_, err = stream.Complete(context.Background())
	if err != nil {
		return fmt.Errorf("error commiting batch: %v", err)
	}
	return
}

// Stop metrics relay
func (m *MetricsRelay) Stop() error {
	if m == nil {
		return nil
	}
	m.ticker.Stop()
	return m.relayBulker.Close()
}
