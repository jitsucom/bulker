package main

import (
	"encoding/json"
	"github.com/mitchellh/mapstructure"
)

type TaskDescriptor struct {
	TaskID         string `json:"taskId"`
	TaskType       string `json:"taskType"` //spec, discover, read, check
	SourceID       string `json:"sourceId"`
	SourceType     string `json:"sourceType"`
	DestinationId  string `json:"destinationId"`
	Protocol       string `json:"protocol"`
	Package        string `json:"package"`
	PackageVersion string `json:"packageVersion"`
}

func (t *TaskDescriptor) ExtractLabels() map[string]string {
	rawLabels := map[string]string{}
	_ = mapstructure.Decode(t, &rawLabels)
	labels := make(map[string]string, len(rawLabels)+1)
	for k, v := range rawLabels {
		if v != "" {
			labels[k8sLabelPrefix+k] = labelUnsupportedChars.ReplaceAllLiteralString(v, "_")
		}
	}
	labels[k8sCreatorLabel] = k8sCreatorLabelValue
	return labels
}

type TaskStatus struct {
	TaskDescriptor `json:",inline" mapstructure:",squash" `
	PodName        string `json:"podName"`
	Status         Status `json:"status"`
	Description    string `json:"description"`
}

type Status string

const (
	StatusRunning      Status = "RUNNING"
	StatusFailed       Status = "FAILED"
	StatusSuccess      Status = "SUCCESS"
	StatusCreated      Status = "CREATED"
	StatusCreateFailed Status = "CREATE_FAILED"
	StatusInitTimeout  Status = "INIT_TIMEOUT"
	StatusPending      Status = "PENDING"
	StatusUnknown      Status = "UNKNOWN"
)

type TaskConfiguration struct {
	Config  map[string]any `json:"config"`
	Catalog map[string]any `json:"catalog"`
	State   map[string]any `json:"state"`
}

func (t *TaskConfiguration) IsEmpty() bool {
	return t == nil && t.Config == nil && t.Catalog == nil && t.State == nil
}

func (t *TaskConfiguration) ToMap() map[string]string {
	if t == nil {
		return nil
	}
	m := map[string]string{}
	if t.Config != nil {
		config, _ := json.Marshal(t.Config)
		m["config"] = string(config)
	}
	if t.Catalog != nil {
		catalog, _ := json.Marshal(t.Catalog)
		m["catalog"] = string(catalog)
	}
	if t.State != nil {
		state, _ := json.Marshal(t.State)
		m["state"] = string(state)
	}
	return m
}
