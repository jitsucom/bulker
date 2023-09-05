package main

import (
	"encoding/json"
	"github.com/mitchellh/mapstructure"
	"time"
)

type TaskDescriptor struct {
	TaskID          string `json:"taskId"`
	TaskType        string `json:"taskType"` //spec, discover, read, check
	SyncID          string `json:"syncId"`
	SourceType      string `json:"sourceType"`
	StorageKey      string `json:"storageKey"`
	Protocol        string `json:"protocol"`
	Package         string `json:"package"`
	PackageVersion  string `json:"packageVersion"`
	TableNamePrefix string `json:"tableNamePrefix"`
	StartedBy       string `json:"startedBy"`
	StartedAt       string `json:"startedAt"`
}

func (t *TaskDescriptor) StartedAtTime() time.Time {
	tm, err := time.Parse(time.RFC3339, t.StartedAt)
	if err != nil {
		return time.Now()
	}
	return tm
}

func (t *TaskDescriptor) ExtractAnnotations() map[string]string {
	rawAnnotations := map[string]string{}
	_ = mapstructure.Decode(t, &rawAnnotations)
	annotations := make(map[string]string, len(rawAnnotations))
	for k, v := range rawAnnotations {
		if v != "" {
			annotations[k] = v
		}
	}
	return annotations
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
	State   any            `json:"state"`
}

func (t *TaskConfiguration) IsEmpty() bool {
	return t == nil || (t.Config == nil && t.Catalog == nil && t.State == nil)
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
