package main

import (
	"github.com/jitsucom/bulker/jitsubase/types"
)

type IngestType string

const (
	IngestTypeS2S     IngestType = "s2s"
	IngestTypeBrowser IngestType = "browser"
	// type of writeKey defines the type of ingest
	IngestTypeWriteKeyDefined IngestType = "writeKey"

	ConnectionIdsHeader = "connection_ids"
)

type IngestMessageOrigin struct {
	BaseURL    string `json:"baseUrl,omitempty"`
	Slug       string `json:"slug,omitempty"`
	SourceId   string `json:"sourceId,omitempty"`
	SourceName string `json:"sourceName,omitempty"`
	Domain     string `json:"domain,omitempty"`
	Classic    bool   `json:"classic,omitempty"`
}

type IngestMessage struct {
	IngestType     IngestType          `json:"ingestType"`
	MessageCreated string              `json:"messageCreated"`
	WriteKey       string              `json:"writeKey,omitempty"`
	MessageId      string              `json:"messageId"`
	Type           string              `json:"type"`
	Origin         IngestMessageOrigin `json:"origin"`
	HttpHeaders    map[string]string   `json:"httpHeaders"`
	HttpPayload    types.Json          `json:"httpPayload"`
}