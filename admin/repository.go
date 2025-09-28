package main

import (
	"encoding/json"
	"fmt"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"io"
	"sync/atomic"
	"time"
)

type RepositoryConfig struct {
	RepositoryURL              string `mapstructure:"REPOSITORY_URL"`
	RepositoryAuthToken        string `mapstructure:"REPOSITORY_AUTH_TOKEN"`
	RepositoryRefreshPeriodSec int    `mapstructure:"REPOSITORY_REFRESH_PERIOD_SEC" default:"2"`
}

func (r *RepositoryConfig) PostInit(settings *appbase.AppSettings) error {
	return nil
}

type Streams struct {
	streams                   []*StreamWithDestinations
	streamsByPlainKeyOrIds    map[string]*StreamWithDestinations
	lastModified              time.Time
}

func (s *Streams) GetStreamByPlainKeyOrId(plainKeyOrSlug string) *StreamWithDestinations {
	return s.streamsByPlainKeyOrIds[plainKeyOrSlug]
}

func (s *Streams) GetStreams() []*StreamWithDestinations {
	return s.streams
}

type StreamsRepositoryData struct {
	data atomic.Pointer[Streams]
}

func (s *StreamsRepositoryData) Init(reader io.Reader, tag any) error {
	dec := json.NewDecoder(reader)
	// read open bracket
	_, err := dec.Token()
	if err != nil {
		return fmt.Errorf("error reading open bracket: %v", err)
	}
	streams := make([]*StreamWithDestinations, 0)
	streamsByPlainKeyOrIds := map[string]*StreamWithDestinations{}
	
	// while the array contains values
	for dec.More() {
		swd := StreamWithDestinations{}
		err = dec.Decode(&swd)
		if err != nil {
			return fmt.Errorf("Error unmarshalling stream config: %v", err)
		}
		swd.init()
		streams = append(streams, &swd)
		streamsByPlainKeyOrIds[swd.Stream.Id] = &swd
	}

	// read closing bracket
	_, err = dec.Token()
	if err != nil {
		return fmt.Errorf("error reading closing bracket: %v", err)
	}

	data := Streams{
		streams:                streams,
		streamsByPlainKeyOrIds: streamsByPlainKeyOrIds,
	}
	if tag != nil {
		data.lastModified = tag.(time.Time)
	}
	s.data.Store(&data)
	return nil
}

func (s *StreamsRepositoryData) GetData() *Streams {
	return s.data.Load()
}

func (s *StreamsRepositoryData) Store(writer io.Writer) error {
	d := s.data.Load()
	if d != nil {
		encoder := json.NewEncoder(writer)
		err := encoder.Encode(d.streams)
		return err
	}
	return nil
}

func NewStreamsRepository(url, token string, refreshPeriodSec int, cacheDir string) appbase.Repository[Streams] {
	return appbase.NewHTTPRepository[Streams]("streams-with-destinations", url, token, appbase.HTTPTagLastModified, &StreamsRepositoryData{}, 1, refreshPeriodSec, cacheDir)
}

type StreamConfig struct {
	Id          string `json:"id"`
	WorkspaceId string `json:"workspaceId"`
	Name        string `json:"name"`
}

type ShortDestinationConfig struct {
	Id              string         `json:"id"`
	ConnectionId    string         `json:"connectionId,omitempty"`
	DestinationType string         `json:"destinationType"`
	Options         map[string]any `json:"options,omitempty"`
}

type StreamWithDestinations struct {
	Stream                   StreamConfig             `json:"stream"`
	UpdateAt                 time.Time                `json:"updatedAt"`
	Destinations             []ShortDestinationConfig `json:"destinations"`
	AsynchronousDestinations []ShortDestinationConfig
}

func (s *StreamWithDestinations) init() {
	// For admin app, initialize asynchronous destinations from destinations
	s.AsynchronousDestinations = make([]ShortDestinationConfig, 0)
	for _, d := range s.Destinations {
		if d.Id != "" && d.DestinationType != "" {
			s.AsynchronousDestinations = append(s.AsynchronousDestinations, d)
		}
	}
}