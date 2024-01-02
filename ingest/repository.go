package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/safego"
	jsoniter "github.com/json-iterator/go"
	"io"
	"os"
	"path"
	"sync/atomic"
	"time"
)

const SQLLastUpdatedQuery = `select * from last_updated`
const SQLQuery = `select * from streams_with_destinations`

type Repository struct {
	appbase.Service
	dbpool           *pgxpool.Pool
	refreshPeriodSec int
	inited           atomic.Bool
	cacheDir         string
	apiKeyBindings   atomic.Pointer[map[string]*ApiKeyBinding]
	streamsByIds     atomic.Pointer[map[string]*StreamWithDestinations]
	streamsByDomains atomic.Pointer[map[string][]*StreamWithDestinations]
	lastModified     atomic.Pointer[time.Time]
	closed           chan struct{}
}

type RepositoryCache struct {
	ApiKeyBindings   map[string]*ApiKeyBinding            `json:"apiKeyBindings"`
	StreamsByIds     map[string]*StreamWithDestinations   `json:"streamsByIds"`
	StreamsByDomains map[string][]*StreamWithDestinations `json:"streamsByDomains"`
}

func NewRepository(dbpool *pgxpool.Pool, refreshPeriodSec int, cacheDir string) *Repository {
	base := appbase.NewServiceBase("repository")
	r := &Repository{
		Service:          base,
		dbpool:           dbpool,
		refreshPeriodSec: refreshPeriodSec,
		cacheDir:         cacheDir,
		closed:           make(chan struct{}),
	}
	r.refresh()
	r.start()
	return r
}

func (r *Repository) loadCached() {
	file, err := os.Open(path.Join(r.cacheDir, "repository.json"))
	if err != nil {
		r.Fatalf("Error opening cached repository: %v\nCannot serve without repository. Exitting...", err)
		return
	}
	stat, err := file.Stat()
	if err != nil {
		r.Fatalf("Error getting cached repository info: %v\nCannot serve without repository. Exitting...", err)
		return
	}
	fileSize := stat.Size()
	if fileSize == 0 {
		r.Fatalf("Cached repository is empty\nCannot serve without repository. Exitting...")
		return
	}
	payload, err := io.ReadAll(file)
	if err != nil {
		r.Fatalf("Error reading cached script: %v\nCannot serve without repository. Exitting...", err)
		return
	}
	repositoryCache := RepositoryCache{}
	err = jsoniter.Unmarshal(payload, &repositoryCache)
	if err != nil {
		r.Fatalf("Error unmarshalling cached repository: %v\nCannot serve without repository. Exitting...", err)
		return
	}
	r.apiKeyBindings.Store(&repositoryCache.ApiKeyBindings)
	r.streamsByIds.Store(&repositoryCache.StreamsByIds)
	r.streamsByDomains.Store(&repositoryCache.StreamsByDomains)
	r.inited.Store(true)
	r.Infof("Loaded cached repository data: %d bytes, last modified: %v", fileSize, stat.ModTime())
}

func (r *Repository) storeCached(payload RepositoryCache) {
	filePath := path.Join(r.cacheDir, "repository.json")
	err := os.MkdirAll(r.cacheDir, 0755)
	if err != nil {
		r.Errorf("Cannot write cached repository to %s: cannot make dir: %v", filePath, err)
		return
	}
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		r.Errorf("Cannot write cached repository to %s: %v", filePath, err)
		return
	}
	err = json.NewEncoder(file).Encode(payload)
	if err != nil {
		r.Errorf("Cannot write cached repository to %s: %v", filePath, err)
		return
	}
	err = file.Sync()
	if err != nil {
		r.Errorf("Cannot write cached script to %s: %v", filePath, err)
		return
	}
}

func (r *Repository) refresh() {
	start := time.Now()
	apiKeyBindings := map[string]*ApiKeyBinding{}
	streamsByIds := map[string]*StreamWithDestinations{}
	streamsByDomains := map[string][]*StreamWithDestinations{}
	var err error
	defer func() {
		if err != nil {
			r.Errorf("Error refreshing repository: %v", err)
			RepositoryErrors().Add(1)
			if !r.inited.Load() {
				if r.cacheDir != "" {
					r.loadCached()
				} else {
					r.Fatalf("Cannot load cached repository. No CACHE_DIR is set. Cannot serve without repository. Exitting...")
				}
			}
		} else {
			r.Debugf("Refreshed in %v", time.Now().Sub(start))
		}
	}()
	ifModifiedSince := r.lastModified.Load()
	var lastModified time.Time
	err = r.dbpool.QueryRow(context.Background(), SQLLastUpdatedQuery).Scan(&lastModified)
	if err != nil {
		err = r.NewError("Error querying last updated: %v", err)
		return
	} else if errors.Is(err, pgx.ErrNoRows) || lastModified.IsZero() {
		//Failed to load repository last updated date. Probably database has no records yet.
		r.apiKeyBindings.Store(&apiKeyBindings)
		r.streamsByIds.Store(&streamsByIds)
		r.streamsByDomains.Store(&streamsByDomains)
		r.inited.Store(true)
		return
	}
	if ifModifiedSince != nil && lastModified.Compare(*ifModifiedSince) <= 0 {
		return
	}
	r.Infof("Config updated: %s previous update date: %s`", lastModified, ifModifiedSince)

	rows, err := r.dbpool.Query(context.Background(), SQLQuery)
	if err != nil {
		err = r.NewError("Error querying streams: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var streamId string
		var streamConfig string
		err = rows.Scan(&streamId, &streamConfig)
		if err != nil {
			err = r.NewError("Error scanning row: %v", err)
			return
		}
		//r.Infof("Stream %s: %s", streamId, streamConfig)
		s := StreamWithDestinations{}
		err = jsoniter.UnmarshalFromString(streamConfig, &s)
		if err != nil {
			err = r.NewError("Error unmarshalling stream config: %v", err)
			return
		}
		s.init()
		streamsByIds[s.Stream.Id] = &s
		for _, domain := range s.Stream.Domains {
			streams, ok := streamsByDomains[domain]
			if !ok {
				streams = make([]*StreamWithDestinations, 0, 1)
			}
			streamsByDomains[domain] = append(streams, &s)
		}
		for _, key := range s.Stream.PublicKeys {
			apiKeyBindings[key.Id] = &ApiKeyBinding{
				Hash:     key.Hash,
				KeyType:  "browser",
				StreamId: s.Stream.Id,
			}
		}
		for _, key := range s.Stream.PrivateKeys {
			apiKeyBindings[key.Id] = &ApiKeyBinding{
				Hash:     key.Hash,
				KeyType:  "s2s",
				StreamId: s.Stream.Id,
			}
		}
	}
	r.apiKeyBindings.Store(&apiKeyBindings)
	r.streamsByIds.Store(&streamsByIds)
	r.streamsByDomains.Store(&streamsByDomains)
	r.inited.Store(true)
	r.lastModified.Store(&lastModified)
	if r.cacheDir != "" {
		r.storeCached(RepositoryCache{ApiKeyBindings: apiKeyBindings, StreamsByIds: streamsByIds, StreamsByDomains: streamsByDomains})
	}
}

func (r *Repository) start() {
	safego.RunWithRestart(func() {
		ticker := time.NewTicker(time.Duration(r.refreshPeriodSec) * time.Second)
		for {
			select {
			case <-ticker.C:
				r.refresh()
			case <-r.closed:
				return
			}
		}
	})
}

func (r *Repository) Close() {
	close(r.closed)
}

func (r *Repository) getStreamByKeyId(keyId string) (*ApiKeyBinding, error) {
	binding := (*r.apiKeyBindings.Load())[keyId]
	return binding, nil
}

func (r *Repository) GetStreamById(slug string) (*StreamWithDestinations, error) {
	stream := (*r.streamsByIds.Load())[slug]
	return stream, nil
}

func (r *Repository) GetStreamsByDomain(domain string) ([]*StreamWithDestinations, error) {
	streams := (*r.streamsByDomains.Load())[domain]
	return streams, nil
}

type DataLayout string

const (
	DataLayoutSegmentCompatible  = "segment-compatible"
	DataLayoutSegmentSingleTable = "segment-single-table"
	DataLayoutJitsuLegacy        = "jitsu-legacy"
)

type ApiKey struct {
	Id        string `json:"id"`
	Plaintext string `json:"plaintext"`
	Hash      string `json:"hash"`
	Hint      string `json:"hint"`
}

type ApiKeyBinding struct {
	Hash     string `json:"hash"`
	KeyType  string `json:"keyType"`
	StreamId string `json:"streamId"`
}

type StreamConfig struct {
	Id                          string   `json:"id"`
	Type                        string   `json:"type"`
	WorkspaceId                 string   `json:"workspaceId"`
	Slug                        string   `json:"slug"`
	Name                        string   `json:"name"`
	Domains                     []string `json:"domains"`
	AuthorizedJavaScriptDomains string   `json:"authorizedJavaScriptDomains"`
	PublicKeys                  []ApiKey `json:"publicKeys"`
	PrivateKeys                 []ApiKey `json:"privateKeys"`
	DataLayout                  string   `json:"dataLayout"`
}

type ShortDestinationConfig struct {
	TagDestinationConfig
	Id              string         `json:"id"`
	ConnectionId    string         `json:"connectionId"`
	DestinationType string         `json:"destinationType"`
	Options         map[string]any `json:"options,omitempty"`
	Credentials     map[string]any `json:"credentials,omitempty"`
}

type TagDestinationConfig struct {
	Mode string `json:"mode,omitempty"`
	Code string `json:"code,omitempty"`
}

type StreamWithDestinations struct {
	Stream                   StreamConfig             `json:"stream"`
	Deleted                  bool                     `json:"deleted"`
	UpdateAt                 time.Time                `json:"updatedAt"`
	BackupEnabled            bool                     `json:"backupEnabled"`
	Destinations             []ShortDestinationConfig `json:"destinations"`
	SynchronousDestinations  []*ShortDestinationConfig
	AsynchronousDestinations []*ShortDestinationConfig
}

func (s *StreamWithDestinations) init() {
	s.SynchronousDestinations = make([]*ShortDestinationConfig, 0)
	s.AsynchronousDestinations = make([]*ShortDestinationConfig, 0)
	for _, d := range s.Destinations {
		if d.Id == "" || d.DestinationType == "" {
			continue
		}
		_, ok := DeviceOptions[d.DestinationType]
		if ok {
			s.SynchronousDestinations = append(s.SynchronousDestinations, &d)
		} else {
			s.AsynchronousDestinations = append(s.AsynchronousDestinations, &d)
		}
	}
}
