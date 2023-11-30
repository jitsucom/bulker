package main

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/safego"
	jsoniter "github.com/json-iterator/go"
	"sync/atomic"
	"time"
)

const SQLQuery = `select b."streamId", json_build_object('stream', b."srcConfig", 'deleted', b."deleted", 'backupEnabled', true, 'destinations', b."connectionData", 'updatedAt', to_char (b."updatedAt", 'YYYY-MM-DD"T"HH24:MI:SS"Z"') ) from
    (select
         src.id as "streamId",
         ws.id,
         src.deleted or ws.deleted as "deleted",
         (src."config" ||  jsonb_build_object('id', src.id, 'workspaceId', ws.id)) as "srcConfig",
         json_agg( case when dst.id is not null and dst.deleted = false and link.id is not null and link.deleted = false then  json_build_object('id', dst.id,
                                                                               'credentials', dst."config",
                                                                               'destinationType', dst."config"->>'destinationType',
                                                                               'connectionId', link."id",
                                                                               'options', link."data") end ) as "connectionData",
         max(greatest(link."updatedAt", src."updatedAt", dst."updatedAt", ws."updatedAt")) as "updatedAt"
     from "ConfigurationObject" src
              join "Workspace" ws on src."workspaceId" = ws.id
              left join "ConfigurationObjectLink" link on src.id = link."fromId" and link."workspaceId" = src."workspaceId"
              left join "ConfigurationObject" dst on dst.id = link."toId" and dst.type='destination' and dst."workspaceId" = link."workspaceId"
     where src."type" ='stream'
     group by 1,2) b`

type Repository struct {
	appbase.Service
	dbpool           *pgxpool.Pool
	refreshPeriodSec int
	apiKeyBindings   atomic.Pointer[map[string]*ApiKeyBinding]
	streamsByIds     atomic.Pointer[map[string]*StreamWithDestinations]
	streamsByDomains atomic.Pointer[map[string][]*StreamWithDestinations]
	closed           chan struct{}
}

func NewRepository(dbpool *pgxpool.Pool, refreshPeriodSec int) *Repository {
	base := appbase.NewServiceBase("repository")
	r := &Repository{
		Service:          base,
		dbpool:           dbpool,
		refreshPeriodSec: refreshPeriodSec,
		closed:           make(chan struct{}),
	}
	r.refresh()
	r.Start()
	return r
}

func (r *Repository) refresh() {
	start := time.Now()
	apiKeyBindings := map[string]*ApiKeyBinding{}
	streamsByIds := map[string]*StreamWithDestinations{}
	streamsByDomains := map[string][]*StreamWithDestinations{}
	defer func() {
		r.Infof("Refreshed in %v", time.Now().Sub(start))
	}()
	rows, err := r.dbpool.Query(context.Background(), SQLQuery)
	if err != nil {
		r.Errorf("Error querying streams: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var streamId string
		var streamConfig string
		err := rows.Scan(&streamId, &streamConfig)
		if err != nil {
			r.Errorf("Error scanning row: %v", err)
			continue
		}
		//r.Infof("Stream %s: %s", streamId, streamConfig)
		s := StreamWithDestinations{}
		err = jsoniter.UnmarshalFromString(streamConfig, &s)
		if err != nil {
			r.Errorf("Error unmarshalling stream config: %v", err)
			continue
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
}

func (r *Repository) Start() {
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
	Options         map[string]any `json:"options"`
}

type TagDestinationConfig struct {
	Mode string `json:"mode"`
	Code string `json:"code"`
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
		_, hasEventsOption := d.Options["events"]
		_, hasHostsOption := d.Options["hosts"]
		if hasEventsOption || hasHostsOption {
			s.SynchronousDestinations = append(s.SynchronousDestinations, &d)
		} else {
			s.AsynchronousDestinations = append(s.AsynchronousDestinations, &d)
		}
	}
}
