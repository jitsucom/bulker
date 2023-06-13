package app

import (
	"github.com/gomodule/redigo/redis"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	jsoniter "github.com/json-iterator/go"
	"strings"
)

const fastStoreServiceName = "fast_store"

const fastStoreStreamIdsKey = "streamIds"
const fastStoreApiKeys = "apiKeys"
const fastStoreStreamDomainsKey = "streamDomains"

type FastStore struct {
	appbase.Service
	redisPool *redis.Pool
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
	PrivateKeys                 []ApiKey `json:"privateKeys""`
	DataLayout                  string   `json:"dataLayout"`
}

type ShortDestinationConfig struct {
	TagDestinationConfig
	Id              string `json:"id"`
	ConnectionId    string `json:"connectionId"`
	DestinationType string `json:"destinationType"`
}

type TagDestinationConfig struct {
	Mode string `json:"mode"`
	Code string `json:"code"`
}

type StreamWithDestinations struct {
	Stream                   StreamConfig             `json:"stream"`
	SynchronousDestinations  []ShortDestinationConfig `json:"synchronousDestinations"`
	AsynchronousDestinations []ShortDestinationConfig `json:"asynchronousDestinations"`
}

func NewFastStore(config *Config) (*FastStore, error) {
	base := appbase.NewServiceBase(fastStoreServiceName)
	base.Debugf("Creating FastStore with redisURL: %s", config.RedisURL)
	redisPool := newPool(config.RedisURL, config.RedisTLSCA)
	fs := FastStore{
		Service:   base,
		redisPool: redisPool,
	}
	return &fs, nil
}

func (fs *FastStore) getStreamByKeyId(keyId string) (*ApiKeyBinding, error) {
	connection := fs.redisPool.Get()
	defer connection.Close()

	keyBytes, err := redis.Bytes(connection.Do("HGET", fastStoreApiKeys, keyId))
	if err == redis.ErrNil {
		return nil, nil
	}
	if err != nil {
		return nil, fs.NewError("failed to get stream binding by keyId: %w", err)
	}
	binding := ApiKeyBinding{}
	err = jsoniter.Unmarshal(keyBytes, &binding)
	if err != nil {
		return nil, fs.NewError("failed to unmarshal binding bytes for keyId [%s]: %w: %s", keyId, err, string(keyBytes))
	}
	return &binding, nil
}

func (fs *FastStore) GetStreamById(slug string) (*StreamWithDestinations, error) {
	connection := fs.redisPool.Get()
	defer connection.Close()

	streamBytes, err := redis.Bytes(connection.Do("HGET", fastStoreStreamIdsKey, slug))
	if err == redis.ErrNil {
		return nil, nil
	}
	if err != nil {
		return nil, fs.NewError("failed to get stream by slug [%s]: %w", slug, err)
	}
	stream := StreamWithDestinations{}
	err = jsoniter.Unmarshal(streamBytes, &stream)
	if err != nil {
		return nil, fs.NewError("failed to unmarshal stream bytes for slug [%s]: %w: %s", slug, err, string(streamBytes))
	}
	return &stream, nil
}

func (fs *FastStore) GetStreamsByDomain(domain string) ([]StreamWithDestinations, error) {
	connection := fs.redisPool.Get()
	defer connection.Close()

	domain = strings.ToLower(domain)

	streamBytes, err := redis.Bytes(connection.Do("HGET", fastStoreStreamDomainsKey, domain))
	if err == redis.ErrNil {
		return nil, nil
	}
	if err != nil {
		return nil, fs.NewError("failed to get stream by domain [%s]: %w", domain, err)
	}
	stream := make([]StreamWithDestinations, 0, 2)
	err = jsoniter.Unmarshal(streamBytes, &stream)
	if err != nil {
		return nil, fs.NewError("failed to unmarshal stream bytes for domain [%s]: %w: %s", domain, err, string(streamBytes))
	}
	return stream, nil
}

func (fs *FastStore) Close() error {
	fs.redisPool.Close()
	return nil
}
