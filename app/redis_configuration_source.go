package app

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/utils"
	"sync"
	"time"
)

const redisDestinationsKey = "bulkerExportDestinations"
const redisConfigurationSourceServiceName = "redis_configuration_source"

type RedisConfigurationSource struct {
	sync.Mutex
	currentHash uint64
	refreshChan chan bool
	changesChan chan bool

	redisPool    *redis.Pool
	config       map[string]any
	destinations map[string]*DestinationConfig
}

func NewRedisConfigurationSource(redisURL string) (*RedisConfigurationSource, error) {
	logging.Debugf("[%s] Creating RedisConfigurationSource with redisURL: %s", redisConfigurationSourceServiceName, redisURL)
	redisPool := newPool(redisURL)
	r := RedisConfigurationSource{
		redisPool:    redisPool,
		refreshChan:  make(chan bool, 1),
		changesChan:  make(chan bool, 1),
		config:       make(map[string]any),
		destinations: make(map[string]*DestinationConfig),
	}
	err := r.init()
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func (rcs *RedisConfigurationSource) init() error {
	conn := rcs.redisPool.Get()
	defer conn.Close()
	_, err := conn.Do("CONFIG", "SET", "notify-keyspace-events", "Kg")
	if err != nil {
		return fmt.Errorf("failed to set 'notify-keyspace-events' config setting: %v", err)
	}
	err = rcs.load(false)
	if err != nil {
		return fmt.Errorf("failed to load initial config: %v", err)
	}
	go rcs.pubsub()
	go rcs.refresh()

	return nil
}

func (rcs *RedisConfigurationSource) pubsub() {
	redisPubSubChannel := "__keyspace@0__:" + redisDestinationsKey
	for {
		select {
		case refresh := <-rcs.refreshChan:
			if !refresh {
				//closed channel. exiting goroutine
				logging.Infof("[%s] Closed Redis Pub/Sub channel: %s", redisConfigurationSourceServiceName, redisPubSubChannel)
				return
			}
		default:
		}
		logging.Infof("[%s] Starting Redis Pub/Sub channel: %s", redisConfigurationSourceServiceName, redisPubSubChannel)
		pubSubConn := rcs.redisPool.Get()
		// Subscribe to the channel
		psc := redis.PubSubConn{Conn: pubSubConn}
		err := psc.Subscribe(redisPubSubChannel)
		if err != nil {
			_ = psc.Unsubscribe(redisPubSubChannel)
			_ = pubSubConn.Close()
			logging.Errorf("[%s] Failed to subscribe to Redis Pub/Sub channel %s: %v", redisConfigurationSourceServiceName, redisPubSubChannel, err)
			time.Sleep(10 * time.Second)
			continue
		}
	loop:
		for {
			switch v := psc.Receive().(type) {
			case redis.Message:
				logging.Infof("[%s] %s: message: %s", redisConfigurationSourceServiceName, v.Channel, v.Data)
				rcs.refreshChan <- true
			case redis.Subscription:
				logging.Infof("[%s] %s: %s %d", redisConfigurationSourceServiceName, v.Channel, v.Kind, v.Count)
				if v.Kind == "subscribe" {
					rcs.refreshChan <- true
				}
			case error:
				logging.Errorf("[%s] Redis PubSub error.: %v", redisConfigurationSourceServiceName, v)
				break loop
			}
		}
		_ = psc.Unsubscribe(redisPubSubChannel)
		_ = pubSubConn.Close()

	}
}

func (rcs *RedisConfigurationSource) refresh() {
	for range rcs.refreshChan {
		err := rcs.load(true)
		if err != nil {
			logging.Errorf("[%s] Failed to refresh RedisConfigurationSource: %v", redisConfigurationSourceServiceName, err)
		}
	}
}

func (rcs *RedisConfigurationSource) load(notify bool) error {
	logging.Debugf("[%s] Loading RedisConfigurationSource", redisConfigurationSourceServiceName)
	conn := rcs.redisPool.Get()
	defer conn.Close()
	configsById, err := redis.StringMap(conn.Do("HGETALL", redisDestinationsKey))
	if err != nil {
		return fmt.Errorf("failed to set 'notify-keyspace-events' config setting: %v", err)
	}
	newHash, err := utils.HashAny(configsById)
	if err != nil {
		return fmt.Errorf("failed generate hash of redis config: %v", err)
	}
	if newHash == rcs.currentHash {
		logging.Infof("[%s] RedisConfigurationSource: no changes", redisConfigurationSourceServiceName)
		return nil
	}
	newDsts := make(map[string]*DestinationConfig, len(configsById))
	for id, config := range configsById {
		dstCfg := DestinationConfig{}
		err := utils.ParseObject(config, &dstCfg)
		if err != nil {
			logging.Errorf("[%s] failed to parse destination config %s: %v", id, config, err)
		} else {
			logging.Infof("[%s] parsed destination config: %+v", id, dstCfg)
			newDsts[id] = &dstCfg
		}
	}
	rcs.Lock()
	rcs.destinations = newDsts
	rcs.currentHash = newHash
	rcs.Unlock()
	if notify {
		select {
		case rcs.changesChan <- true:
			logging.Infof("[%s] RedisConfigurationSource: changes detected", redisConfigurationSourceServiceName)
			//notify listener if it is listening
		default:
		}
	}
	return nil
}

func newPool(redisURL string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		// Dial or DialContext must be set. When both are set, DialContext takes precedence over Dial.
		Dial: func() (redis.Conn, error) { return redis.DialURL(redisURL) },
	}
}

func (rcs *RedisConfigurationSource) GetDestinationConfig(id string) *DestinationConfig {
	rcs.Lock()
	defer rcs.Unlock()
	return rcs.destinations[id]
}

func (rcs *RedisConfigurationSource) GetValue(key string) any {
	rcs.Lock()
	defer rcs.Unlock()
	return rcs.config[key]
}

func (rcs *RedisConfigurationSource) GetDestinationConfigs() []*DestinationConfig {
	rcs.Lock()
	defer rcs.Unlock()
	dstConfigs := make([]*DestinationConfig, 0, len(rcs.destinations))
	for _, dstCfg := range rcs.destinations {
		dstConfigs = append(dstConfigs, dstCfg)
	}
	return dstConfigs
}

func (rcs *RedisConfigurationSource) Close() error {
	close(rcs.refreshChan)
	return rcs.redisPool.Close()
}

func (rcs *RedisConfigurationSource) ChangesChannel() <-chan bool {
	return rcs.changesChan
}
