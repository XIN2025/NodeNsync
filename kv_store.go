package main

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type KVStore struct {
	mu                 sync.RWMutex
	data               map[string]*KeyValue
	config             *ServerConfig
	metrics            *Metrics
	snapshot           *SnapshotManager
	replicationManager *ReplicationManager
}

type KeyValue struct {
	Type      string
	Value     []byte
	Hash      map[string]*HashField
	CreatedAt time.Time
	UpdatedAt time.Time
	ExpiresAt time.Time

	mu                 sync.RWMutex
	data               map[string]*KeyValue
	replicationManager *ReplicationManager
}

func NewKVStore(config *ServerConfig, metrics *Metrics, replicationManager *ReplicationManager) *KVStore {
	if config.SnapshotInterval <= 0 {
		panic("SnapshotInterval must be greater than 0")
	}
	kv := &KVStore{
		data:               make(map[string]*KeyValue),
		config:             config,
		metrics:            metrics,
		replicationManager: replicationManager,
	}
	kv.snapshot = NewSnapshotManager(kv, config)
	go kv.cleanupLoop()
	return kv
}

func (kv *KVStore) ApplyReplicationCommand(cmd ReplicationCommand) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch cmd.Operation {
	case "SET":
		kv.data[cmd.Key] = &KeyValue{Value: cmd.Value}
	case "DEL":
		delete(kv.data, cmd.Key)
	}
}

func (kv *KVStore) Set(key string, value []byte, ttl time.Duration) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	expiresAt := time.Time{}
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	kv.data[key] = &KeyValue{
		Type:      "string",
		Value:     value,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		ExpiresAt: expiresAt,
	}

	slog.Info("Set key in KV store", "key", key, "value", value)
	atomic.AddUint64(&kv.metrics.KeyCount, 1)

	if kv.replicationManager != nil {
		kv.replicationManager.BroadcastToReplicas(ReplicationCommand{
			Operation: "SET",
			Key:       key,
			Value:     value,
			TTL:       ttl,
		})
	}

	return nil
}

func (kv *KVStore) PrintStore() {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	slog.Info("Key-Value Store Contents:")
	for key, value := range kv.data {
		slog.Info("Key-Value Pair", "key", key, "value", string(value.Value))
	}
}

func (kv *KVStore) Delete(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, exists := kv.data[key]; exists {
		delete(kv.data, key)
		atomic.AddUint64(&kv.metrics.KeyCount, ^uint64(0))

		if kv.replicationManager != nil {
			kv.replicationManager.BroadcastToReplicas(ReplicationCommand{
				Operation: "DEL",
				Key:       key,
			})
		}

		return true
	}
	return false
}

func (kv *KVStore) Get(key string) ([]byte, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	val, exists := kv.data[key]
	if !exists {
		return nil, false
	}

	slog.Info("Retrieved key from KV store", "key", key, "value", val.Value)
	if !val.ExpiresAt.IsZero() && time.Now().After(val.ExpiresAt) {
		return nil, false
	}

	return val.Value, true
}

func (kv *KVStore) cleanupLoop() {
	ticker := time.NewTicker(kv.config.CleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		kv.cleanupExpired()
	}
}

func (kv *KVStore) cleanupExpired() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	now := time.Now()
	for key, value := range kv.data {
		if !value.ExpiresAt.IsZero() && now.After(value.ExpiresAt) {
			delete(kv.data, key)
			atomic.AddUint64(&kv.metrics.KeyCount, ^uint64(0))
		}
	}
}

type SnapshotManager struct {
	kv     *KVStore
	config *ServerConfig
}

type Monitor struct {
	mu       sync.RWMutex
	watchers map[*Peer]bool
	msgCh    chan string
}

func NewMonitor() *Monitor {
	m := &Monitor{
		watchers: make(map[*Peer]bool),
		msgCh:    make(chan string, 1000),
	}
	go m.broadcastLoop()
	return m
}

func (m *Monitor) AddWatcher(peer *Peer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.watchers[peer] = true
}

func (m *Monitor) RemoveWatcher(peer *Peer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.watchers, peer)
}

func (m *Monitor) Record(msg string) {
	select {
	case m.msgCh <- msg:
	default:

	}
}

func (m *Monitor) broadcastLoop() {
	for msg := range m.msgCh {
		m.mu.RLock()
		for peer := range m.watchers {
			select {
			case peer.monitorCh <- msg:
			default:

				m.RemoveWatcher(peer)
			}
		}
		m.mu.RUnlock()
	}
}
