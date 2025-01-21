package main

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type KVStore struct {
	mu       sync.RWMutex
	data     map[string]*KeyValue
	config   *ServerConfig
	metrics  *Metrics
	snapshot *SnapshotManager
}

type KeyValue struct {
	Type      string
	Value     []byte
	Hash      map[string]*HashField
	CreatedAt time.Time
	UpdatedAt time.Time
	ExpiresAt time.Time
}

func NewKVStore(config *ServerConfig, metrics *Metrics) *KVStore {
	if config.SnapshotInterval <= 0 {
		panic("SnapshotInterval must be greater than 0")
	}
	kv := &KVStore{
		data:    make(map[string]*KeyValue),
		config:  config,
		metrics: metrics,
	}
	kv.snapshot = NewSnapshotManager(kv, config)
	go kv.cleanupLoop()
	return kv
}

func (kv *KVStore) Set(key string, value []byte, ttl time.Duration) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	expiresAt := time.Time{}
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	kv.data[key] = &KeyValue{
		Value:     value,
		ExpiresAt: expiresAt,
	}

	atomic.AddUint64(&kv.metrics.KeyCount, 1)
	return nil
}

func (kv *KVStore) Get(key string) ([]byte, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	val, exists := kv.data[key]
	if !exists {
		return nil, false
	}

	if !val.ExpiresAt.IsZero() && time.Now().After(val.ExpiresAt) {
		return nil, false
	}

	return val.Value, true
}

func (kv *KVStore) Delete(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, exists := kv.data[key]; exists {
		delete(kv.data, key)
		atomic.AddUint64(&kv.metrics.KeyCount, ^uint64(0))
		return true
	}
	return false
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

func NewSnapshotManager(kv *KVStore, config *ServerConfig) *SnapshotManager {
	if config.SnapshotInterval <= 0 {
		panic("SnapshotInterval must be greater than 0")
	}
	sm := &SnapshotManager{
		kv:     kv,
		config: config,
	}
	go sm.autoSnapshotLoop()
	return sm
}

func (sm *SnapshotManager) autoSnapshotLoop() {
	ticker := time.NewTicker(sm.config.SnapshotInterval)
	defer ticker.Stop()

	for range ticker.C {
		if err := sm.CreateSnapshot(); err != nil {

			continue
		}
	}
}

func (sm *SnapshotManager) CreateSnapshot() error {

	file, err := os.CreateTemp(sm.config.DataDir, "snapshot-*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer file.Close()

	sm.kv.mu.RLock()
	defer sm.kv.mu.RUnlock()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(sm.kv.data); err != nil {
		return fmt.Errorf("failed to encode data: %w", err)
	}

	finalPath := fmt.Sprintf("%s/snapshot-%d.db", sm.config.DataDir, time.Now().UnixNano())
	if err := os.Rename(file.Name(), finalPath); err != nil {
		return fmt.Errorf("failed to rename snapshot: %w", err)
	}

	return sm.cleanupOldSnapshots()
}

func (sm *SnapshotManager) LoadLatestSnapshot() error {
	entries, err := os.ReadDir(sm.config.DataDir)
	if err != nil {
		return fmt.Errorf("failed to read data directory: %w", err)
	}

	var latestFile string
	var latestTime int64

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if matched, _ := filepath.Match("snapshot-*.db", entry.Name()); !matched {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		if info.ModTime().UnixNano() > latestTime {
			latestFile = entry.Name()
			latestTime = info.ModTime().UnixNano()
		}
	}

	if latestFile == "" {
		return nil
	}

	file, err := os.Open(filepath.Join(sm.config.DataDir, latestFile))
	if err != nil {
		return fmt.Errorf("failed to open snapshot: %w", err)
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	sm.kv.mu.Lock()
	defer sm.kv.mu.Unlock()

	return decoder.Decode(&sm.kv.data)
}

func (sm *SnapshotManager) cleanupOldSnapshots() error {
	entries, err := os.ReadDir(sm.config.DataDir)
	if err != nil {
		return err
	}

	var snapshots []string
	for _, entry := range entries {
		if matched, _ := filepath.Match("snapshot-*.db", entry.Name()); matched {
			snapshots = append(snapshots, entry.Name())
		}
	}

	if len(snapshots) <= 3 {
		return nil
	}

	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i] > snapshots[j]
	})

	for _, snapshot := range snapshots[3:] {
		if err := os.Remove(filepath.Join(sm.config.DataDir, snapshot)); err != nil {

			continue
		}
	}

	return nil
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
