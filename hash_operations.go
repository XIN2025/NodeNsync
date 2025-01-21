package main

import (
	"sync/atomic"
	"time"
)

type HashOperations struct {
	kv      *KVStore
	metrics *Metrics
	monitor *Monitor
}

type HashField struct {
	Field     string
	Value     []byte
	UpdatedAt time.Time
}

type HashData struct {
	Fields    map[string]*HashField
	CreatedAt time.Time
	UpdatedAt time.Time
}

func NewHashOperations(kv *KVStore, metrics *Metrics, monitor *Monitor) *HashOperations {
	return &HashOperations{
		kv:      kv,
		metrics: metrics,
		monitor: monitor,
	}
}

func (ho *HashOperations) HSet(key string, fields map[string][]byte) (int, error) {
	start := time.Now()
	defer func() {
		ho.metrics.RecordLatency("HSET", time.Since(start))
	}()

	ho.kv.mu.Lock()
	defer ho.kv.mu.Unlock()

	hash, exists := ho.kv.data[key]
	if !exists || hash.Type != "hash" {
		hash = &KeyValue{
			Type:      "hash",
			Hash:      make(map[string]*HashField),
			CreatedAt: time.Now(),
		}
		ho.kv.data[key] = hash
		atomic.AddUint64(&ho.metrics.KeyCount, 1)
	}

	newFields := 0
	now := time.Now()

	for field, value := range fields {
		_, existed := hash.Hash[field]
		hash.Hash[field] = &HashField{
			Field:     field,
			Value:     value,
			UpdatedAt: now,
		}
		if !existed {
			newFields++
		}
	}

	hash.UpdatedAt = now
	return newFields, nil
}

func (ho *HashOperations) HGet(key string, field string) ([]byte, bool) {
	start := time.Now()
	defer func() {
		ho.metrics.RecordLatency("HGET", time.Since(start))
	}()

	ho.kv.mu.RLock()
	defer ho.kv.mu.RUnlock()

	hash, exists := ho.kv.data[key]
	if !exists || hash.Type != "hash" {
		return nil, false
	}

	hashField, exists := hash.Hash[field]
	if !exists {
		return nil, false
	}

	return hashField.Value, true
}

func (ho *HashOperations) HGetAll(key string) (map[string][]byte, bool) {
	start := time.Now()
	defer func() {
		ho.metrics.RecordLatency("HGETALL", time.Since(start))
	}()

	ho.kv.mu.RLock()
	defer ho.kv.mu.RUnlock()

	hash, exists := ho.kv.data[key]
	if !exists || hash.Type != "hash" {
		return nil, false
	}

	result := make(map[string][]byte, len(hash.Hash))
	for field, hashField := range hash.Hash {
		result[field] = hashField.Value
	}

	return result, true
}

func (ho *HashOperations) HDel(key string, fields ...string) int {
	start := time.Now()
	defer func() {
		ho.metrics.RecordLatency("HDEL", time.Since(start))
	}()

	ho.kv.mu.Lock()
	defer ho.kv.mu.Unlock()

	hash, exists := ho.kv.data[key]
	if !exists || hash.Type != "hash" {
		return 0
	}

	deleted := 0
	for _, field := range fields {
		if _, exists := hash.Hash[field]; exists {
			delete(hash.Hash, field)
			deleted++
		}
	}

	if len(hash.Hash) == 0 {
		delete(ho.kv.data, key)
		atomic.AddUint64(&ho.metrics.KeyCount, ^uint64(0))
	}

	return deleted
}

func (ho *HashOperations) HExists(key string, field string) bool {
	start := time.Now()
	defer func() {
		ho.metrics.RecordLatency("HEXISTS", time.Since(start))
	}()

	ho.kv.mu.RLock()
	defer ho.kv.mu.RUnlock()

	hash, exists := ho.kv.data[key]
	if !exists || hash.Type != "hash" {
		return false
	}

	_, exists = hash.Hash[field]
	return exists
}

func (ho *HashOperations) HLen(key string) int {
	start := time.Now()
	defer func() {
		ho.metrics.RecordLatency("HLEN", time.Since(start))
	}()

	ho.kv.mu.RLock()
	defer ho.kv.mu.RUnlock()

	hash, exists := ho.kv.data[key]
	if !exists || hash.Type != "hash" {
		return 0
	}

	return len(hash.Hash)
}

func (ho *HashOperations) HKeys(key string) []string {
	start := time.Now()
	defer func() {
		ho.metrics.RecordLatency("HKEYS", time.Since(start))
	}()

	ho.kv.mu.RLock()
	defer ho.kv.mu.RUnlock()

	hash, exists := ho.kv.data[key]
	if !exists || hash.Type != "hash" {
		return nil
	}

	keys := make([]string, 0, len(hash.Hash))
	for field := range hash.Hash {
		keys = append(keys, field)
	}
	return keys
}

func (ho *HashOperations) HVals(key string) [][]byte {
	start := time.Now()
	defer func() {
		ho.metrics.RecordLatency("HVALS", time.Since(start))
	}()

	ho.kv.mu.RLock()
	defer ho.kv.mu.RUnlock()

	hash, exists := ho.kv.data[key]
	if !exists || hash.Type != "hash" {
		return nil
	}

	values := make([][]byte, 0, len(hash.Hash))
	for _, hashField := range hash.Hash {
		values = append(values, hashField.Value)
	}
	return values
}
