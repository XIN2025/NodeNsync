package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type SerializableKV struct {
	Type      string            `json:"type"`
	Value     string            `json:"value,omitempty"`
	Hash      map[string]string `json:"hash,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
	ExpiresAt time.Time         `json:"expires_at,omitempty"`
}

func (sm *SnapshotManager) autoSnapshotLoop() {
	ticker := time.NewTicker(sm.config.SnapshotInterval)
	defer ticker.Stop()

	for range ticker.C {
		slog.Info("Snapshot triggered", "time", time.Now().Format(time.RFC3339))
		if err := sm.CreateSnapshot(); err != nil {
			slog.Error("Snapshot failed", "error", err)
		}
	}
}

func NewSnapshotManager(kv *KVStore, config *ServerConfig) *SnapshotManager {
	// Ensure data directory exists
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		panic(fmt.Sprintf("Failed to create data dir: %v", err))
	}

	// Test write capability
	testFile := filepath.Join(config.DataDir, "test_write.txt")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		panic(fmt.Sprintf("Write test failed: %v", err))
	}
	defer os.Remove(testFile)
	sm := &SnapshotManager{
		kv:     kv,
		config: config,
	}
	go sm.autoSnapshotLoop()
	return sm
}

func (sm *SnapshotManager) cleanupOldSnapshots() error {
	entries, err := os.ReadDir(sm.config.DataDir)
	if err != nil {
		return err
	}

	var snapshots []os.DirEntry
	for _, entry := range entries {
		if matched, _ := filepath.Match("snapshot-*.json", entry.Name()); matched {
			snapshots = append(snapshots, entry)
		}
	}

	if len(snapshots) <= 3 {
		return nil
	}

	// Sort oldest first
	sort.Slice(snapshots, func(i, j int) bool {
		infoI, _ := snapshots[i].Info()
		infoJ, _ := snapshots[j].Info()
		return infoI.ModTime().Before(infoJ.ModTime())
	})

	// Keep last 3 files
	for _, entry := range snapshots[:len(snapshots)-3] {
		if err := os.Remove(filepath.Join(sm.config.DataDir, entry.Name())); err != nil {
			slog.Error("Failed to remove old snapshot", "name", entry.Name(), "error", err)
		}
	}
	return nil
}

func (sm *SnapshotManager) CreateSnapshot() error {
	file, err := os.CreateTemp(sm.config.DataDir, "snapshot-*.tmp.json")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpPath := file.Name()

	sm.kv.mu.RLock()
	defer sm.kv.mu.RUnlock()

	// Create a serializable map
	snapshotData := make(map[string]interface{})
	for key, kv := range sm.kv.data {
		snapshotData[key] = map[string]interface{}{
			"type":       kv.Type,
			"value":      string(kv.Value),
			"created_at": kv.CreatedAt.Format(time.RFC3339),
			"updated_at": kv.UpdatedAt.Format(time.RFC3339),
			"expires_at": kv.ExpiresAt.Format(time.RFC3339),
		}
	}

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(snapshotData); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to encode data: %w", err)
	}

	if err := file.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	finalPath := filepath.Join(sm.config.DataDir,
		fmt.Sprintf("snapshot-%d.json", time.Now().UnixNano()))

	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath)
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
		if matched, _ := filepath.Match("snapshot-*.json", entry.Name()); !matched {
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

	var snapshotData map[string]interface{}
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&snapshotData); err != nil {
		return fmt.Errorf("failed to decode snapshot: %w", err)
	}

	sm.kv.mu.Lock()
	defer sm.kv.mu.Unlock()

	for key, data := range snapshotData {
		item := data.(map[string]interface{})

		createdAt, _ := time.Parse(time.RFC3339, item["created_at"].(string))
		updatedAt, _ := time.Parse(time.RFC3339, item["updated_at"].(string))
		expiresAt, _ := time.Parse(time.RFC3339, item["expires_at"].(string))

		sm.kv.data[key] = &KeyValue{
			Type:      item["type"].(string),
			Value:     []byte(item["value"].(string)),
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
			ExpiresAt: expiresAt,
		}
	}

	return nil
}
