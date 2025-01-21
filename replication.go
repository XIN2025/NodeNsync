package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type ReplicationRole string

const (
	RoleMaster  ReplicationRole = "master"
	RoleReplica ReplicationRole = "replica"
)

type ReplicationManager struct {
	mu sync.RWMutex

	role ReplicationRole
	kv   *KVStore

	replicas map[string]*ReplicaInfo

	masterAddr string
	masterConn net.Conn
	offset     uint64

	config  *ServerConfig
	metrics *Metrics
}

type ReplicaInfo struct {
	Addr      string
	LastSeen  time.Time
	Offset    uint64
	IsHealthy bool
}

type ReplicationCommand struct {
	Operation string
	Key       string
	Value     []byte
	TTL       time.Duration
}

func NewReplicationManager(config *ServerConfig, kv *KVStore, metrics *Metrics) *ReplicationManager {
	rm := &ReplicationManager{
		role:     RoleMaster,
		kv:       kv,
		replicas: make(map[string]*ReplicaInfo),
		config:   config,
		metrics:  metrics,
	}
	go rm.healthCheckLoop()
	return rm
}

func (rm *ReplicationManager) SetAsMaster() error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.role = RoleMaster
	if rm.masterConn != nil {
		rm.masterConn.Close()
		rm.masterConn = nil
	}
	return nil
}

func (rm *ReplicationManager) SetAsReplica(masterAddr string) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.role = RoleReplica
	rm.masterAddr = masterAddr

	slog.Info("Connecting to master", "masterAddr", masterAddr)
	err := rm.connectToMaster()
	if err != nil {
		slog.Error("Failed to connect to master", "err", err)
		return err
	}

	slog.Info("Successfully connected to master", "masterAddr", masterAddr)
	return nil
}

func (rm *ReplicationManager) connectToMaster() error {
	conn, err := net.Dial("tcp", rm.masterAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to master: %w", err)
	}

	rm.masterConn = conn
	go rm.replicationLoop()
	return nil
}

func (rm *ReplicationManager) replicationLoop() {
	defer rm.masterConn.Close()

	decoder := gob.NewDecoder(rm.masterConn)
	for {
		var cmd ReplicationCommand
		if err := decoder.Decode(&cmd); err != nil {
			time.Sleep(time.Second)
			rm.connectToMaster()
			return
		}

		rm.applyCommand(cmd)
		rm.offset++
	}
}

func (rm *ReplicationManager) applyCommand(cmd ReplicationCommand) error {
	switch cmd.Operation {
	case "SET":
		return rm.kv.Set(cmd.Key, cmd.Value, cmd.TTL)
	case "DEL":
		rm.kv.Delete(cmd.Key)
		return nil
	default:
		return fmt.Errorf("unknown replication command: %s", cmd.Operation)
	}
}

func (rm *ReplicationManager) BroadcastToReplicas(cmd ReplicationCommand) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if rm.role != RoleMaster {
		return
	}

	encoded := &bytes.Buffer{}
	encoder := gob.NewEncoder(encoded)
	if err := encoder.Encode(cmd); err != nil {
		return
	}

	for addr, replica := range rm.replicas {
		if !replica.IsHealthy {
			continue
		}

		go func(addr string, data []byte) {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				return
			}
			defer conn.Close()
			conn.Write(data)
		}(addr, encoded.Bytes())
	}
}

func (rm *ReplicationManager) healthCheckLoop() {
	ticker := time.NewTicker(rm.config.HeartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		rm.mu.Lock()
		now := time.Now()

		for _, replica := range rm.replicas {
			wasHealthy := replica.IsHealthy
			replica.IsHealthy = now.Sub(replica.LastSeen) < rm.config.ConnectionTimeout

			if wasHealthy != replica.IsHealthy {
				if replica.IsHealthy {
					atomic.AddUint64(&rm.metrics.ReplicaCount, 1)
				} else {
					atomic.AddUint64(&rm.metrics.ReplicaCount, ^uint64(0))
				}
			}
		}
		rm.mu.Unlock()
	}
}
