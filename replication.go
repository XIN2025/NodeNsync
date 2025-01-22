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

func (rm *ReplicationManager) connectToMaster() error {

	if rm.masterConn != nil {
		rm.masterConn.Close()
		rm.masterConn = nil
	}

	conn, err := net.Dial("tcp", rm.masterAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to master: %w", err)
	}

	rm.masterConn = conn

	go rm.replicationLoop()

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

func (rm *ReplicationManager) handleHeartbeat() {
	buf := make([]byte, 4)
	for {
		_, err := rm.masterConn.Read(buf)
		if err != nil {
			slog.Error("Failed to read heartbeat from master", "err", err)
			return
		}

		if string(buf) == "PING" {
			slog.Info("Received heartbeat from master")
		}
	}
}

func (rm *ReplicationManager) heartbeatLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		rm.mu.RLock()
		for addr, replica := range rm.replicas {
			if !replica.IsHealthy {
				continue
			}

			go func(addr string) {
				conn, err := net.Dial("tcp", addr)
				if err != nil {
					slog.Error("Failed to send heartbeat to replica", "addr", addr, "err", err)
					return
				}
				defer conn.Close()

				_, err = conn.Write([]byte("PING"))
				if err != nil {
					slog.Error("Failed to send heartbeat to replica", "addr", addr, "err", err)
					return
				}

				slog.Info("Sent heartbeat to replica", "addr", addr)
			}(addr)
		}
		rm.mu.RUnlock()
	}
}

func (rm *ReplicationManager) replicationLoop() {

	slog.Info("Starting replication loop")
	defer func() {
		if rm.masterConn != nil {
			rm.masterConn.Close()
			rm.masterConn = nil
		}
	}()

	decoder := gob.NewDecoder(rm.masterConn)

	for {
		var cmd ReplicationCommand
		err := decoder.Decode(&cmd)
		if err != nil {
			slog.Error("Replication error", "err", err)
			time.Sleep(time.Second)
			if err := rm.connectToMaster(); err != nil {
				slog.Error("Failed to reconnect to master", "err", err)
			}
			return
		}

		slog.Info("Received replication command from master", "cmd", cmd)
		if err := rm.applyCommand(cmd); err != nil {
			slog.Error("Failed to apply replicated command", "err", err)
			continue
		}

		atomic.AddUint64(&rm.offset, 1)
		if rm.metrics != nil {
			atomic.StoreUint64(&rm.metrics.ReplicationOffset, rm.offset)
		}
	}
}
func (rm *ReplicationManager) applyCommand(cmd ReplicationCommand) error {
	slog.Info("Applying replication command", "cmd", cmd)
	switch cmd.Operation {
	case "SET":
		slog.Info("Applying SET command", "key", cmd.Key, "value", cmd.Value)
		return rm.kv.Set(cmd.Key, cmd.Value, cmd.TTL)
	case "DEL":
		slog.Info("Applying DEL command", "key", cmd.Key)
		rm.kv.Delete(cmd.Key)
		return nil
	default:
		slog.Error("Unknown replication command", "operation", cmd.Operation)
		return nil
	}
}

func (rm *ReplicationManager) BroadcastToReplicas(cmd ReplicationCommand) {

	slog.Info("Broadcasting replication command", "cmd", cmd)
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if rm.role != RoleMaster {
		return
	}

	encoded := &bytes.Buffer{}
	encoder := gob.NewEncoder(encoded)
	if err := encoder.Encode(cmd); err != nil {
		slog.Error("Failed to encode replication command", "err", err)
		return
	}

	slog.Info("Encoded replication command", "cmd", cmd, "encoded", encoded.Bytes())

	for addr, replica := range rm.replicas {
		if !replica.IsHealthy {
			continue
		}

		go func(addr string, data []byte) {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				slog.Error("Failed to connect to replica", "addr", addr, "err", err)
				return
			}
			defer conn.Close()

			_, err = conn.Write(data)
			if err != nil {
				slog.Error("Failed to send replication command to replica", "addr", addr, "err", err)
				return
			}

			slog.Info("Successfully sent replication command to replica", "addr", addr, "cmd", cmd)
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
