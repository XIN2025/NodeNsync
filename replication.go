package main

import (
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/tidwall/resp"
)

type ReplicationRole string

const (
	ReplHandshake                 = "REPLHANDSHAKE"
	ReplAck                       = "REPLACK"
	ReplSync                      = "SYNC"
	ReplData                      = "REPLDATA"
	ReplEnd                       = "REPLEND"
	RoleMaster    ReplicationRole = "master"
	RoleReplica   ReplicationRole = "replica"
)

type ReplicationManager struct {
	mu sync.RWMutex

	role      ReplicationRole
	kv        *KVStore
	replicaKV *KVStore

	replicas     map[string]*ReplicaInfo
	replicaConns map[string]net.Conn

	masterAddr string
	masterConn net.Conn

	config       *ServerConfig
	metrics      *Metrics
	retryBackoff time.Duration
	maxBackoff   time.Duration
	activeConn   net.Conn
	syncComplete bool
	stopChan     chan struct{}
	activeSync   map[string]bool
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
	return &ReplicationManager{
		kv:           kv,
		replicas:     make(map[string]*ReplicaInfo),
		replicaConns: make(map[string]net.Conn),
		activeSync:   make(map[string]bool),
		config:       config,
		metrics:      metrics,
		stopChan:     make(chan struct{}),
	}
}
func (rm *ReplicationManager) StartReplication(masterAddr string) error {
	rm.mu.Lock()
	rm.masterAddr = masterAddr
	rm.role = RoleReplica
	rm.syncComplete = false
	rm.mu.Unlock()

	if err := rm.connectAndSync(); err != nil {
		slog.Error("Initial sync failed", "error", err)
		return err
	}

	go rm.replicationLoop()
	return nil
}

func (rm *ReplicationManager) replicationLoop() {
	retryDelay := time.Second
	for {
		select {
		case <-rm.stopChan:
			return
		default:
			if err := rm.connectAndSync(); err != nil {
				slog.Error("Replication error", "error", err)
				time.Sleep(retryDelay)
				retryDelay = min(retryDelay*2, 30*time.Second)
				continue
			}
			retryDelay = time.Second
		}
	}
}

func (rm *ReplicationManager) handleMasterConnection(conn net.Conn) error {

	writer := resp.NewWriter(conn)
	if err := writer.WriteArray([]resp.Value{
		resp.StringValue("REPLHANDSHAKE"),
		resp.StringValue(rm.config.ListenAddr),
	}); err != nil {
		conn.Close()
		return err
	}

	reader := resp.NewReader(conn)
	if _, _, err := reader.ReadValue(); err != nil {
		conn.Close()
		return fmt.Errorf("handshake failed: %w", err)
	}

	if err := writer.WriteArray([]resp.Value{resp.StringValue("SYNC")}); err != nil {
		conn.Close()
		return err
	}

	if err := rm.processSyncStream(conn); err != nil {
		conn.Close()
		return err
	}

	return nil
}

func (rm *ReplicationManager) processSyncStream(conn net.Conn) error {
	reader := resp.NewReader(conn)

	for {
		val, _, err := reader.ReadValue()
		if err != nil {
			return err
		}

		arr := val.Array()
		if len(arr) == 0 {
			continue
		}

		switch arr[0].String() {
		case "REPLDATA":
			if len(arr) < 3 {
				slog.Warn("Invalid REPLDATA command: insufficient arguments", "args", arr)
				continue
			}
			if err := rm.HandleReplData(arr); err != nil {
				slog.Error("Error processing REPLDATA", "err", err)
			}
		case "REPLEND":
			slog.Info("Initial sync completed")
			rm.mu.Lock()
			rm.syncComplete = true
			rm.mu.Unlock()

			go rm.handlePersistentConnection(conn)
			return nil
		}
	}
}
func (rm *ReplicationManager) handlePersistentConnection(conn net.Conn) error {
	defer conn.Close()
	reader := resp.NewReader(conn)
	writer := resp.NewWriter(conn)

	if err := writer.WriteString("READY"); err != nil {
		return err
	}

	for {
		val, _, err := reader.ReadValue()
		if err != nil {
			slog.Error("Replication stream error", "error", err)
			return err
		}

		if arr := val.Array(); len(arr) > 0 {
			switch arr[0].String() {
			case "REPLDATA":
				if len(arr) < 3 {
					slog.Warn("Invalid REPLDATA format")
					continue
				}
				rm.kv.ApplyReplicationCommand(ReplicationCommand{
					Operation: "SET",
					Key:       arr[1].String(),
					Value:     arr[2].Bytes(),
				})
			}
		}
	}
}

func (rm *ReplicationManager) HandleSyncRequest(peer *Peer) error {
	rm.kv.mu.RLock()
	defer rm.kv.mu.RUnlock()

	writer := resp.NewWriter(peer.conn)

	for key, value := range rm.kv.data {
		if err := writer.WriteArray([]resp.Value{
			resp.StringValue("REPLDATA"),
			resp.StringValue(key),
			resp.BytesValue(value.Value),
		}); err != nil {
			peer.conn.Close()
			return err
		}
	}

	if err := writer.WriteString("REPLEND"); err != nil {
		peer.conn.Close()
		return err
	}

	go rm.handleMasterConnectionPersistent(peer.conn)
	return nil
}

func (rm *ReplicationManager) handleMasterConnectionPersistent(conn net.Conn) {

	addr := conn.RemoteAddr().String()
	rm.mu.Lock()
	rm.replicas[addr] = &ReplicaInfo{
		Addr:      addr,
		LastSeen:  time.Now(),
		IsHealthy: true,
	}
	rm.mu.Unlock()

	tcpConn := conn.(*net.TCPConn)
	tcpConn.SetKeepAlive(true)
	tcpConn.SetKeepAlivePeriod(30 * time.Second)
	conn.SetDeadline(time.Time{})

	ticker := time.NewTicker(5 * time.Second)
	defer func() {
		ticker.Stop()
		conn.Close()
		rm.mu.Lock()
		rm.replicas[addr].IsHealthy = false
		rm.mu.Unlock()
	}()

	go func() {
		writer := resp.NewWriter(conn)
		for range ticker.C {
			rm.mu.Lock()
			rm.replicas[addr].LastSeen = time.Now()
			rm.mu.Unlock()

			if err := writer.WriteString("PING"); err != nil {
				return
			}
		}
	}()

	reader := resp.NewReader(conn)
	for {
		_, _, err := reader.ReadValue()
		if err != nil {
			break
		}
	}
}

func (rm *ReplicationManager) handleMasterUpdates(conn net.Conn) {
	defer conn.Close()

	rm.mu.Lock()
	addr := conn.RemoteAddr().String()
	rm.replicas[addr] = &ReplicaInfo{
		Addr:      addr,
		LastSeen:  time.Now(),
		IsHealthy: true,
	}
	rm.replicas[addr].IsHealthy = true
	rm.mu.Unlock()

	reader := resp.NewReader(conn)
	writer := resp.NewWriter(conn)

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ticker.C:
				rm.mu.Lock()
				rm.replicas[addr].LastSeen = time.Now()
				rm.mu.Unlock()

				if err := writer.WriteString("PING"); err != nil {
					return
				}
			}
		}
	}()

	for {
		val, _, err := reader.ReadValue()
		if err != nil {
			rm.mu.Lock()
			rm.replicas[addr].IsHealthy = false
			rm.mu.Unlock()
			return
		}

		if val.String() == "PONG" {
			rm.mu.Lock()
			rm.replicas[addr].LastSeen = time.Now()
			rm.mu.Unlock()
		}
	}
}

func (rm *ReplicationManager) handleContinuousUpdates(conn net.Conn) {
	defer conn.Close()
	reader := resp.NewReader(conn)
	writer := resp.NewWriter(conn)

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	go func() {
		for range ticker.C {
			if err := writer.WriteString("PONG"); err != nil {
				return
			}
		}
	}()

	for {
		val, _, err := reader.ReadValue()
		if err != nil {
			slog.Error("Update stream broken", "error", err)
			rm.mu.Lock()
			rm.syncComplete = false
			rm.mu.Unlock()
			return
		}

		switch val.String() {
		case "PING":
			writer.WriteString("PONG")
		default:
			arr := val.Array()
			if len(arr) > 0 && arr[0].String() == "REPLDATA" {
				rm.kv.ApplyReplicationCommand(ReplicationCommand{
					Operation: "SET",
					Key:       arr[1].String(),
					Value:     arr[2].Bytes(),
				})
			}
		}
	}
}

func (rm *ReplicationManager) Stop() {
	close(rm.stopChan)
	if rm.activeConn != nil {
		rm.activeConn.Close()
	}
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func (rm *ReplicationManager) HandleReplData(args []resp.Value) error {
	if len(args) < 3 {
		return fmt.Errorf("invalid REPLDATA: expected at least 3 arguments, got %d", len(args))
	}

	key := args[1].String()
	value := args[2].Bytes()

	rm.kv.mu.Lock()
	defer rm.kv.mu.Unlock()

	rm.kv.data[key] = &KeyValue{
		Value:     value,
		Type:      "string",
		UpdatedAt: time.Now(),
	}

	slog.Info("Processed replication data", "key", key, "value", string(value))
	return nil
}

func (rm *ReplicationManager) HandleReplHandshake(peer *Peer) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	writer := resp.NewWriter(peer.conn)
	if err := writer.WriteString(ReplAck); err != nil {
		return fmt.Errorf("handshake ACK failed: %w", err)
	}

	slog.Info("Handshake completed", "replica", peer.conn.RemoteAddr().String())
	rm.replicas[peer.conn.RemoteAddr().String()] = &ReplicaInfo{
		Addr:      peer.conn.RemoteAddr().String(),
		LastSeen:  time.Now(),
		IsHealthy: true,
	}
	return nil
}

func (rm *ReplicationManager) handleSyncCommand(peer *Peer) error {
	slog.Info("Starting sync process with replica", "replicaAddr", peer.conn.RemoteAddr())

	rm.kv.mu.RLock()
	defer rm.kv.mu.RUnlock()

	writer := resp.NewWriter(peer.conn)
	for key, value := range rm.kv.data {
		err := writer.WriteArray([]resp.Value{
			resp.StringValue("REPLDATA"),
			resp.StringValue(key),
			resp.BytesValue(value.Value),
		})
		if err != nil {
			return err
		}
	}

	if err := writer.WriteArray([]resp.Value{resp.StringValue("REPLEND")}); err != nil {
		return err
	}

	go rm.handleMasterConnectionPersistent(peer.conn)
	return nil
}

func (rm *ReplicationManager) connectAndSync() error {
	rm.mu.Lock()
	masterAddr := rm.masterAddr
	rm.mu.Unlock()

	conn, err := net.DialTimeout("tcp", masterAddr, 30*time.Second)
	if err != nil {
		return fmt.Errorf("connection failed: %w", err)
	}
	defer conn.Close()

	writer := resp.NewWriter(conn)
	if err := writer.WriteArray([]resp.Value{
		resp.StringValue("REPLHANDSHAKE"),
		resp.StringValue(rm.config.ListenAddr),
	}); err != nil {
		return fmt.Errorf("handshake failed: %w", err)
	}

	reader := resp.NewReader(conn)
	if _, _, err := reader.ReadValue(); err != nil {
		return fmt.Errorf("handshake response failed: %w", err)
	}

	rm.mu.RLock()
	syncNeeded := !rm.syncComplete
	rm.mu.RUnlock()

	if syncNeeded {
		if err := writer.WriteArray([]resp.Value{resp.StringValue("SYNC")}); err != nil {
			return fmt.Errorf("SYNC command failed: %w", err)
		}

		if err := rm.processSyncStream(conn); err != nil {
			return fmt.Errorf("sync failed: %w", err)
		}

		rm.mu.Lock()
		rm.syncComplete = true
		rm.mu.Unlock()
	}

	return rm.handlePersistentConnection(conn)
}

func (rm *ReplicationManager) continuousReplication(conn net.Conn) error {
	rd := resp.NewReader(conn)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:

			writer := resp.NewWriter(conn)
			if err := writer.WriteArray([]resp.Value{resp.StringValue("PING")}); err != nil {
				return fmt.Errorf("heartbeat failed: %w", err)
			}
		default:
			conn.SetReadDeadline(time.Time{})
			_, _, err := rd.ReadValue()
			if err != nil {
				return fmt.Errorf("replication read failed: %w", err)
			}

		}
	}
}
func (rm *ReplicationManager) BroadcastToReplicas(cmd ReplicationCommand) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	data := []resp.Value{
		resp.StringValue(ReplData),
		resp.StringValue(cmd.Key),
		resp.BytesValue(cmd.Value),
	}

	for addr, conn := range rm.replicaConns {
		writer := resp.NewWriter(conn)
		if err := writer.WriteArray(data); err != nil {
			slog.Error("Broadcast failed", "replica", addr, "err", err)
		}
	}
}

func (rm *ReplicationManager) healthCheckLoop() {
	ticker := time.NewTicker(rm.config.HeartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		rm.mu.Lock()
		now := time.Now()

		for addr, replica := range rm.replicas {
			if now.Sub(replica.LastSeen) > rm.config.ConnectionTimeout {
				slog.Warn("Replica timeout", "addr", addr)
				replica.IsHealthy = false
				if conn, exists := rm.replicaConns[addr]; exists {
					conn.Close()
					delete(rm.replicaConns, addr)
				}
			}
		}
		rm.mu.Unlock()
	}
}

func (rm *ReplicationManager) SetAsReplica(masterAddr string) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	conn, err := net.DialTimeout("tcp", masterAddr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to master: %w", err)
	}
	rm.masterAddr = masterAddr
	rm.masterConn = conn

	writer := resp.NewWriter(conn)
	if err := writer.WriteArray([]resp.Value{resp.StringValue(ReplHandshake), resp.StringValue(":6380")}); err != nil {
		return fmt.Errorf("failed to send handshake: %w", err)
	}

	go rm.receiveSyncData(conn)
	return nil
}
func (rm *ReplicationManager) receiveSyncData(conn net.Conn) {
	reader := resp.NewReader(conn)

	for {
		v, _, err := reader.ReadValue()
		if err != nil {
			slog.Error("Error reading sync data from master", "err", err)
			return
		}

		args := v.Array()
		if len(args) == 0 {
			slog.Warn("Invalid sync data received: empty array")
			continue
		}

		command := args[0].String()
		switch command {
		case ReplData:
			if len(args) < 3 {
				slog.Warn("Invalid REPLDATA command: insufficient arguments", "args", args)
				continue
			}
			if err := rm.HandleReplData(args); err != nil {
				slog.Error("Error processing REPLDATA", "err", err)
			}
		case ReplEnd:
			slog.Info("Sync completed, switching to continuous replication")
			conn.Close()
			return
		default:
			slog.Warn("Unknown replication command received", "command", command, "args", args)
		}
	}
}
