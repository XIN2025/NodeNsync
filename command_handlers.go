package main

import (
	"fmt"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/tidwall/resp"
)

type CommandHandler struct {
	authManager        *AuthManager
	clusterManager     *ClusterManager
	kv                 *KVStore
	metrics            *Metrics
	monitor            *Monitor
	pubsub             *PubSubManager
	replicationManager *ReplicationManager
	startTime          time.Time
}

func NewCommandHandler(config *ServerConfig, authManager *AuthManager, clusterManager *ClusterManager, kv *KVStore, metrics *Metrics, monitor *Monitor, pubsub *PubSubManager, replicationManager *ReplicationManager) *CommandHandler {
	return &CommandHandler{
		authManager:        authManager,
		clusterManager:     clusterManager,
		kv:                 kv,
		metrics:            metrics,
		monitor:            monitor,
		pubsub:             pubsub,
		replicationManager: replicationManager,
		startTime:          time.Now(),
	}
}

func (h *CommandHandler) HandleCommand(cmd Command, peer *Peer) error {

	slog.Info("Handling command", "command", fmt.Sprintf("%T", cmd))

	if h.monitor != nil {
		cmdStr := formatCommand(cmd)
		h.monitor.Record(cmdStr)
	}

	switch v := cmd.(type) {

	case ReplicaOfCommand:
		slog.Info("Handling REPLICAOF command", "host", v.host, "port", v.port)
		return h.handleReplicaOf(v, peer)
	case PingCommand:
		return h.handlePing(peer)
	case QuitCommand:
		return h.handleQuit(peer)
	case AuthCommand:
		return h.handleAuth(v, peer)
	case SetCommand:
		return h.handleSet(v, peer)
	case GetCommand:
		return h.handleGet(v, peer)
	case DelCommand:
		return h.handleDel(v, peer)
	case IncrCommand:
		return h.handleIncr(v, peer)
	case HSetCommand:
		return h.handleHSet(v, peer)
	case HGetCommand:
		return h.handleHGet(v, peer)
	case PublishCommand:
		return h.handlePublish(v, peer)
	case SubscribeCommand:
		return h.handleSubscribe(v, peer)
	case UnsubscribeCommand:
		return h.handleUnsubscribe(v, peer)
	case MonitorCommand:
		return h.handleMonitor(peer)
	case InfoCommand:
		return h.handleInfo(peer)
	case RoleCommand:
		return h.handleRole(peer)
	case SyncCommand:
		return h.handleSync(peer)
	case HelpCommand:

		return h.handleHelp(v, peer)
	case CommandCommand:
		return h.handleCommand(peer)
	case HelloCommand:
		return h.handleHello(peer)
	default:
		slog.Error("Unknown command", "command", fmt.Sprintf("%T", cmd))
		return fmt.Errorf("ERR unknown command")
	}
}
func formatCommand(cmd Command) string {
	switch v := cmd.(type) {
	case AuthCommand:
		return fmt.Sprintf("AUTH %s %s", v.username, v.password)
	case SetCommand:
		return fmt.Sprintf("SET %s %s", string(v.key), string(v.value))
	case GetCommand:
		return fmt.Sprintf("GET %s", string(v.key))
	case DelCommand:
		return fmt.Sprintf("DEL %s", string(v.key))
	case IncrCommand:
		return fmt.Sprintf("INCR %s", string(v.key))
	case HSetCommand:
		return fmt.Sprintf("HSET %s %s %s", string(v.key), string(v.field), string(v.value))
	case HGetCommand:
		return fmt.Sprintf("HGET %s %s", string(v.key), string(v.field))
	case PublishCommand:
		return fmt.Sprintf("PUBLISH %s %s", v.channel, string(v.message))
	case SubscribeCommand:
		return fmt.Sprintf("SUBSCRIBE %s", v.channel)
	case UnsubscribeCommand:
		return fmt.Sprintf("UNSUBSCRIBE %v", v.channels)
	case MonitorCommand:
		return "MONITOR"
	case InfoCommand:
		return "INFO"
	case RoleCommand:
		return "ROLE"
	case HelpCommand:
		return "HELP"
	case CommandCommand:
		return "COMMAND"
	case HelloCommand:
		return "HELLO"
	default:
		return fmt.Sprintf("UNKNOWN: %v", cmd)
	}
}

func (h *CommandHandler) handleReplicaOf(cmd ReplicaOfCommand, peer *Peer) error {
	slog.Info("Handling REPLICAOF command", "host", cmd.host, "port", cmd.port)

	if err := h.requireAuth(peer); err != nil {
		slog.Error("Authentication required", "err", err)
		return err
	}

	masterAddr := fmt.Sprintf("%s:%s", cmd.host, cmd.port)
	slog.Info("Setting as replica of master", "masterAddr", masterAddr)

	err := h.replicationManager.SetAsReplica(masterAddr)
	if err != nil {
		slog.Error("Failed to set as replica", "err", err)
		return fmt.Errorf("ERR failed to set as replica: %v", err)
	}

	slog.Info("Successfully set as replica", "masterAddr", masterAddr)

	if err := resp.NewWriter(peer.conn).WriteString("OK"); err != nil {
		return fmt.Errorf("ERR failed to write response: %v", err)
	}

	return nil
}

func (h *CommandHandler) handleSync(peer *Peer) error {
	slog.Info("Handling SYNC command")

	// Send all key-value data to the replica
	h.kv.mu.RLock()
	defer h.kv.mu.RUnlock()

	writer := resp.NewWriter(peer.conn)
	for key, value := range h.kv.data {
		if err := writer.WriteArray([]resp.Value{
			resp.StringValue("SET"),
			resp.StringValue(key),
			resp.BytesValue(value.Value),
		}); err != nil {
			slog.Error("Failed to send key-value data", "err", err)
			return err
		}
	}

	slog.Info("Finished sending key-value data to replica")
	return nil
}

func (h *CommandHandler) handleCommand(peer *Peer) error {
	commands := []string{
		"PING", "QUIT", "AUTH", "SET", "GET", "DEL", "INCR", "HSET", "HGET",
		"PUBLISH", "SUBSCRIBE", "UNSUBSCRIBE", "MONITOR", "INFO", "ROLE", "HELP", "COMMAND", "HELLO", "SYNC", "REPLICAOF",
	}
	writer := resp.NewWriter(peer.conn)
	return writer.WriteArray([]resp.Value{
		resp.StringValue("COMMAND"),
		resp.ArrayValue(stringsToValues(commands)),
	})
}
func (h *CommandHandler) handleHelp(cmd HelpCommand, peer *Peer) error {
	commands := []string{
		"PING", "QUIT", "AUTH", "SET", "GET", "DEL", "INCR", "HSET", "HGET",
		"PUBLISH", "SUBSCRIBE", "MONITOR", "INFO", "ROLE", "HELP", "COMMAND", "HELLO", "SYNC", "REPLICAOF",
	}
	writer := resp.NewWriter(peer.conn)
	return writer.WriteArray([]resp.Value{
		resp.StringValue("HELP"),
		resp.ArrayValue(stringsToValues(commands)),
	})
}

func stringsToValues(strs []string) []resp.Value {
	values := make([]resp.Value, len(strs))
	for i, s := range strs {
		values[i] = resp.StringValue(s)
	}
	return values
}
func (h *CommandHandler) handleHello(peer *Peer) error {
	spec := map[string]string{
		"server":  "redis",
		"version": "1.0.0",
	}
	writer := resp.NewWriter(peer.conn)
	return writer.WriteArray([]resp.Value{
		resp.StringValue("HELLO"),
		resp.StringValue(fmt.Sprintf("%v", spec)),
	})
}

func (h *CommandHandler) handlePing(peer *Peer) error {
	return resp.NewWriter(peer.conn).WriteString("PONG")
}

func (h *CommandHandler) handleQuit(peer *Peer) error {
	resp.NewWriter(peer.conn).WriteString("OK")
	return peer.conn.Close()
}

func (h *CommandHandler) handleAuth(cmd AuthCommand, peer *Peer) error {
	token, err := h.authManager.Authenticate(cmd.username, cmd.password)
	if err != nil {
		return err
	}
	peer.sessionToken = token
	return resp.NewWriter(peer.conn).WriteString("OK")
}

func (h *CommandHandler) handleSet(cmd SetCommand, peer *Peer) error {
	if err := h.requireAuth(peer); err != nil {
		return err
	}
	h.kv.mu.Lock()
	defer h.kv.mu.Unlock()
	h.kv.data[string(cmd.key)] = &KeyValue{
		Value: cmd.value,
	}
	return resp.NewWriter(peer.conn).WriteString("OK")
}

func (h *CommandHandler) handleGet(cmd GetCommand, peer *Peer) error {
	if err := h.requireAuth(peer); err != nil {
		return err
	}
	h.kv.mu.RLock()
	defer h.kv.mu.RUnlock()
	val, exists := h.kv.data[string(cmd.key)]
	if !exists {
		return resp.NewWriter(peer.conn).WriteNull()
	}
	return resp.NewWriter(peer.conn).WriteString(string(val.Value))
}

func (h *CommandHandler) handleDel(cmd DelCommand, peer *Peer) error {
	if err := h.requireAuth(peer); err != nil {
		return err
	}
	h.kv.mu.Lock()
	defer h.kv.mu.Unlock()
	delete(h.kv.data, string(cmd.key))
	return resp.NewWriter(peer.conn).WriteString("OK")
}

func (h *CommandHandler) handleIncr(cmd IncrCommand, peer *Peer) error {
	if err := h.requireAuth(peer); err != nil {
		return err
	}
	h.kv.mu.Lock()
	defer h.kv.mu.Unlock()
	val, exists := h.kv.data[string(cmd.key)]
	if !exists {
		val = &KeyValue{
			Value: []byte("0"),
		}
	}
	num, err := strconv.ParseInt(string(val.Value), 10, 64)
	if err != nil {
		return fmt.Errorf("ERR value is not an integer")
	}
	num++
	h.kv.data[string(cmd.key)] = &KeyValue{
		Value: []byte(strconv.FormatInt(num, 10)),
	}
	return resp.NewWriter(peer.conn).WriteInteger(int(num))
}

func (h *CommandHandler) requireAuth(peer *Peer) error {
	if h.authManager != nil {
		if peer.sessionToken == "" {
			writer := resp.NewWriter(peer.conn)
			if err := writer.WriteError(fmt.Errorf("authentication required")); err != nil {
				return err
			}
			return fmt.Errorf("authentication required")
		}
		_, err := h.authManager.ValidateSession(peer.sessionToken)
		if err != nil {
			writer := resp.NewWriter(peer.conn)
			if err := writer.WriteError(fmt.Errorf("authentication failed")); err != nil {
				return err
			}
			return err
		}
	}
	return nil
}
func (h *CommandHandler) getCurrentOps() int64 {
	now := time.Now()
	window := now.Add(-time.Second)

	var ops int64
	for _, stats := range h.metrics.CommandLatencies {
		if stats.LastTime.After(window) {
			ops += int64(atomic.LoadUint64(&stats.Count))
		}
	}
	return ops
}

func (h *CommandHandler) handleUnsubscribe(cmd UnsubscribeCommand, peer *Peer) error {
	if err := h.requireAuth(peer); err != nil {
		return err
	}

	var remainingSubs int
	var err error

	if len(cmd.channels) == 0 {

		remainingSubs, err = h.pubsub.Unsubscribe(peer)
	} else {

		remainingSubs, err = h.pubsub.Unsubscribe(peer, cmd.channels...)
	}

	if err != nil {
		return err
	}

	writer := resp.NewWriter(peer.conn)
	for _, channel := range cmd.channels {
		writer.WriteArray([]resp.Value{
			resp.StringValue("unsubscribe"),
			resp.StringValue(channel),
			resp.IntegerValue(remainingSubs),
		})
	}

	if remainingSubs == 0 {
		writer.WriteArray([]resp.Value{
			resp.StringValue("unsubscribe"),
			resp.StringValue(""),
			resp.IntegerValue(0),
		})
	}

	return nil
}

func (h *CommandHandler) handleHSet(cmd HSetCommand, peer *Peer) error {
	if err := h.requireAuth(peer); err != nil {
		return err
	}

	h.kv.mu.Lock()
	defer h.kv.mu.Unlock()

	key := string(cmd.key)
	val, exists := h.kv.data[key]
	if !exists || val.Type != "hash" {
		val = &KeyValue{
			Type: "hash",
			Hash: make(map[string]*HashField),
		}
		h.kv.data[key] = val
	}

	val.Hash[string(cmd.field)] = &HashField{
		Value: cmd.value,
	}
	return resp.NewWriter(peer.conn).WriteInteger(1)
}

func (h *CommandHandler) handleHGet(cmd HGetCommand, peer *Peer) error {
	if err := h.requireAuth(peer); err != nil {
		return err
	}

	h.kv.mu.RLock()
	defer h.kv.mu.RUnlock()

	val, exists := h.kv.data[string(cmd.key)]
	if !exists || val.Type != "hash" {
		return resp.NewWriter(peer.conn).WriteNull()
	}

	field, exists := val.Hash[string(cmd.field)]
	if !exists {
		return resp.NewWriter(peer.conn).WriteNull()
	}

	return resp.NewWriter(peer.conn).WriteString(string(field.Value))
}

func (h *CommandHandler) handlePublish(cmd PublishCommand, peer *Peer) error {
	if err := h.requireAuth(peer); err != nil {
		return err
	}

	count := h.pubsub.Publish(cmd.channel, cmd.message)
	return resp.NewWriter(peer.conn).WriteInteger(int(count))
}

func (h *CommandHandler) handleSubscribe(cmd SubscribeCommand, peer *Peer) error {
	if err := h.requireAuth(peer); err != nil {
		return err
	}

	return h.pubsub.Subscribe(peer, cmd.channel)
}

func (h *CommandHandler) handleMonitor(peer *Peer) error {
	if err := h.requireAuth(peer); err != nil {
		return err
	}

	h.monitor.AddWatcher(peer)

	go func() {
		for msg := range peer.monitorCh {
			if err := resp.NewWriter(peer.conn).WriteString(msg); err != nil {

				h.monitor.RemoveWatcher(peer)
				return
			}
		}
	}()

	return nil
}
func (h *CommandHandler) handleInfo(peer *Peer) error {
	if err := h.requireAuth(peer); err != nil {
		return err
	}

	info := fmt.Sprintf(`# Server
redis_version:1.0.0
uptime_in_seconds:%d
connected_clients:%d
used_memory:%d

# Stats
total_connections_received:%d
total_commands_processed:%d
instantaneous_ops_per_sec:%d

# Replication
role:%s
connected_slaves:%d
master_repl_offset:%d

# Keyspace
db0:keys=%d`,
		time.Since(h.startTime).Seconds(),
		atomic.LoadUint64(&h.metrics.Connections),
		atomic.LoadUint64(&h.metrics.MemoryUsage),
		atomic.LoadUint64(&h.metrics.CommandsProcessed),
		atomic.LoadUint64(&h.metrics.CommandsProcessed),
		h.getCurrentOps(),
		h.replicationManager.role,
		atomic.LoadUint64(&h.metrics.ReplicaCount),
		atomic.LoadUint64(&h.metrics.ReplicationOffset),
		atomic.LoadUint64(&h.metrics.KeyCount))

	return resp.NewWriter(peer.conn).WriteString(info)
}

func (h *CommandHandler) handleRole(peer *Peer) error {
	if err := h.requireAuth(peer); err != nil {
		return err
	}

	role := "master"
	if h.replicationManager.role == RoleReplica {
		role = "replica"
	}
	return resp.NewWriter(peer.conn).WriteString(role)
}
