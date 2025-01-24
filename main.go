package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"

	"github.com/tidwall/resp"
)

type Config struct {
	ListenAddr string
}

type Message struct {
	cmd  Command
	peer *Peer
}

type Server struct {
	Config             *ServerConfig
	metrics            *Metrics
	peers              map[*Peer]bool
	ln                 net.Listener
	addPeerCh          chan *Peer
	delPeerCh          chan *Peer
	replicationManager *ReplicationManager
	quitCh             chan struct{}
	msgCh              chan Message
	kv                 *KVStore
	commandHandler     *CommandHandler
}

func NewServer(cfg *ServerConfig) *Server {
	metrics := NewMetrics()
	kv := NewKVStore(cfg, metrics, nil)

	authManager := NewAuthManager()
	clusterManager := NewClusterManager()
	monitor := NewMonitor()
	pubsub := NewPubSubManager(cfg, metrics)
	replicationManager := NewReplicationManager(cfg, kv, metrics)

	kv.replicationManager = replicationManager

	server := &Server{
		Config:             cfg,
		kv:                 kv,
		metrics:            metrics,
		replicationManager: replicationManager,
		peers:              make(map[*Peer]bool),
		addPeerCh:          make(chan *Peer),
		delPeerCh:          make(chan *Peer),
		quitCh:             make(chan struct{}),
		msgCh:              make(chan Message),
	}

	server.commandHandler = NewCommandHandler(
		cfg,
		authManager,
		clusterManager,
		kv,
		metrics,
		monitor,
		pubsub,
		replicationManager,
		server,
	)

	return server
}

func (s *Server) Start() error {

	if s.Config.SnapshotInterval <= 0 || s.Config.CleanupInterval <= 0 {
		return fmt.Errorf("SnapshotInterval and CleanupInterval must be positive")
	}

	slog.Info("Starting server with config",
		"SnapshotInterval", s.Config.SnapshotInterval,
		"CleanupInterval", s.Config.CleanupInterval)

	ln, err := net.Listen("tcp", s.Config.ListenAddr)
	if err != nil {
		return err
	}
	s.ln = ln

	go s.loop()

	slog.Info("goredis server running", "listenAddr", s.Config.ListenAddr)

	return s.acceptLoop()
}

func (c SyncCommand) Name() string {
	return "SYNC"
}

func (s *Server) handleMessage(msg Message) error {
	switch v := msg.cmd.(type) {
	case SyncCommand:
		if s.replicationManager == nil {
			writer := resp.NewWriter(msg.peer.conn)
			return writer.WriteError(fmt.Errorf("replication not enabled"))
		}
		return s.replicationManager.handleSyncCommand(msg.peer)

	case ReplHandshakeCommand:
		if s.replicationManager == nil {
			return fmt.Errorf("replication not enabled")
		}
		return s.commandHandler.HandleReplHandshake(v, msg.peer)

	case ReplDataCommand:
		if s.replicationManager == nil {
			return fmt.Errorf("replication not enabled")
		}
		return s.replicationManager.HandleReplData(v.Args)

	case ReplAckCommand:

		return nil
	case PingCommand:
		return s.commandHandler.HandleCommand(v, msg.peer)
	case QuitCommand:
		return s.commandHandler.HandleCommand(v, msg.peer)
	case AuthCommand:
		return s.commandHandler.HandleCommand(v, msg.peer)
	case SetCommand:
		return s.commandHandler.HandleCommand(v, msg.peer)
	case GetCommand:
		return s.commandHandler.HandleCommand(v, msg.peer)
	case DelCommand:
		return s.commandHandler.HandleCommand(v, msg.peer)
	case IncrCommand:
		return s.commandHandler.HandleCommand(v, msg.peer)
	case HSetCommand:
		return s.commandHandler.HandleCommand(v, msg.peer)
	case HGetCommand:
		return s.commandHandler.HandleCommand(v, msg.peer)
	case PublishCommand:
		return s.commandHandler.HandleCommand(v, msg.peer)
	case SubscribeCommand:
		return s.commandHandler.HandleCommand(v, msg.peer)
	case UnsubscribeCommand:
		return s.commandHandler.HandleCommand(v, msg.peer)

	case MonitorCommand:

		go func() {
			if err := s.commandHandler.HandleCommand(v, msg.peer); err != nil {
				slog.Error("MONITOR command error", "err", err)
			}
		}()
		return nil

	case ReplicaOfCommand:
		slog.Info("Handling REPLICAOF command", "host", v.host, "port", v.port)
		err := s.commandHandler.HandleCommand(v, msg.peer)
		if err != nil {
			slog.Error("Failed to handle REPLICAOF command", "err", err)
			return err
		}
		return nil
	case InfoCommand:
		return s.commandHandler.HandleCommand(v, msg.peer)
	case RoleCommand:
		return s.commandHandler.HandleCommand(v, msg.peer)
	case HelpCommand:
		return s.commandHandler.HandleCommand(v, msg.peer)
	case CommandCommand:
		return s.commandHandler.HandleCommand(v, msg.peer)
	case HelloCommand:
		return s.commandHandler.HandleCommand(v, msg.peer)

	default:
		if msg.cmd == nil {

			slog.Debug("Ignoring unexpected <nil> command from replica", "peer", msg.peer.conn.RemoteAddr())
		} else {

			slog.Warn("Received unexpected command from replica", "command", msg.cmd)
		}
		return nil
	}
}

func (s *Server) loop() {
	for {
		select {
		case msg := <-s.msgCh:
			if err := s.handleMessage(msg); err != nil {
				slog.Error("raw message error", "err", err)
			}
		case <-s.quitCh:
			return
		case peer := <-s.addPeerCh:
			slog.Info("peer connected", "remoteAddr", peer.conn.RemoteAddr())
			s.peers[peer] = true
		case peer := <-s.delPeerCh:
			slog.Info("peer disconnected", "remoteAddr", peer.conn.RemoteAddr())
			delete(s.peers, peer)
		}
	}
}

func (s *Server) acceptLoop() error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			slog.Error("accept error", "err", err)
			continue
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	peer := NewPeer(conn, s.msgCh, s.delPeerCh)
	peer.server = s
	s.addPeerCh <- peer
	if err := peer.readLoop(); err != nil {
		slog.Error("peer read error", "err", err, "remoteAddr", conn.RemoteAddr())
	}
}

func main() {
	listenAddr := flag.String("listenAddr", "", "listen address of the goredis server (e.g., :6379)")
	flag.Parse()

	userConfig := &ServerConfig{}
	if *listenAddr != "" {
		userConfig.ListenAddr = *listenAddr
	}

	serverCfg := DefaultConfig(userConfig)

	server := NewServer(serverCfg)
	log.Fatal(server.Start())
}
