package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
)

const defaultListenAddr = ":6379"

type Config struct {
	ListenAddr string
}

type Message struct {
	cmd  Command
	peer *Peer
}

type Server struct {
	ServerConfig
	peers          map[*Peer]bool
	ln             net.Listener
	addPeerCh      chan *Peer
	delPeerCh      chan *Peer
	quitCh         chan struct{}
	msgCh          chan Message
	kv             *KVStore
	commandHandler *CommandHandler
}

func NewServer(cfg *ServerConfig) *Server {
	metrics := NewMetrics()
	replicationManager := NewReplicationManager(cfg, nil, metrics) // Create ReplicationManager
	kv := NewKVStore(cfg, metrics, replicationManager)             // Pass ReplicationManager to KVStore
	authManager := NewAuthManager()
	clusterManager := NewClusterManager()
	monitor := NewMonitor()
	pubsub := NewPubSubManager(cfg, metrics)
	replicationManager.kv = kv // Set the KVStore in the ReplicationManager

	commandHandler := NewCommandHandler(cfg, authManager, clusterManager, kv, metrics, monitor, pubsub, replicationManager)

	return &Server{
		ServerConfig:   *cfg,
		peers:          make(map[*Peer]bool),
		addPeerCh:      make(chan *Peer),
		delPeerCh:      make(chan *Peer),
		quitCh:         make(chan struct{}),
		msgCh:          make(chan Message),
		kv:             kv,
		commandHandler: commandHandler,
	}
}

func (s *Server) Start() error {

	if s.SnapshotInterval <= 0 || s.CleanupInterval <= 0 {
		return fmt.Errorf("SnapshotInterval and CleanupInterval must be positive")
	}

	slog.Info("Starting server with config",
		"SnapshotInterval", s.SnapshotInterval,
		"CleanupInterval", s.CleanupInterval)

	ln, err := net.Listen("tcp", s.ListenAddr)
	if err != nil {
		return err
	}
	s.ln = ln

	go s.loop()

	slog.Info("goredis server running", "listenAddr", s.ListenAddr)

	return s.acceptLoop()
}

func (s *Server) getPrompt(peer *Peer) string {
	if peer.inSubscribedMode {
		return fmt.Sprintf("%s(subscribed mode)> ", s.ListenAddr)
	}
	return fmt.Sprintf("%s> ", s.ListenAddr)
}

func (s *Server) handleMessage(msg Message) error {
	switch v := msg.cmd.(type) {
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
		err := s.commandHandler.HandleCommand(msg.cmd, msg.peer)
		if err != nil {
			slog.Error("Command handling error", "err", err, "command", fmt.Sprintf("%T", msg.cmd))
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
	case SyncCommand:
		return s.commandHandler.handleCommand(msg.peer)
	default:
		return fmt.Errorf("ERR unknown command")
	}
}
func (s *Server) loop() {
	for {
		select {
		case msg := <-s.msgCh:
			if err := s.handleMessage(msg); err != nil {
				slog.Error("raw message eror", "err", err)
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
