package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/resp"
)

func generatePeerID() string {
	buf := make([]byte, 16)
	_, err := rand.Read(buf)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(buf)
}

type Peer struct {
	conn             net.Conn
	msgCh            chan Message
	delCh            chan *Peer
	ID               string
	sessionToken     string
	lastActivity     time.Time
	monitorCh        chan string
	inSubscribedMode bool
	server           *Server
	mu               sync.Mutex
}

func NewPeer(conn net.Conn, msgCh chan Message, delCh chan *Peer) *Peer {
	return &Peer{
		conn:      conn,
		msgCh:     msgCh,
		delCh:     delCh,
		monitorCh: make(chan string, 100),
	}
}

func (p *Peer) Send(msg []byte) (int, error) {
	return p.conn.Write(msg)
}

func argsToStrings(args []resp.Value) []string {
	result := make([]string, len(args))
	for i, arg := range args {
		result[i] = arg.String()
	}
	return result
}

func (p *Peer) ExitSubscribedMode() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inSubscribedMode = false
}

func (p *Peer) readLoop() error {
	rd := resp.NewReader(p.conn)
	for {
		v, _, err := rd.ReadValue()
		if err == io.EOF {
			slog.Info("Client disconnected", "remoteAddr", p.conn.RemoteAddr())
			p.delCh <- p
			break
		}
		if err != nil {
			slog.Error("Read error", "err", err)
			return err
		}

		if v.Type() == resp.Array {
			rawCMD := v.Array()[0].String()
			args := v.Array()[1:]
			var cmd Command

			slog.Info("Received command", "command", rawCMD, "args", args)

			switch strings.ToUpper(rawCMD) {
			// In peer.go's readLoop
			case "REPLACK":
				cmd = ReplAckCommand{}
				// ... other cases ...
			case "AUTH":
				if len(args) < 2 {
					writer := resp.NewWriter(p.conn)
					if err := writer.WriteError(fmt.Errorf("ERR wrong number of arguments for 'AUTH' command")); err != nil {
						return err
					}
					continue
				}
				cmd = AuthCommand{
					username: args[0].String(),
					password: args[1].String(),
				}
			case "PING":
				cmd = PingCommand{}
			case "QUIT":
				cmd = QuitCommand{}
			case "SET":
				if len(args) < 2 {
					writer := resp.NewWriter(p.conn)
					if err := writer.WriteError(fmt.Errorf("ERR wrong number of arguments for 'SET' command")); err != nil {
						return err
					}
					continue
				}
				cmd = SetCommand{
					key:   args[0].Bytes(),
					value: args[1].Bytes(),
				}
			case "GET":
				if len(args) < 1 {
					writer := resp.NewWriter(p.conn)
					if err := writer.WriteError(fmt.Errorf("ERR wrong number of arguments for 'GET' command")); err != nil {
						return err
					}
					continue
				}
				cmd = GetCommand{
					key: args[0].Bytes(),
				}
			case "DEL":
				if len(args) < 1 {
					writer := resp.NewWriter(p.conn)
					if err := writer.WriteError(fmt.Errorf("ERR wrong number of arguments for 'DEL' command")); err != nil {
						return err
					}
					continue
				}
				cmd = DelCommand{
					key: args[0].Bytes(),
				}
			case "INCR":
				if len(args) < 1 {
					writer := resp.NewWriter(p.conn)
					if err := writer.WriteError(fmt.Errorf("ERR wrong number of arguments for 'INCR' command")); err != nil {
						return err
					}
					continue
				}
				cmd = IncrCommand{
					key: args[0].Bytes(),
				}
			case "HSET":
				if len(args) < 2 {
					writer := resp.NewWriter(p.conn)
					if err := writer.WriteError(fmt.Errorf("ERR wrong number of arguments for 'HSET' command")); err != nil {
						return err
					}
					continue
				}
				key := args[0].Bytes()
				field := []byte("default")
				value := args[1].Bytes()
				if len(args) >= 3 {
					field = args[1].Bytes()
					value = args[2].Bytes()
				}
				cmd = HSetCommand{
					key:   key,
					field: field,
					value: value,
				}
			case "HGET":
				if len(args) < 2 {
					writer := resp.NewWriter(p.conn)
					if err := writer.WriteError(fmt.Errorf("ERR wrong number of arguments for 'HGET' command")); err != nil {
						return err
					}
					continue
				}
				cmd = HGetCommand{
					key:   args[0].Bytes(),
					field: args[1].Bytes(),
				}
			case "PUBLISH":
				if len(args) < 2 {
					writer := resp.NewWriter(p.conn)
					if err := writer.WriteError(fmt.Errorf("ERR wrong number of arguments for 'PUBLISH' command")); err != nil {
						return err
					}
					continue
				}
				channel := args[0].String()
				var messageParts []string
				for _, arg := range args[1:] {
					messageParts = append(messageParts, arg.String())
				}
				message := strings.Join(messageParts, " ")
				cmd = PublishCommand{
					channel: channel,
					message: []byte(message),
				}
			case "REPLICAOF":
				if len(args) < 2 {
					writer := resp.NewWriter(p.conn)
					if err := writer.WriteError(fmt.Errorf("ERR wrong number of arguments for 'REPLICAOF' command")); err != nil {
						return err
					}
					continue
				}
				cmd = ReplicaOfCommand{
					host: args[0].String(),
					port: args[1].String(),
				}
			case "REPLHANDSHAKE":
				if len(args) < 1 {
					writer := resp.NewWriter(p.conn)
					if err := writer.WriteError(fmt.Errorf("ERR wrong number of arguments for 'REPLHANDSHAKE' command")); err != nil {
						return err
					}
					continue
				}
				cmd = ReplHandshakeCommand{
					addr: args[0].String(),
				}
			case "SUBSCRIBE":
				if len(args) < 1 {
					writer := resp.NewWriter(p.conn)
					if err := writer.WriteError(fmt.Errorf("ERR wrong number of arguments for 'SUBSCRIBE' command")); err != nil {
						return err
					}
					continue
				}
				cmd = SubscribeCommand{
					channel: args[0].String(),
				}
			case "UNSUBSCRIBE":
				channels := argsToStrings(args)
				cmd = UnsubscribeCommand{
					channels: channels,
				}

			case "MONITOR":
				cmd = MonitorCommand{}
			case "INFO":
				cmd = InfoCommand{}
			case "ROLE":
				cmd = RoleCommand{}
			case "HELP":
				cmd = HelpCommand{}
			case "COMMAND":
				cmd = CommandCommand{}
			case "HELLO":
				cmd = HelloCommand{}
			case "SYNC":
				slog.Info("Received SYNC command")
				p.msgCh <- Message{
					cmd:  SyncCommand{},
					peer: p,
				}
			default:
				slog.Error("Unknown command", "command", rawCMD)
				writer := resp.NewWriter(p.conn)
				if err := writer.WriteError(fmt.Errorf("ERR unknown command '%s'", rawCMD)); err != nil {
					return err
				}
				continue
			}

			p.msgCh <- Message{
				cmd:  cmd,
				peer: p,
			}
		}
	}
	return nil
}

func (p *Peer) IsActive() bool {
	return time.Since(p.lastActivity) < time.Minute
}
