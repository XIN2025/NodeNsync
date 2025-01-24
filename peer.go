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
	defer func() {
		p.conn.Close()
		p.delCh <- p
		slog.Info("Connection closed", "remote", p.conn.RemoteAddr())
	}()

	reader := resp.NewReader(p.conn)
	writer := resp.NewWriter(p.conn)

	for {
		// Read RESP value from connection
		val, _, err := reader.ReadValue()
		if err != nil {
			if err == io.EOF {
				slog.Debug("Client closed connection", "remote", p.conn.RemoteAddr())
				return nil
			}
			slog.Error("Read error", "remote", p.conn.RemoteAddr(), "error", err)
			return err
		}

		// Validate command structure
		if val.Type() != resp.Array || len(val.Array()) == 0 {
			slog.Warn("Invalid command format", "remote", p.conn.RemoteAddr())
			writer.WriteError(fmt.Errorf("ERR invalid command format"))
			continue
		}

		cmdParts := val.Array()
		rawCmd := strings.ToUpper(cmdParts[0].String())
		args := cmdParts[1:]

		// Rate limiting check
		if p.server != nil && p.server.Config.RateLimit > 0 {
			clientIP, _, _ := net.SplitHostPort(p.conn.RemoteAddr().String())
			if !p.server.rateLimiter.Allow(clientIP) {
				errMsg := fmt.Sprintf("ERR rate limit exceeded (max %d commands/second)",
					p.server.Config.RateLimit)
				slog.Warn("Rate limit blocked", "client", clientIP)
				writer.WriteError(fmt.Errorf(errMsg))
				continue
			}
		}

		// Parse command and handle errors
		var cmd Command
		switch rawCmd {
		case "AUTH":
			if len(args) < 2 {
				writer.WriteError(fmt.Errorf("ERR wrong number of arguments for 'AUTH'"))
				continue
			}
			cmd = AuthCommand{
				username: args[0].String(),
				password: args[1].String(),
			}

		case "PING":
			cmd = PingCommand{}

		case "SET":
			if len(args) < 2 {
				writer.WriteError(fmt.Errorf("ERR wrong number of arguments for 'SET'"))
				continue
			}
			cmd = SetCommand{
				key:   args[0].Bytes(),
				value: args[1].Bytes(),
			}

		case "GET":
			if len(args) < 1 {
				writer.WriteError(fmt.Errorf("ERR wrong number of arguments for 'GET'"))
				continue
			}
			cmd = GetCommand{
				key: args[0].Bytes(),
			}

		case "DEL":
			if len(args) < 1 {
				writer.WriteError(fmt.Errorf("ERR wrong number of arguments for 'DEL'"))
				continue
			}
			cmd = DelCommand{
				key: args[0].Bytes(),
			}

		case "INCR":
			if len(args) < 1 {
				writer.WriteError(fmt.Errorf("ERR wrong number of arguments for 'INCR'"))
				continue
			}
			cmd = IncrCommand{
				key: args[0].Bytes(),
			}

		case "HSET":
			if len(args) < 3 {
				writer.WriteError(fmt.Errorf("ERR wrong number of arguments for 'HSET'"))
				continue
			}
			cmd = HSetCommand{
				key:   args[0].Bytes(),
				field: args[1].Bytes(),
				value: args[2].Bytes(),
			}

		case "HGET":
			if len(args) < 2 {
				writer.WriteError(fmt.Errorf("ERR wrong number of arguments for 'HGET'"))
				continue
			}
			cmd = HGetCommand{
				key:   args[0].Bytes(),
				field: args[1].Bytes(),
			}

		case "QUIT":
			writer.WriteString("OK")
			return nil

		default:
			writer.WriteError(fmt.Errorf("ERR unknown command '%s'", rawCmd))
			continue
		}

		// Send command to handler
		p.msgCh <- Message{
			cmd:  cmd,
			peer: p,
		}

		// Update last activity time
		p.mu.Lock()
		p.lastActivity = time.Now()
		p.mu.Unlock()
	}
}

func (p *Peer) IsActive() bool {
	return time.Since(p.lastActivity) < time.Minute
}
