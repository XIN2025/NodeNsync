package main

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type ConnectionManager struct {
	mu          sync.RWMutex
	connections map[string]*Peer
	rateLimiter *RateLimiter
	authManager *AuthManager
	config      *ServerConfig
	metrics     *Metrics
	blacklist   map[string]time.Time
}

func NewConnectionManager(config *ServerConfig, metrics *Metrics, auth *AuthManager) *ConnectionManager {
	cm := &ConnectionManager{
		connections: make(map[string]*Peer),
		rateLimiter: NewRateLimiter(config),
		authManager: auth,
		config:      config,
		metrics:     metrics,
		blacklist:   make(map[string]time.Time),
	}
	go cm.cleanup()
	return cm
}

func (cm *ConnectionManager) Accept(conn net.Conn) (*Peer, error) {
	addr := conn.RemoteAddr().String()

	if cm.isBlacklisted(addr) {
		conn.Close()
		return nil, fmt.Errorf("connection blacklisted")
	}

	if !cm.checkConnectionLimit() {
		conn.Close()
		return nil, fmt.Errorf("max connections reached")
	}

	if !cm.rateLimiter.Allow(addr) {
		conn.Close()
		return nil, fmt.Errorf("rate limit exceeded")
	}

	peer := NewPeer(conn, make(chan Message, 100), make(chan *Peer, 1))
	peer.ID = generatePeerID()

	cm.mu.Lock()
	cm.connections[peer.ID] = peer
	atomic.AddUint64(&cm.metrics.Connections, 1)
	cm.mu.Unlock()

	return peer, nil
}

func (cm *ConnectionManager) Remove(peer *Peer) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if _, exists := cm.connections[peer.ID]; exists {
		delete(cm.connections, peer.ID)
		atomic.AddUint64(&cm.metrics.Connections, ^uint64(0))
		peer.conn.Close()
	}
}

func (cm *ConnectionManager) Blacklist(addr string, duration time.Duration) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.blacklist[addr] = time.Now().Add(duration)
}

func (cm *ConnectionManager) isBlacklisted(addr string) bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	expiry, exists := cm.blacklist[addr]
	if !exists {
		return false
	}

	if time.Now().After(expiry) {
		delete(cm.blacklist, addr)
		return false
	}

	return true
}

func (cm *ConnectionManager) checkConnectionLimit() bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return len(cm.connections) < cm.config.MaxConnections
}

func (cm *ConnectionManager) cleanup() {
	ticker := time.NewTicker(cm.config.CleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		cm.mu.Lock()
		now := time.Now()

		for addr, expiry := range cm.blacklist {
			if now.After(expiry) {
				delete(cm.blacklist, addr)
			}
		}

		for id, peer := range cm.connections {
			if now.Sub(peer.lastActivity) > cm.config.ConnectionTimeout {
				cm.Remove(peer)
				delete(cm.connections, id)
			}
		}
		cm.mu.Unlock()
	}
}
