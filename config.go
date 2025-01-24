package main

import (
	"crypto/tls"
	"time"
)

type ServerConfig struct {
	ListenAddr     string
	DataDir        string
	MaxConnections int
	MaxMessageSize int64

	TLSConfig *tls.Config
	RateLimit int

	ConnectionTimeout time.Duration
	CommandTimeout    time.Duration
	SnapshotInterval  time.Duration
	CleanupInterval   time.Duration
	HeartbeatInterval time.Duration

	EnableTLS      bool
	EnableMetrics  bool
	EnableLogging  bool
	EnablePubSub   bool
	EnableReplicas bool
}

func DefaultConfig(overrides ...*ServerConfig) *ServerConfig {

	cfg := &ServerConfig{
		ListenAddr:        ":6379",
		DataDir:           "./data",
		MaxConnections:    10000,
		SnapshotInterval:  1 * time.Minute,
		MaxMessageSize:    512 * 1024 * 1024,
		RateLimit:         3,
		ConnectionTimeout: 5 * time.Second,
		CommandTimeout:    2 * time.Second,
		CleanupInterval:   1 * time.Minute,
		HeartbeatInterval: 1 * time.Second,
		EnableMetrics:     true,
		EnableLogging:     true,
		EnablePubSub:      true,
		EnableReplicas:    true,
	}

	for _, override := range overrides {
		if override.ListenAddr != "" {
			cfg.ListenAddr = override.ListenAddr
		}

	}

	return cfg
}
