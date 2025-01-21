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
	// Create a default configuration
	cfg := &ServerConfig{
		ListenAddr:        ":6379", // Default port
		DataDir:           "./data",
		MaxConnections:    10000,
		MaxMessageSize:    512 * 1024 * 1024,
		RateLimit:         10000,
		ConnectionTimeout: 5 * time.Second,
		CommandTimeout:    2 * time.Second,
		SnapshotInterval:  15 * time.Minute,
		CleanupInterval:   1 * time.Minute,
		HeartbeatInterval: 1 * time.Second,
		EnableMetrics:     true,
		EnableLogging:     true,
		EnablePubSub:      true,
		EnableReplicas:    true,
	}

	// Apply overrides if provided
	for _, override := range overrides {
		if override.ListenAddr != "" {
			cfg.ListenAddr = override.ListenAddr
		}
		// Add other fields as needed
	}

	return cfg
}
