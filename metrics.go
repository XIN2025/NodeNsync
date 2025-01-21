package main

import (
	"sync/atomic"
	"time"
)

type Metrics struct {
	CommandsProcessed uint64
	CommandErrors     uint64

	BytesRead    uint64
	BytesWritten uint64
	Connections  uint64

	CommandLatencies map[string]*LatencyStats

	MemoryUsage uint64
	KeyCount    uint64

	ReplicationOffset uint64
	ReplicaCount      uint64
}

type LatencyStats struct {
	Count    uint64
	TotalMs  uint64
	MinMs    uint64
	MaxMs    uint64
	LastTime time.Time
}

func NewMetrics() *Metrics {
	return &Metrics{
		CommandLatencies: make(map[string]*LatencyStats),
	}
}

func (m *Metrics) IncrementCommands() {
	atomic.AddUint64(&m.CommandsProcessed, 1)
}

func (m *Metrics) IncrementErrors() {
	atomic.AddUint64(&m.CommandErrors, 1)
}

func (m *Metrics) AddBytesRead(n uint64) {
	atomic.AddUint64(&m.BytesRead, n)
}

func (m *Metrics) AddBytesWritten(n uint64) {
	atomic.AddUint64(&m.BytesWritten, n)
}

func (m *Metrics) RecordLatency(command string, duration time.Duration) {
	ms := uint64(duration.Milliseconds())

	stats, exists := m.CommandLatencies[command]
	if !exists {
		stats = &LatencyStats{
			MinMs: ms,
			MaxMs: ms,
		}
		m.CommandLatencies[command] = stats
	}

	atomic.AddUint64(&stats.Count, 1)
	atomic.AddUint64(&stats.TotalMs, ms)
	if ms < atomic.LoadUint64(&stats.MinMs) {
		atomic.StoreUint64(&stats.MinMs, ms)
	}
	if ms > atomic.LoadUint64(&stats.MaxMs) {
		atomic.StoreUint64(&stats.MaxMs, ms)
	}
	stats.LastTime = time.Now()
}
