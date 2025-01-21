package main

import (
	"sync"
	"time"
)

type RateLimiter struct {
	mu      sync.Mutex
	windows map[string]*SlidingWindow
	config  *ServerConfig
}

type SlidingWindow struct {
	count     int
	startTime time.Time
}

func NewRateLimiter(config *ServerConfig) *RateLimiter {
	rl := &RateLimiter{
		windows: make(map[string]*SlidingWindow),
		config:  config,
	}
	go rl.cleanup()
	return rl
}

func (rl *RateLimiter) Allow(clientID string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	window, exists := rl.windows[clientID]

	if !exists || now.Sub(window.startTime) >= time.Second {
		rl.windows[clientID] = &SlidingWindow{
			count:     1,
			startTime: now,
		}
		return true
	}

	if window.count >= rl.config.RateLimit {
		return false
	}

	window.count++
	return true
}

func (rl *RateLimiter) cleanup() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		rl.mu.Lock()
		now := time.Now()
		for id, window := range rl.windows {
			if now.Sub(window.startTime) >= time.Second {
				delete(rl.windows, id)
			}
		}
		rl.mu.Unlock()
	}
}
