package main

import (
	"sync"
	"time"

	"github.com/tidwall/resp"
)

type PubSubManager struct {
	mu              sync.RWMutex
	channels        map[string]map[*Peer]bool
	patterns        map[string]map[*Peer]bool
	subscriberCount map[*Peer]int
	metrics         *Metrics
	config          *ServerConfig
}

type PubSubMessage struct {
	Channel string
	Pattern string
	Message []byte
	Time    time.Time
}

func NewPubSubManager(config *ServerConfig, metrics *Metrics) *PubSubManager {
	ps := &PubSubManager{
		channels:        make(map[string]map[*Peer]bool),
		patterns:        make(map[string]map[*Peer]bool),
		subscriberCount: make(map[*Peer]int),
		metrics:         metrics,
		config:          config,
	}
	go ps.cleanup()
	return ps
}

func (ps *PubSubManager) Publish(channel string, message []byte) int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	count := 0
	msg := &PubSubMessage{
		Channel: channel,
		Message: message,
		Time:    time.Now(),
	}

	if subscribers := ps.channels[channel]; subscribers != nil {
		for peer := range subscribers {
			if err := ps.sendMessage(peer, msg); err == nil {
				count++
			}
		}
	}

	for pattern, subscribers := range ps.patterns {
		if ps.patternMatch(pattern, channel) {
			msg.Pattern = pattern
			for peer := range subscribers {
				if err := ps.sendMessage(peer, msg); err == nil {
					count++
				}
			}
		}
	}

	return count
}

func (ps *PubSubManager) Subscribe(peer *Peer, channels ...string) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	for _, channel := range channels {
		if ps.channels[channel] == nil {
			ps.channels[channel] = make(map[*Peer]bool)
		}
		ps.channels[channel][peer] = true
		ps.subscriberCount[peer]++
	}

	peer.EnterSubscribedMode()

	writer := resp.NewWriter(peer.conn)
	for _, channel := range channels {
		writer.WriteArray([]resp.Value{
			resp.StringValue("subscribe"),
			resp.StringValue(channel),
			resp.IntegerValue(int(ps.subscriberCount[peer])),
		})
	}

	return nil
}

func (p *Peer) EnterSubscribedMode() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inSubscribedMode = true
}

func (ps *PubSubManager) Unsubscribe(peer *Peer, channels ...string) (int, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if len(channels) == 0 {

		for channel, subscribers := range ps.channels {
			if subscribers[peer] {
				delete(subscribers, peer)
				ps.subscriberCount[peer]--
				if len(subscribers) == 0 {
					delete(ps.channels, channel)
				}
			}
		}
	} else {

		for _, channel := range channels {
			if subscribers := ps.channels[channel]; subscribers != nil {
				if subscribers[peer] {
					delete(subscribers, peer)
					ps.subscriberCount[peer]--
					if len(subscribers) == 0 {
						delete(ps.channels, channel)
					}
				}
			}
		}
	}

	if ps.subscriberCount[peer] == 0 {
		delete(ps.subscriberCount, peer)

		peer.ExitSubscribedMode()
	}

	return ps.subscriberCount[peer], nil
}

func (ps *PubSubManager) PSubscribe(peer *Peer, patterns ...string) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	for _, pattern := range patterns {
		if ps.patterns[pattern] == nil {
			ps.patterns[pattern] = make(map[*Peer]bool)
		}
		ps.patterns[pattern][peer] = true
		ps.subscriberCount[peer]++
	}

	writer := resp.NewWriter(peer.conn)
	for _, pattern := range patterns {
		writer.WriteArray([]resp.Value{

			resp.StringValue("psubscribe"),
			resp.StringValue(pattern),
			resp.IntegerValue(int(ps.subscriberCount[peer])),
		})
	}

	return nil
}

func (ps *PubSubManager) sendMessage(peer *Peer, msg *PubSubMessage) error {
	writer := resp.NewWriter(peer.conn)

	if msg.Pattern != "" {
		return writer.WriteArray([]resp.Value{
			resp.StringValue("pmessage"),
			resp.StringValue(msg.Pattern),
			resp.StringValue(msg.Channel),
			resp.BytesValue(msg.Message),
		})
	}

	return writer.WriteArray([]resp.Value{
		resp.StringValue("message"),
		resp.StringValue(msg.Channel),
		resp.BytesValue(msg.Message),
	})
}

func (ps *PubSubManager) cleanup() {
	ticker := time.NewTicker(ps.config.CleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		ps.mu.Lock()
		for peer := range ps.subscriberCount {
			if !peer.IsActive() {
				ps.Unsubscribe(peer)
				for pattern := range ps.patterns {
					if ps.patterns[pattern][peer] {
						delete(ps.patterns[pattern], peer)
					}
				}
				delete(ps.subscriberCount, peer)
			}
		}
		ps.mu.Unlock()
	}
}
func (ps *PubSubManager) patternMatch(pattern, channel string) bool {
	patternRunes := []rune(pattern)
	channelRunes := []rune(channel)

	return ps.matchInternal(patternRunes, channelRunes, 0, 0)
}

func (ps *PubSubManager) matchInternal(pattern, channel []rune, pi, ci int) bool {
	for pi < len(pattern) && ci < len(channel) {
		switch pattern[pi] {
		case '*':

			for pi+1 < len(pattern) && pattern[pi+1] == '*' {
				pi++
			}

			for i := ci; i <= len(channel); i++ {
				if ps.matchInternal(pattern, channel, pi+1, i) {
					return true
				}
			}
			return false
		case '?':
			pi++
			ci++
		default:
			if pattern[pi] != channel[ci] {
				return false
			}
			pi++
			ci++
		}
	}
	return pi == len(pattern) && ci == len(channel)
}
