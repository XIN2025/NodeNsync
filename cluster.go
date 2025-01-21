package main

import (
	"fmt"
	"hash/fnv"
	"sync"
	"time"
)

const (
	TotalSlots = 16384
)

type NodeRole string

const (
	RoleLeader   NodeRole = "leader"
	RoleFollower NodeRole = "follower"
)

type Node struct {
	ID        string
	Address   string
	Role      NodeRole
	Slots     []int
	JoinedAt  time.Time
	LastSeen  time.Time
	IsHealthy bool
}

type ClusterManager struct {
	mu    sync.RWMutex
	nodes map[string]*Node

	slotMapping [TotalSlots]string

	currentLeader string
	term          uint64

	heartbeatInterval time.Duration
	nodeTimeout       time.Duration
}

func NewClusterManager() *ClusterManager {
	cm := &ClusterManager{
		nodes:             make(map[string]*Node),
		heartbeatInterval: 1 * time.Second,
		nodeTimeout:       5 * time.Second,
	}
	go cm.healthCheck()
	return cm
}

func (cm *ClusterManager) AddNode(address string) (*Node, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	nodeID := generateNodeID(address)

	if _, exists := cm.nodes[nodeID]; exists {
		return nil, fmt.Errorf("node already exists")
	}

	node := &Node{
		ID:        nodeID,
		Address:   address,
		Role:      RoleFollower,
		JoinedAt:  time.Now(),
		LastSeen:  time.Now(),
		IsHealthy: true,
	}

	cm.nodes[nodeID] = node

	if len(cm.nodes) == 1 {
		cm.promoteToLeader(nodeID)
	} else {
		cm.rebalanceSlots()
	}

	return node, nil
}

func (cm *ClusterManager) RemoveNode(nodeID string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	node, exists := cm.nodes[nodeID]
	if !exists {
		return fmt.Errorf("node not found")
	}

	if node.Role == RoleLeader {
		cm.electNewLeader()
	}

	delete(cm.nodes, nodeID)
	cm.rebalanceSlots()
	return nil
}

func (cm *ClusterManager) GetNodeForSlot(slot int) (*Node, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if slot < 0 || slot >= TotalSlots {
		return nil, fmt.Errorf("invalid slot number")
	}

	nodeID := cm.slotMapping[slot]
	node, exists := cm.nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node not found for slot")
	}

	return node, nil
}

func generateNodeID(address string) string {
	h := fnv.New64a()
	h.Write([]byte(address))
	return fmt.Sprintf("node:%x", h.Sum64())
}

func (cm *ClusterManager) promoteToLeader(nodeID string) {
	node := cm.nodes[nodeID]
	node.Role = RoleLeader
	cm.currentLeader = nodeID
	cm.term++
}

func (cm *ClusterManager) electNewLeader() {

	var oldestNode *Node
	var oldestTime time.Time

	for _, node := range cm.nodes {
		if node.Role != RoleLeader && node.IsHealthy {
			if oldestNode == nil || node.JoinedAt.Before(oldestTime) {
				oldestNode = node
				oldestTime = node.JoinedAt
			}
		}
	}

	if oldestNode != nil {
		cm.promoteToLeader(oldestNode.ID)
	}
}

func (cm *ClusterManager) rebalanceSlots() {

	for i := range cm.slotMapping {
		cm.slotMapping[i] = ""
	}

	healthyNodes := 0
	for _, node := range cm.nodes {
		if node.IsHealthy {
			healthyNodes++
		}
	}

	if healthyNodes == 0 {
		return
	}

	slotsPerNode := TotalSlots / healthyNodes
	remainingSlots := TotalSlots % healthyNodes

	currentSlot := 0
	for _, node := range cm.nodes {
		if !node.IsHealthy {
			continue
		}

		slots := slotsPerNode
		if remainingSlots > 0 {
			slots++
			remainingSlots--
		}

		for i := 0; i < slots; i++ {
			cm.slotMapping[currentSlot] = node.ID
			currentSlot++
		}
	}
}

func (cm *ClusterManager) healthCheck() {
	ticker := time.NewTicker(cm.heartbeatInterval)
	for range ticker.C {
		cm.mu.Lock()
		now := time.Now()

		needRebalance := false
		for _, node := range cm.nodes {
			wasHealthy := node.IsHealthy
			node.IsHealthy = now.Sub(node.LastSeen) < cm.nodeTimeout

			if wasHealthy != node.IsHealthy {
				needRebalance = true
			}
		}

		if needRebalance {
			cm.rebalanceSlots()
		}

		cm.mu.Unlock()
	}
}
