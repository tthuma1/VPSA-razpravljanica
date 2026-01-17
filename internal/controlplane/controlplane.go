// internal/controlplane/controlplane.go
package controlplane

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	pb "messageboard/proto"

	"google.golang.org/protobuf/types/known/emptypb"
)

type ControlPlane struct {
	pb.UnimplementedControlPlaneServer

	mu            sync.RWMutex
	nodes         map[string]*NodeState
	chain         []*pb.NodeInfo
	lastHeartbeat map[string]time.Time
}

type NodeState struct {
	Info     *pb.NodeInfo
	Healthy  bool
	JoinedAt time.Time
	Syncing  bool
}

func NewControlPlane() *ControlPlane {
	cp := &ControlPlane{
		nodes:         make(map[string]*NodeState),
		chain:         make([]*pb.NodeInfo, 0),
		lastHeartbeat: make(map[string]time.Time),
	}

	go cp.monitorHealth()
	return cp
}

func (cp *ControlPlane) RegisterNode(ctx context.Context, req *pb.RegisterNodeRequest) (*emptypb.Empty, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	log.Printf("Registering node: %s at %s", req.NodeId, req.Address)

	nodeInfo := &pb.NodeInfo{
		NodeId:  req.NodeId,
		Address: req.Address,
	}

	isFirst := len(cp.chain) == 0
	cp.nodes[req.NodeId] = &NodeState{
		Info:     nodeInfo,
		Healthy:  true,
		JoinedAt: time.Now(),
		Syncing:  !isFirst,
	}

	cp.lastHeartbeat[req.NodeId] = time.Now()

	// Add to chain
	cp.chain = append(cp.chain, nodeInfo)
	cp.reconfigureChain()

	return &emptypb.Empty{}, nil
}

func (cp *ControlPlane) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*emptypb.Empty, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	cp.lastHeartbeat[req.NodeId] = time.Now()

	if state, exists := cp.nodes[req.NodeId]; exists {
		state.Healthy = true
	}

	return &emptypb.Empty{}, nil
}

func (cp *ControlPlane) GetClusterState(ctx context.Context, _ *emptypb.Empty) (*pb.GetClusterStateResponse, error) {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	if len(cp.chain) == 0 {
		return nil, fmt.Errorf("no nodes in cluster")
	}

	response := &pb.GetClusterStateResponse{
		Head: cp.chain[0],
		Tail: cp.chain[len(cp.chain)-1],
	}

	return response, nil
}

func (cp *ControlPlane) GetChainState(ctx context.Context, _ *emptypb.Empty) (*pb.ChainStateResponse, error) {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	return &pb.ChainStateResponse{
		Chain: cp.chain,
	}, nil
}

func (cp *ControlPlane) ConfirmSynced(ctx context.Context, req *pb.ConfirmSyncedRequest) (*emptypb.Empty, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if state, exists := cp.nodes[req.NodeId]; exists {
		state.Syncing = false
		log.Printf("Node %s confirmed synced", req.NodeId)
	}
	return &emptypb.Empty{}, nil
}

func (cp *ControlPlane) monitorHealth() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		cp.checkHealth()
	}
}

func (cp *ControlPlane) checkHealth() {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	now := time.Now()
	healthChanged := false

	for nodeID, lastHB := range cp.lastHeartbeat {
		if now.Sub(lastHB) > 5*time.Second {
			if state, exists := cp.nodes[nodeID]; exists && state.Healthy {
				log.Printf("Node %s marked unhealthy", nodeID)
				state.Healthy = false
				healthChanged = true
			}
		}
	}

	if healthChanged {
		cp.rebuildChain()
		cp.reconfigureChain()
	}
}

func (cp *ControlPlane) rebuildChain() {
	newChain := make([]*pb.NodeInfo, 0)

	for _, node := range cp.chain {
		if state, exists := cp.nodes[node.NodeId]; exists && state.Healthy {
			newChain = append(newChain, node)
		}
	}

	cp.chain = newChain
	log.Printf("Chain rebuilt: %d nodes", len(cp.chain))
}

func (cp *ControlPlane) reconfigureChain() {
	// In a production system, this would send RPCs to nodes to update their
	// next pointer and role. For simplicity, we'll log the configuration.
	if len(cp.chain) == 0 {
		log.Println("Chain is empty")
		return
	}

	log.Printf("Chain configuration:")
	for i, node := range cp.chain {
		role := "MIDDLE"
		if i == 0 {
			role = "HEAD"
		}
		if i == len(cp.chain)-1 {
			role = "TAIL"
		}

		nextAddr := ""
		if i < len(cp.chain)-1 {
			nextAddr = cp.chain[i+1].Address
		}

		log.Printf("  [%d] %s (%s) -> %s, next: %s", i, node.NodeId, node.Address, role, nextAddr)
	}
}
