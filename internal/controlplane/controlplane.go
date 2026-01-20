// internal/controlplane/controlplane.go
package controlplane

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	pb "messageboard/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type ControlPlane struct {
	pb.UnimplementedControlPlaneServer

	// This needs to be locked when changing cluster state. When adding multiple nodes at the same time,
	// the second one needs to wait for the first one to finish registering.
	mu             sync.RWMutex
	nodes          map[string]*NodeState
	chain          []*pb.NodeInfo
	lastHeartbeats map[string]time.Time // For each node id, track the timestamp of its last heartbeat

	nodeClients map[string]pb.MessageBoardClient
	nodeConns   map[string]*grpc.ClientConn // Add this to track connections
}

type NodeState struct {
	Info    *pb.NodeInfo
	Healthy bool
	Syncing bool
}

func NewControlPlane() *ControlPlane {
	cp := &ControlPlane{
		nodes:          make(map[string]*NodeState),
		chain:          make([]*pb.NodeInfo, 0),
		lastHeartbeats: make(map[string]time.Time),
		nodeClients:    make(map[string]pb.MessageBoardClient),
		nodeConns:      make(map[string]*grpc.ClientConn),
	}

	go cp.monitorHealth()
	return cp
}

func (cp *ControlPlane) RegisterNode(_ context.Context, req *pb.RegisterNodeRequest) (*emptypb.Empty, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	log.Printf("Registering node: %s at %s", req.NodeId, req.Address)

	nodeInfo := &pb.NodeInfo{
		NodeId:  req.NodeId,
		Address: req.Address,
	}

	isFirst := len(cp.chain) == 0
	cp.nodes[req.NodeId] = &NodeState{
		Info:    nodeInfo,
		Healthy: true,
		Syncing: !isFirst,
	}

	cp.lastHeartbeats[req.NodeId] = time.Now()

	// Create a connection to the client for notifying about state updates.
	log.Printf("Establishing a connection for state updates to: %s at %s", req.NodeId, req.Address)
	conn, err := grpc.NewClient(req.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to create client for node %s: %v", req.NodeId, err)
	}

	cp.nodeClients[req.NodeId] = pb.NewMessageBoardClient(conn)
	cp.nodeConns[req.NodeId] = conn

	// Add to chain
	cp.chain = append(cp.chain, nodeInfo)
	cp.logReconfiguration()

	return &emptypb.Empty{}, nil
}

func (cp *ControlPlane) Heartbeat(_ context.Context, req *pb.HeartbeatRequest) (*emptypb.Empty, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	cp.lastHeartbeats[req.NodeId] = time.Now()

	if state, exists := cp.nodes[req.NodeId]; exists {
		state.Healthy = true
		// log.Printf("Received heartbeat from node: %s", req.NodeId)
	}

	return &emptypb.Empty{}, nil
}

func (cp *ControlPlane) GetClusterState(_ context.Context, _ *emptypb.Empty) (*pb.GetClusterStateResponse, error) {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	if len(cp.chain) == 0 {
		return nil, fmt.Errorf("no nodes in cluster")
	}

	var tail *pb.NodeInfo
	// Iterate backwards to find the last node that is not syncing
	for i := len(cp.chain) - 1; i >= 0; i-- {
		node := cp.chain[i]
		if state, exists := cp.nodes[node.NodeId]; exists && !state.Syncing {
			tail = node
			break
		}
	}
	if tail == nil {
		tail = cp.chain[len(cp.chain)-1]
	}

	response := &pb.GetClusterStateResponse{
		Head: cp.chain[0],
		Tail: tail,
	}

	return response, nil
}

func (cp *ControlPlane) GetChainState(_ context.Context, _ *emptypb.Empty) (*pb.ChainStateResponse, error) {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	return &pb.ChainStateResponse{
		Chain: cp.chain,
	}, nil
}

func (cp *ControlPlane) ConfirmSynced(_ context.Context, req *pb.ConfirmSyncedRequest) (*emptypb.Empty, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if state, exists := cp.nodes[req.NodeId]; exists {
		state.Syncing = false
		log.Printf("Node %s confirmed synced", req.NodeId)
		cp.notifyAllNodes() // It would be better to only notify the tail, but this is easier to write.
	}
	return &emptypb.Empty{}, nil
}

func (cp *ControlPlane) monitorHealth() {
	ticker := time.NewTicker(3 * time.Second)
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

	for nodeID, lastHB := range cp.lastHeartbeats {
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
		cp.logReconfiguration()

		cp.notifyAllNodes()
	}
}

func (cp *ControlPlane) rebuildChain() {
	newChain := make([]*pb.NodeInfo, 0)

	// Rebuild the chain by iterating through the current chain and only appending healthy nodes.
	// Unhealthy nodes get their connection closed but stay in cp.nodes.
	for _, node := range cp.chain {
		if state, exists := cp.nodes[node.NodeId]; exists && state.Healthy {
			newChain = append(newChain, node)
		} else if conn, exists := cp.nodeConns[node.NodeId]; exists {
			if err := conn.Close(); err != nil {
				log.Printf("Error closing connection for node %s: %v", node.NodeId, err)
			}
			delete(cp.nodeConns, node.NodeId)
		}
	}

	cp.chain = newChain
	log.Printf("Chain rebuilt: %d nodes", len(cp.chain))
}

func (cp *ControlPlane) logReconfiguration() {
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

func (cp *ControlPlane) notifyAllNodes() {
	for i, node := range cp.chain {
		var prevNode, nextNode string

		if i > 0 {
			prevNode = cp.chain[i-1].Address
		}
		if i < len(cp.chain)-1 {
			nextNode = cp.chain[i+1].Address
		}

		isHead := i == 0
		isTail := i == len(cp.chain)-1

		go cp.notifyNode(node.NodeId, prevNode, nextNode, isHead, isTail)
	}
}

func (cp *ControlPlane) notifyNode(nodeID string, predecessor, successor string, isHead, isTail bool) {
	client, exists := cp.nodeClients[nodeID]
	if !exists {
		log.Printf("No client found for node %s", nodeID)
		return
	}

	notification := &pb.StateChangeNotification{
		PrevNode: predecessor,
		NextNode: successor,
		IsHead:   isHead,
		IsTail:   isTail,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	log.Printf("Notifying client %s of state change: %v", nodeID, notification)
	_, err := client.NotifyStateChange(ctx, notification)
	if err != nil {
		log.Printf("Failed to notify node %s: %v", nodeID, err)
	}
}
