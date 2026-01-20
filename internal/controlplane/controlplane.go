// internal/controlplane/controlplane.go
package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	pb "messageboard/proto"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// This map defines the static list of peers in the Raft cluster.
var raftPeers = map[string]string{
	"node1": "localhost:60051",
	"node2": "localhost:60052",
	"node3": "localhost:60053",
}

type ControlPlane struct {
	pb.UnimplementedControlPlaneServer

	mu               sync.RWMutex
	nodes            map[string]*NodeState
	chain            []*pb.NodeInfo
	lastHeartbeats   map[string]time.Time
	heartbeatTimeout time.Duration

	nodeClients map[string]pb.MessageBoardClient
	nodeConns   map[string]*grpc.ClientConn

	raft         *raft.Raft
	leadershipCh chan bool
}

type NodeState struct {
	Info    *pb.NodeInfo
	Healthy bool
	Syncing bool
}

func NewControlPlane() *ControlPlane {
	cp := &ControlPlane{
		nodes:            make(map[string]*NodeState),
		chain:            make([]*pb.NodeInfo, 0),
		lastHeartbeats:   make(map[string]time.Time),
		heartbeatTimeout: 15 * time.Second,
		nodeClients:      make(map[string]pb.MessageBoardClient),
		nodeConns:        make(map[string]*grpc.ClientConn),
		leadershipCh:     make(chan bool, 1),
	}

	go cp.monitorDataPlaneHealth()
	go cp.leaderReconciliationLoop()

	return cp
}

func (cp *ControlPlane) leaderReconciliationLoop() {
	for isLeader := range cp.leadershipCh {
		if isLeader {
			log.Println("Assumed leadership. Starting Raft peer reconciliation.")
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()

		Outer:
			for {
				cp.reconcileRaftPeers()
				select {
				case <-ticker.C:
					cp.reconcileRaftPeers()
				case isLeader := <-cp.leadershipCh:
					if !isLeader {
						log.Println("Lost leadership. Stopping Raft peer reconciliation.")
						break Outer
					}
				}
			}
		}
	}
}

func (cp *ControlPlane) reconcileRaftPeers() {
	if cp.raft == nil || cp.raft.State() != raft.Leader {
		return
	}

	configFuture := cp.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		log.Printf("Error getting Raft configuration for reconciliation: %v", err)
		return
	}
	config := configFuture.Configuration()
	currentServers := make(map[raft.ServerID]bool)
	for _, srv := range config.Servers {
		currentServers[srv.ID] = true
	}

	// Add missing peers
	for id, addr := range raftPeers {
		serverID := raft.ServerID(id)
		if _, exists := currentServers[serverID]; !exists {
			log.Printf("Reconciliation: Peer %s is missing from Raft config. Attempting to add.", id)
			conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
			if err != nil {
				log.Printf("Reconciliation: Peer %s at %s is not reachable, skipping add: %v", id, addr, err)
				continue
			}
			conn.Close()

			addFuture := cp.raft.AddVoter(serverID, raft.ServerAddress(addr), 0, 0)
			if err := addFuture.Error(); err != nil {
				log.Printf("Reconciliation: Error adding peer %s: %v", id, err)
			} else {
				log.Printf("Reconciliation: Successfully added peer %s to the cluster.", id)
			}
		}
	}

	// Remove extra peers
	for _, srv := range config.Servers {
		if _, exists := raftPeers[string(srv.ID)]; !exists {
			log.Printf("Reconciliation: Peer %s is in Raft config but not in static peer list. Attempting to remove.", srv.ID)
			removeFuture := cp.raft.RemoveServer(srv.ID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				log.Printf("Reconciliation: Error removing peer %s: %v", srv.ID, err)
			} else {
				log.Printf("Reconciliation: Successfully removed peer %s from the cluster.", srv.ID)
			}
		}
	}
}

func (cp *ControlPlane) monitorDataPlaneHealth() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if cp.raft != nil && cp.raft.State() == raft.Leader {
			cp.checkDataPlaneNodeHealth()
		}
	}
}

func (cp *ControlPlane) checkDataPlaneNodeHealth() {
	cp.mu.RLock()
	var unhealthyNodes []string
	now := time.Now()
	for nodeID, lastHB := range cp.lastHeartbeats {
		if now.Sub(lastHB) > cp.heartbeatTimeout {
			if state, exists := cp.nodes[nodeID]; exists && state.Healthy {
				log.Printf("Leader detected data plane node %s as unhealthy.", nodeID)
				unhealthyNodes = append(unhealthyNodes, nodeID)
			}
		}
	}
	cp.mu.RUnlock()

	if len(unhealthyNodes) > 0 {
		cp.removeDataPlaneNodes(unhealthyNodes)
	}
}

func (cp *ControlPlane) removeDataPlaneNodes(nodeIDs []string) {
	var removalApplied bool
	for _, nodeID := range nodeIDs {
		cmdData, err := json.Marshal(nodeID)
		if err != nil {
			log.Printf("Error marshalling remove command data for %s: %v", nodeID, err)
			continue
		}
		cmd := &Command{Op: "remove", Data: cmdData}
		cmdBytes, err := json.Marshal(cmd)
		if err != nil {
			log.Printf("Error marshalling remove command for %s: %v", nodeID, err)
			continue
		}

		log.Printf("Leader proposing removal of data plane node %s", nodeID)
		f := cp.raft.Apply(cmdBytes, 500*time.Millisecond)
		if err := f.Error(); err != nil {
			log.Printf("Failed to apply removal for data plane node %s: %v", nodeID, err)
		} else {
			removalApplied = true
		}
	}

	if removalApplied {
		time.Sleep(250 * time.Millisecond)
		cp.mu.RLock()
		defer cp.mu.RUnlock()
		cp.notifyAllNodes()
	}
}

func (cp *ControlPlane) GetLeader(
	_ context.Context,
	_ *emptypb.Empty,
) (*pb.LeaderResponse, error) {

	if cp.raft == nil {
		return nil, status.Error(codes.Unavailable, "raft not initialized")
	}

	leaderAddr, leaderID := cp.raft.LeaderWithID()

	if leaderAddr == "" {
		return nil, status.Error(codes.Unavailable, "no leader elected")
	}

	return &pb.LeaderResponse{
		RaftId:  string(leaderID),
		Address: string(leaderAddr),
	}, nil
}

func (cp *ControlPlane) RegisterNode(_ context.Context, req *pb.RegisterNodeRequest) (*emptypb.Empty, error) {
	if cp.raft.State() != raft.Leader {
		leaderAddr, _ := cp.raft.LeaderWithID()
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"NOT_LEADER:%s",
			leaderAddr,
		)
	}

	cmdData, err := json.Marshal(&pb.NodeInfo{NodeId: req.NodeId, Address: req.Address})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal node info for raft: %v", err)
	}

	cmd := &Command{Op: "register", Data: cmdData}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal command: %v", err)
	}

	f := cp.raft.Apply(cmdBytes, 500*time.Millisecond)
	if err := f.Error(); err != nil {
		return nil, fmt.Errorf("raft apply failed: %v", err)
	}

	cp.mu.RLock()
	defer cp.mu.RUnlock()
	cp.notifyAllNodes()

	return &emptypb.Empty{}, nil
}

func (cp *ControlPlane) applyRegisterNode(nodeInfo *pb.NodeInfo) {
	cp.lastHeartbeats[nodeInfo.NodeId] = time.Now()

	if state, exists := cp.nodes[nodeInfo.NodeId]; exists {
		log.Printf("Node %s re-registered. Updating info and resetting health status.", nodeInfo.NodeId)
		state.Info = nodeInfo
		state.Healthy = true

		if oldConn, connExists := cp.nodeConns[nodeInfo.NodeId]; connExists {
			_ = oldConn.Close()
		}
		conn, err := grpc.NewClient(nodeInfo.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Failed to create client for re-registered node %s: %v", nodeInfo.NodeId, err)
			return
		}
		cp.nodeClients[nodeInfo.NodeId] = pb.NewMessageBoardClient(conn)
		cp.nodeConns[nodeInfo.NodeId] = conn

		for i, n := range cp.chain {
			if n.NodeId == nodeInfo.NodeId {
				cp.chain[i] = nodeInfo
				break
			}
		}

	} else {
		log.Printf("Applying registration for new node: %s at %s", nodeInfo.NodeId, nodeInfo.Address)
		isFirst := len(cp.chain) == 0
		cp.nodes[nodeInfo.NodeId] = &NodeState{
			Info:    nodeInfo,
			Healthy: true,
			Syncing: !isFirst,
		}

		conn, err := grpc.NewClient(nodeInfo.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Failed to create client for node %s: %v", nodeInfo.NodeId, err)
			return
		}
		cp.nodeClients[nodeInfo.NodeId] = pb.NewMessageBoardClient(conn)
		cp.nodeConns[nodeInfo.NodeId] = conn

		cp.chain = append(cp.chain, nodeInfo)
	}

	cp.logReconfiguration()
}

func (cp *ControlPlane) Heartbeat(_ context.Context, req *pb.HeartbeatRequest) (*emptypb.Empty, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.lastHeartbeats[req.NodeId] = time.Now()
	if state, exists := cp.nodes[req.NodeId]; exists {
		state.Healthy = true
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
	for i := len(cp.chain) - 1; i >= 0; i-- {
		node := cp.chain[i]
		if state, exists := cp.nodes[node.NodeId]; exists && !state.Syncing {
			tail = node
			break
		}
	}
	if tail == nil && len(cp.chain) > 0 {
		tail = cp.chain[len(cp.chain)-1]
	}

	return &pb.GetClusterStateResponse{
		Head: cp.chain[0],
		Tail: tail,
	}, nil
}

func (cp *ControlPlane) GetChainState(_ context.Context, _ *emptypb.Empty) (*pb.ChainStateResponse, error) {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	return &pb.ChainStateResponse{Chain: cp.chain}, nil
}

func (cp *ControlPlane) ConfirmSynced(_ context.Context, req *pb.ConfirmSyncedRequest) (*emptypb.Empty, error) {
	cmdData, err := json.Marshal(req.NodeId)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal node id for raft: %v", err)
	}
	cmd := &Command{Op: "synced", Data: cmdData}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal sync command: %v", err)
	}

	f := cp.raft.Apply(cmdBytes, 500*time.Millisecond)
	if err := f.Error(); err != nil {
		return nil, fmt.Errorf("raft apply for sync failed: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (cp *ControlPlane) applyConfirmSynced(nodeID string) {
	if state, exists := cp.nodes[nodeID]; exists {
		state.Syncing = false
		log.Printf("Node %s confirmed synced", nodeID)
	}
}

func (cp *ControlPlane) applyRemoveNode(nodeID string) {
	if _, exists := cp.nodes[nodeID]; !exists {
		return
	}

	log.Printf("Applying removal of node: %s", nodeID)
	newChain := make([]*pb.NodeInfo, 0, len(cp.chain)-1)
	for _, node := range cp.chain {
		if node.NodeId != nodeID {
			newChain = append(newChain, node)
		}
	}

	cp.chain = newChain
	delete(cp.nodes, nodeID)
	delete(cp.lastHeartbeats, nodeID)
	if conn, exists := cp.nodeConns[nodeID]; exists {
		_ = conn.Close()
		delete(cp.nodeConns, nodeID)
		delete(cp.nodeClients, nodeID)
	}
	cp.logReconfiguration()
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
	_, err := client.NotifyStateChange(ctx, notification)
	if err != nil {
		log.Printf("Failed to notify node %s: %v", nodeID, err)
	}
}
