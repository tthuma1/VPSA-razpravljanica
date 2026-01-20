package controlplane

import (
	"encoding/json"
	"io"
	"log"
	"time"

	"github.com/hashicorp/raft"
	pb "messageboard/proto"
)

// FSMWrapper wraps ControlPlane to implement raft.FSM
type FSMWrapper struct {
	cp *ControlPlane
}

// Command is the structure that is written to the Raft log.
type Command struct {
	Op   string          `json:"op"`
	Data json.RawMessage `json:"data"`
}

type SnapshotState struct {
	Chain []*pb.NodeInfo
	Nodes map[string]*NodeState
}

func (f *FSMWrapper) Apply(l *raft.Log) interface{} {
	var c Command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		log.Printf("failed to unmarshal command: %v", err)
		return nil
	}

	f.cp.mu.Lock()
	defer f.cp.mu.Unlock()

	switch c.Op {
	case "register":
		var nodeInfo pb.NodeInfo
		if err := json.Unmarshal(c.Data, &nodeInfo); err != nil {
			log.Printf("failed to unmarshal register data: %v", err)
			return nil
		}
		f.cp.applyRegisterNode(&nodeInfo)
	case "remove":
		var nodeID string
		if err := json.Unmarshal(c.Data, &nodeID); err != nil {
			log.Printf("failed to unmarshal remove data: %v", err)
			return nil
		}
		f.cp.applyRemoveNode(nodeID)
	case "synced":
		var nodeID string
		if err := json.Unmarshal(c.Data, &nodeID); err != nil {
			log.Printf("failed to unmarshal synced data: %v", err)
			return nil
		}
		f.cp.applyConfirmSynced(nodeID)
	}
	return nil
}

func (f *FSMWrapper) Snapshot() (raft.FSMSnapshot, error) {
	f.cp.mu.RLock()
	defer f.cp.mu.RUnlock()

	nodes := make(map[string]*NodeState)
	for k, v := range f.cp.nodes {
		infoCopy := *v.Info
		nodes[k] = &NodeState{
			Info:    &infoCopy,
			Healthy: v.Healthy,
			Syncing: v.Syncing,
		}
	}

	chain := make([]*pb.NodeInfo, len(f.cp.chain))
	for i, n := range f.cp.chain {
		infoCopy := *n
		chain[i] = &infoCopy
	}

	return &fsmSnapshot{chain: chain, nodes: nodes}, nil
}

func (f *FSMWrapper) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var state SnapshotState
	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		return err
	}

	f.cp.mu.Lock()
	defer f.cp.mu.Unlock()

	f.cp.chain = state.Chain
	f.cp.nodes = state.Nodes
	f.cp.lastHeartbeats = make(map[string]time.Time)
	for id := range f.cp.nodes {
		f.cp.lastHeartbeats[id] = time.Now()
	}

	return nil
}

type fsmSnapshot struct {
	chain []*pb.NodeInfo
	nodes map[string]*NodeState
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		b, err := json.Marshal(SnapshotState{Chain: f.chain, Nodes: f.nodes})
		if err != nil {
			return err
		}
		if _, err := sink.Write(b); err != nil {
			return err
		}
		return nil
	}()
	if err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (f *fsmSnapshot) Release() {}
