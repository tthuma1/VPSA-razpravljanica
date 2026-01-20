package controlplane

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

func (cp *ControlPlane) SetupRaft(nodeID, raftAddr, raftDir string, bootstrap bool) error {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)
	config.SnapshotInterval = 20 * time.Second
	config.SnapshotThreshold = 2
	config.NotifyCh = cp.leadershipCh

	addr, err := net.ResolveTCPAddr("tcp", raftAddr)
	if err != nil {
		return err
	}

	transport, err := raft.NewTCPTransport(raftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	snapshots, err := raft.NewFileSnapshotStore(raftDir, 2, os.Stderr)
	if err != nil {
		return err
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft-log.bolt"))
	if err != nil {
		return err
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft-stable.bolt"))
	if err != nil {
		return err
	}

	fsm := &FSMWrapper{cp: cp}

	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		return err
	}

	cp.raft = r

	if bootstrap {
		log.Printf("Bootstrapping cluster with %s", nodeID)
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{ID: config.LocalID, Address: transport.LocalAddr()},
			},
		}
		f := r.BootstrapCluster(configuration)
		if err := f.Error(); err != nil {
			return err
		}
	}

	return nil
}

func (cp *ControlPlane) Join(nodeID, addr string) error {
	log.Printf("Received join request for remote node %s at %s", nodeID, addr)

	if cp.raft.State() != raft.Leader {
		leaderAddr, leaderID := cp.raft.LeaderWithID()
		if leaderID == "" {
			return fmt.Errorf("no leader elected, cannot join cluster")
		}
		return fmt.Errorf("not the leader, please forward join request to %s at %s", leaderID, leaderAddr)
	}

	log.Printf("Leader received join request for %s. The reconciliation loop will handle it.", nodeID)
	// The leader's reconciliation loop will eventually add the node.
	// We can trigger it here for a faster join, or just let it run periodically.
	go cp.reconcileRaftPeers()

	return nil
}
