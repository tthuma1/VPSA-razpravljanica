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
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
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

	configFuture := cp.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		log.Printf("failed to get raft configuration: %v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				log.Printf("node %s at %s already member of cluster, ignoring join request", nodeID, addr)
				return nil
			}
			future := cp.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s at %s: %s", nodeID, addr, err)
			}
		}
	}

	f := cp.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}
	log.Printf("node %s at %s joined successfully", nodeID, addr)
	return nil
}
