package test

import (
	"context"
	"fmt"
	"log"
	"messageboard/internal/controlplane"
	"messageboard/internal/dataplane"
	pb "messageboard/proto"
	"net"
	"os"
	"testing"
	"time"

	"messageboard/internal/client"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Helper to start a control plane with fixed ports matching the static config
func startControlPlaneTailTestFixed(t *testing.T, baseGrpcPort int) ([]*grpc.Server, string) {
	var servers []*grpc.Server
	var addrs []string

	// The static config in controlplane.go expects:
	// node1: localhost:60051
	// node2: localhost:60052
	// node3: localhost:60053
	raftPorts := []int{60051, 60052, 60053}

	for i := 0; i < 3; i++ {
		nodeID := fmt.Sprintf("node%d", i+1)
		grpcPort := baseGrpcPort + i
		raftPort := raftPorts[i]

		lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", grpcPort))
		if err != nil {
			t.Fatalf("failed to listen: %v", err)
		}

		raftDir, err := os.MkdirTemp("", fmt.Sprintf("raft-tail-fail-test-%s-*", nodeID))
		if err != nil {
			t.Fatalf("failed to create raft dir: %v", err)
		}
		t.Cleanup(func() { os.RemoveAll(raftDir) })

		s := grpc.NewServer()
		cp := controlplane.NewControlPlane()

		raftAddr := fmt.Sprintf("localhost:%d", raftPort)
		// Bootstrap only the first node
		bootstrap := i == 0
		if err := cp.SetupRaft(nodeID, raftAddr, raftDir, bootstrap); err != nil {
			t.Fatalf("failed to setup raft: %v", err)
		}

		pb.RegisterControlPlaneServer(s, cp)
		go func() {
			if err := s.Serve(lis); err != nil {
				// t.Errorf("failed to serve: %v", err)
			}
		}()

		servers = append(servers, s)
		addrs = append(addrs, lis.Addr().String())
	}

	// Wait for cluster to form
	time.Sleep(5 * time.Second)

	return servers, fmt.Sprintf("%s,%s,%s", addrs[0], addrs[1], addrs[2])
}

// Improved node starter that returns a stop function
func startNodeWithStopperTailTest(t *testing.T, id string, port int, controlAddr string, dbPath string, testing bool) (*grpc.Server, *dataplane.Node, func()) {
	// Clean up DB if exists
	os.Remove(dbPath)

	node, err := dataplane.NewNode(id, fmt.Sprintf("localhost:%d", port), dbPath, controlAddr, testing)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterMessageBoardServer(s, node)
	pb.RegisterReplicationServer(s, node)

	go func() {
		if err := s.Serve(lis); err != nil {
			// t.Errorf("failed to serve: %v", err)
		}
	}()

	stopChan := make(chan struct{})

	// Register with control plane
	go func() {
		// Parse controlAddr to find a working CP node
		// We'll try the first one for simplicity, assuming it's the leader or redirects
		targetCP := controlAddr
		for i, c := range controlAddr {
			if c == ',' {
				targetCP = controlAddr[:i]
				break
			}
		}

		conn, err := grpc.NewClient(targetCP, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return
		}
		defer conn.Close()
		controlPlaneClient := pb.NewControlPlaneClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		controlPlaneClient.RegisterNode(ctx, &pb.RegisterNodeRequest{
			NodeId:  id,
			Address: fmt.Sprintf("localhost:%d", port),
		})

		// Start heartbeat loop
		ticker := time.NewTicker(500 * time.Millisecond) // Fast heartbeat
		defer ticker.Stop()

		hasSynced := false

		for {
			select {
			case <-stopChan:
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				_, err := controlPlaneClient.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: id})
				cancel()

				if err != nil {
					continue
				}

				state, err := controlPlaneClient.GetClusterState(context.Background(), &emptypb.Empty{})
				if err == nil {
					var role dataplane.NodeRole
					var nextNode, prevNode string

					chainState, _ := controlPlaneClient.GetChainState(context.Background(), &emptypb.Empty{})

					if state.Head.NodeId == id {
						role = dataplane.RoleHead
					} else if state.Tail.NodeId == id {
						role = dataplane.RoleTail
					} else {
						role = dataplane.RoleMiddle
					}

					for i, n := range chainState.Chain {
						if n.NodeId == id {
							if i < len(chainState.Chain)-1 {
								nextNode = chainState.Chain[i+1].Address
							}
							if i > 0 {
								prevNode = chainState.Chain[i-1].Address
							}
							break
						}
					}
					node.SetRole(role, nextNode, prevNode)

					if !hasSynced {
						// Sync
						syncCtx, syncCancel := context.WithTimeout(context.Background(), 5*time.Second)
						if err := node.SyncWithTail(syncCtx); err == nil {
							hasSynced = true
						}
						syncCancel()
					}
				}
			}
		}
	}()

	return s, node, func() {
		close(stopChan)
		s.Stop()
		node.Close()
	}
}

func TestTailFailScenario(t *testing.T) {
	// 1. Start Control Plane (Cluster of 3)
	// We use ports 50056, 50057, 50058 for gRPC
	// And 60051, 60052, 60053 for Raft (fixed in startControlPlaneTailTestFixed)
	cpServers, cpAddr := startControlPlaneTailTestFixed(t, 50056)
	defer func() {
		for _, s := range cpServers {
			s.Stop()
		}
	}()

	// 2. Start 4 Nodes
	// Node 1
	_, _, stop1 := startNodeWithStopperTailTest(t, "node1", 50071, cpAddr, "test_tail_node1.db", false)
	defer func() {
		os.Remove("test_tail_node1.db")
		stop1()
	}()

	time.Sleep(1 * time.Second)

	// Node 2
	_, _, stop2 := startNodeWithStopperTailTest(t, "node2", 50072, cpAddr, "test_tail_node2.db", false)
	defer func() {
		os.Remove("test_tail_node2.db")
		stop2()
	}()

	time.Sleep(1 * time.Second)

	// Node 3
	_, _, stop3 := startNodeWithStopperTailTest(t, "node3", 50073, cpAddr, "test_tail_node3.db", false)
	defer func() {
		os.Remove("test_tail_node3.db")
		stop3()
	}()

	time.Sleep(1 * time.Second)

	// Node 4 (Tail, testing=true)
	_, _, stop4 := startNodeWithStopperTailTest(t, "node4", 50074, cpAddr, "test_tail_node4.db", true)

	node4Stopped := false
	stop4Safe := func() {
		if !node4Stopped {
			stop4()
			node4Stopped = true
		}
	}

	defer func() {
		os.Remove("test_tail_node4.db")
		stop4Safe()
	}()

	time.Sleep(5 * time.Second) // Wait for cluster to stabilize

	// 3. Client Setup
	c := client.New()
	defer c.Close()

	if err := c.Connect(cpAddr); err != nil {
		t.Fatalf("Failed to connect to control plane: %v", err)
	}

	ctx := context.Background()

	// 4. Register and Login
	log.Println("Registering user...")
	if err := c.RegisterUser(ctx, "tim", "tim"); err != nil {
		t.Fatalf("Failed to register user: %v", err)
	}

	if err := c.LoginUser(ctx, "tim", "tim"); err != nil {
		t.Fatalf("Failed to login: %v", err)
	}

	// 5. Create Topic
	log.Println("Creating topic...")
	if err := c.CreateTopic(ctx, "general"); err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// 6. Post Message 1
	log.Println("Posting message 1...")
	if err := c.PostMessage(ctx, "general", "message 1"); err != nil {
		t.Fatalf("Failed to post message 1: %v", err)
	}

	// 7. Post "node4freeze"
	log.Println("Posting node4freeze...")
	go func() {
		// This might block or fail, we ignore the error as we are testing fault tolerance
		c.PostMessage(ctx, "general", "node4freeze")
	}()

	log.Println("Waiting 2 seconds...")
	time.Sleep(2 * time.Second)

	// 8. Kill Node 4
	log.Println("Killing node4...")
	stop4Safe()

	// 9. Wait 2 seconds
	log.Println("Waiting 10 seconds...")
	time.Sleep(10 * time.Second)

	// 10. Check messages
	// Refresh topology because the tail changed
	if err := c.RefreshTopology(); err != nil {
		t.Fatalf("Failed to refresh topology: %v", err)
	}

	log.Println("Checking messages...")
	msgs, err := c.GetMessages(ctx, "general", 0, 100)
	if err != nil {
		t.Fatalf("Failed to get messages: %v", err)
	}

	if len(msgs) != 2 {
		t.Errorf("Expected 2 messages, got %d", len(msgs))
		for _, m := range msgs {
			t.Logf(" - %s", m.Text)
		}
	} else {
		log.Println("All 2 messages present!")
		expected := []string{"message 1", "node4freeze"}
		for i, msg := range msgs {
			if msg.Text != expected[i] {
				t.Errorf("Message %d: expected %s, got %s", i, expected[i], msg.Text)
			}
		}
	}
}
