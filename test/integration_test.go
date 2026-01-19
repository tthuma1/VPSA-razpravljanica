package test

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"messageboard/internal/client"
	"messageboard/internal/controlplane"
	"messageboard/internal/dataplane"
	pb "messageboard/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Helper to start a control plane
func startControlPlane(t *testing.T, port int) (*grpc.Server, string) {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	cp := controlplane.NewControlPlane()
	pb.RegisterControlPlaneServer(s, cp)
	go func() {
		if err := s.Serve(lis); err != nil {
			// t.Errorf("failed to serve: %v", err)
		}
	}()
	return s, lis.Addr().String()
}

// Improved node starter that returns a stop function
func startNodeWithStopper(t *testing.T, id string, port int, controlAddr string, dbPath string) (*grpc.Server, *dataplane.Node, func()) {
	// Clean up DB if exists
	os.Remove(dbPath)

	node, err := dataplane.NewNode(id, fmt.Sprintf("localhost:%d", port), dbPath, controlAddr)
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
		conn, err := grpc.NewClient(controlAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
						if len(chainState.Chain) > 1 {
							nextNode = chainState.Chain[1].Address
						}
					} else if state.Tail.NodeId == id {
						role = dataplane.RoleTail
						if len(chainState.Chain) > 1 {
							prevNode = chainState.Chain[len(chainState.Chain)-2].Address
						}
					} else {
						role = dataplane.RoleMiddle
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

func TestIntegrationScenario(t *testing.T) {
	// 1. Start Control Plane
	cpServer, cpAddr := startControlPlane(t, 50055)
	defer cpServer.Stop()

	// 2. Start 3 Nodes
	// Node 1 (Head initially)
	_, _, stop1 := startNodeWithStopper(t, "node1", 50061, cpAddr, "test_node1.db")
	defer func() {
		os.Remove("test_node1.db")
		// stop1() // Don't call stop1 here, we call it manually in the test
	}()

	// Wait a bit for registration
	time.Sleep(1 * time.Second)

	// Node 2
	_, _, stop2 := startNodeWithStopper(t, "node2", 50062, cpAddr, "test_node2.db")
	defer func() {
		os.Remove("test_node2.db")
		//stop2()
	}()

	time.Sleep(1 * time.Second)

	// Node 3 (Tail initially)
	_, _, stop3 := startNodeWithStopper(t, "node3", 50063, cpAddr, "test_node3.db")
	defer func() {
		os.Remove("test_node3.db")
		//stop3()
	}()

	time.Sleep(2 * time.Second) // Wait for cluster to stabilize

	// 3. Client Setup
	c := client.New()
	defer c.Close()

	// Connect to Control Plane
	if err := c.Connect(cpAddr); err != nil {
		t.Fatalf("Failed to connect to control plane: %v", err)
	}

	ctx := context.Background()

	// 4. Register Account "tim:tim"
	log.Println("Registering user...")
	if err := c.RegisterUser(ctx, "tim", "tim"); err != nil {
		t.Fatalf("Failed to register user: %v", err)
	}

	// Login
	if err := c.LoginUser(ctx, "tim", "tim"); err != nil {
		t.Fatalf("Failed to login: %v", err)
	}

	// 5. Create Topic
	log.Println("Creating topic...")
	if err := c.CreateTopic(ctx, "general"); err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// 6. Post Message
	log.Println("Posting message 1...")
	if err := c.PostMessage(ctx, "general", "message 1"); err != nil {
		t.Fatalf("Failed to post message 1: %v", err)
	}

	// Verify message exists
	msgs, err := c.GetMessages(ctx, "general", 0, 10)
	if err != nil {
		t.Fatalf("Failed to get messages: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(msgs))
	}

	log.Println("Waiting 2 seconds...")
	time.Sleep(2 * time.Second)

	// 7. Kill Head Node (Node 1)
	log.Println("Killing head node (node1)...")
	stop1() // This stops the server and the heartbeat loop
	// s1 is already stopped by stop1

	// 8. Wait 6 seconds (Heartbeat timeout is 5s in control plane)
	log.Println("Waiting 10 seconds...")
	time.Sleep(10 * time.Second)

	// Now Node 2 should be Head.
	// Client needs to reconnect or refresh topology.
	// The client library might not auto-reconnect in this test setup unless we trigger it.
	// We'll manually refresh topology by calling Connect again or relying on retry logic.
	// Since we killed the head, the next request might fail and trigger retry.
	// But let's be explicit.
	if err := c.RefreshTopology(); err != nil {
		t.Fatalf("Failed to refresh topology: %v", err)
	}

	// 9. Post two more messages
	log.Println("Posting message 2...")
	if err := c.PostMessage(ctx, "general", "message 2"); err != nil {
		t.Fatalf("Failed to post message 2: %v", err)
	}
	log.Println("Posting message 3...")
	if err := c.PostMessage(ctx, "general", "message 3"); err != nil {
		t.Fatalf("Failed to post message 3: %v", err)
	}

	// 10. Bring back the killed node (Node 1)
	log.Println("Bringing back node1...")
	// We need to restart it. We can't reuse the old server object easily.
	// We'll start a new one with same ID and DB path.
	// Note: DB path "test_node1.db" should persist the data from before?
	// Wait, in startNodeWithStopper we do os.Remove(dbPath).
	// If we want to simulate "bringing back" a node that crashed but kept disk, we shouldn't remove DB.
	// But the prompt says "brings back the killed node". Usually implies recovery.
	// If we remove DB, it joins as a fresh node and syncs.
	// If we keep DB, it might have old state.
	// Let's modify startNodeWithStopper to optionally NOT remove DB?
	// Or just manually start it here.

	// For this test, let's assume it comes back as a fresh node (or with cleared state) and syncs from others.
	// This is safer for consistency.
	_, _, stop1New := startNodeWithStopper(t, "node1", 50061, cpAddr, "test_node1_recovered.db")
	defer func() {
		os.Remove("test_node1_recovered.db")
		stop1New()
	}()

	// 11. Wait 6 seconds
	log.Println("Waiting 10 seconds...")
	time.Sleep(10 * time.Second)

	// Node 1 should have joined and synced.
	// It will likely be the Tail now (since it joined last).
	// Chain: Node 2 (Head) -> Node 3 -> Node 1 (Tail)

	// 12. Kill the other two nodes (Node 2 and Node 3)
	log.Println("Killing node2 and node3...")
	stop2()
	stop3()

	// 13. Wait 6 seconds
	log.Println("Waiting 10 seconds...")
	time.Sleep(10 * time.Second)

	// Now Node 1 should be the only one left. It should become Head (and Tail).
	// Refresh topology.
	if err := c.RefreshTopology(); err != nil {
		t.Fatalf("Failed to refresh topology after killing 2 nodes: %v", err)
	}

	// 14. Check if all three messages are still present
	log.Println("Checking messages...")
	msgs, err = c.GetMessages(ctx, "general", 0, 100)
	if err != nil {
		t.Fatalf("Failed to get messages: %v", err)
	}

	if len(msgs) != 3 {
		t.Errorf("Expected 3 messages, got %d", len(msgs))
		for _, m := range msgs {
			t.Logf(" - %s", m.Text)
		}
	} else {
		log.Println("All 3 messages present!")
		// Verify content
		expected := []string{"message 1", "message 2", "message 3"}
		for i, msg := range msgs {
			if msg.Text != expected[i] {
				t.Errorf("Message %d: expected %s, got %s", i, expected[i], msg.Text)
			}
		}
	}
}
