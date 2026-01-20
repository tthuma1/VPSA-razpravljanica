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

// Helper to start a control plane
func startControlPlaneMiddleTest(t *testing.T, port int) (*grpc.Server, string) {
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
func startNodeWithStopperMiddleTest(t *testing.T, id string, port int, controlAddr string, dbPath string, testing bool) (*grpc.Server, *dataplane.Node, func()) {
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

func TestMiddleFailScenario(t *testing.T) {
	// 1. Start Control Plane
	cpServer, cpAddr := startControlPlaneMiddleTest(t, 50057)
	defer cpServer.Stop()

	// 2. Start 4 Nodes
	// Node 1
	_, _, stop1 := startNodeWithStopperMiddleTest(t, "node1", 50081, cpAddr, "test_middle_node1.db", false)
	defer func() {
		os.Remove("test_middle_node1.db")
		stop1()
	}()

	time.Sleep(1 * time.Second)

	// Node 2
	_, _, stop2 := startNodeWithStopperMiddleTest(t, "node2", 50082, cpAddr, "test_middle_node2.db", false)
	defer func() {
		os.Remove("test_middle_node2.db")
		stop2()
	}()

	time.Sleep(1 * time.Second)

	// Node 3 (Middle, testing=true)
	_, _, stop3 := startNodeWithStopperMiddleTest(t, "node3", 50083, cpAddr, "test_middle_node3.db", true)

	node3Stopped := false
	stop3Safe := func() {
		if !node3Stopped {
			stop3()
			node3Stopped = true
		}
	}

	defer func() {
		os.Remove("test_middle_node3.db")
		stop3Safe()
	}()

	time.Sleep(1 * time.Second)

	// Node 4
	_, _, stop4 := startNodeWithStopperMiddleTest(t, "node4", 50084, cpAddr, "test_middle_node4.db", false)
	defer func() {
		os.Remove("test_middle_node4.db")
		stop4()
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

	// 7. Post "node3freeze" 5 times
	log.Println("Posting node3freeze 10 times...")
	go func() {
		for i := 0; i < 5; i++ {
			// This might block or fail, we ignore the error as we are testing fault tolerance
			c.PostMessage(ctx, "general", "node3freeze")
		}
	}()

	log.Println("Waiting 2 seconds...")
	time.Sleep(2 * time.Second)

	// 8. Kill Node 3
	log.Println("Killing node3...")
	stop3Safe()

	// 9. Wait 10 seconds
	log.Println("Waiting 10 seconds...")
	time.Sleep(10 * time.Second)

	// 10. Check messages
	// Refresh topology because the middle node failed
	if err := c.RefreshTopology(); err != nil {
		t.Fatalf("Failed to refresh topology: %v", err)
	}

	log.Println("Checking messages...")
	for i := 0; i < 3; i++ {
		msgs, err := c.GetMessages(ctx, "general", 0, 100)
		if err != nil {
			t.Fatalf("Failed to get messages: %v", err)
		}

		if len(msgs) == 6 {
			log.Println("All 6 messages present!")
			expected := "message 1"
			if msgs[0].Text != expected {
				t.Errorf("Message 0: expected %s, got %s", expected, msgs[0].Text)
			}
			for j := 1; j < 6; j++ {
				if msgs[j].Text != "node3freeze" {
					t.Errorf("Message %d: expected node3freeze, got %s", j, msgs[j].Text)
				}
			}
			return
		}

		log.Printf("Attempt %d: Expected 6 messages, got %d", i+1, len(msgs))
		time.Sleep(1 * time.Second)
	}

	// If we get here, we failed
	msgs, _ := c.GetMessages(ctx, "general", 0, 100)
	t.Errorf("Failed to get 6 messages after 3 attempts. Got %d", len(msgs))
}
