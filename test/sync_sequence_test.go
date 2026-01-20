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
func startControlPlaneSync(t *testing.T, port int) (*grpc.Server, string) {
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
func startNodeWithStopperSync(t *testing.T, id string, port int, controlAddr string, dbPath string, testing bool) (*grpc.Server, *dataplane.Node, func()) {
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

func TestSyncSequence(t *testing.T) {
	// 1. Start Control Plane
	cpServer, cpAddr := startControlPlaneSync(t, 50070)
	defer cpServer.Stop()

	// 2. Start 3 Nodes
	_, _, stop1 := startNodeWithStopperSync(t, "node1", 50071, cpAddr, "test_sync_node1.db", false)
	defer os.Remove("test_sync_node1.db")
	defer stop1()

	time.Sleep(1 * time.Second)

	_, _, stop2 := startNodeWithStopperSync(t, "node2", 50072, cpAddr, "test_sync_node2.db", false)
	defer os.Remove("test_sync_node2.db")
	defer stop2()

	time.Sleep(1 * time.Second)

	_, _, stop3 := startNodeWithStopperSync(t, "node3", 50073, cpAddr, "test_sync_node3.db", false)
	defer os.Remove("test_sync_node3.db")
	defer stop3()

	time.Sleep(2 * time.Second) // Wait for cluster to stabilize

	// 3. Client Setup
	c := client.New()
	defer c.Close()

	if err := c.Connect(cpAddr); err != nil {
		t.Fatalf("Failed to connect to control plane: %v", err)
	}

	ctx := context.Background()

	// 4. Register, Login, Create Topic, Post Message
	if err := c.RegisterUser(ctx, "testuser", "password"); err != nil {
		t.Fatalf("Failed to register user: %v", err)
	}

	if err := c.LoginUser(ctx, "testuser", "password"); err != nil {
		t.Fatalf("Failed to login: %v", err)
	}

	if err := c.CreateTopic(ctx, "synctest"); err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	if err := c.PostMessage(ctx, "synctest", "initial message"); err != nil {
		t.Fatalf("Failed to post initial message: %v", err)
	}

	// 5. Start node4 with -testing flag
	log.Println("Starting node4")
	time.Sleep(1 * time.Second)

	// 6. Send 10 more messages
	go func(cpAddr2 string) {
		c2 := client.New()
		defer c2.Close()
		ctx2 := context.Background()

		if err := c2.Connect(cpAddr2); err != nil {
			fmt.Errorf("Failed to connect to control plane: %v", err)
			return
		}

		if err := c2.LoginUser(ctx2, "testuser", "password"); err != nil {
			fmt.Errorf("Failed to login: %v", err)
			return
		}

		for i := 0; i < 10; i++ {
			log.Printf("Sending message %d", i)
			msg := fmt.Sprintf("message %d", i)
			go c2.PostMessage(ctx2, "synctest", msg)
			time.Sleep(200 * time.Millisecond)
		}
		time.Sleep(5 * time.Second)
	}(cpAddr)

	_, _, stop4 := startNodeWithStopperSync(t, "node4", 50074, cpAddr, "test_sync_node4.db", true)
	defer os.Remove("test_sync_node4.db")
	defer stop4()

	time.Sleep(10 * time.Second) // Wait for node4 to join and sync

	// 7. Verify messages
	for i := 0; i < 4; i++ {
		log.Printf("Verification run %d", i+1)
		msgs, err := c.GetMessages(ctx, "synctest", 0, 100)
		if err != nil {
			t.Fatalf("Run %d: Failed to get messages: %v", i+1, err)
		}

		if len(msgs) != 11 {
			t.Fatalf("Run %d: Expected 11 messages, got %d", i+1, len(msgs))
			//log.Printf("Run %d: Expected 11 messages, got %d", i+1, len(msgs))
		}
		log.Printf("Run %d: Correctly received 11 messages", i+1)
		time.Sleep(500 * time.Millisecond)
	}
}
