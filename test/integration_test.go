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
func startControlPlane(t *testing.T, port int) ([]*grpc.Server, string) {
	var servers []*grpc.Server
	var addrs []string

	// Start 3 control plane nodes
	for i := 0; i < 3; i++ {
		nodeID := fmt.Sprintf("node%d", i+1)
		nodePort := port + i
		raftPort := port + 1000 + i

		lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", nodePort))
		if err != nil {
			t.Fatalf("failed to listen: %v", err)
		}

		raftDir, err := os.MkdirTemp("", fmt.Sprintf("raft-integration-test-%s-*", nodeID))
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

	// Join nodes to the cluster
	// Wait for leader election on node1
	time.Sleep(3 * time.Second)

	// Join node2 and node3 to node1
	leaderAddr := addrs[0]
	for i := 1; i < 3; i++ {
		nodeID := fmt.Sprintf("node%d", i+1)
		raftAddr := fmt.Sprintf("localhost:%d", port+1000+i)

		// We need to call Join on the leader via HTTP or some other mechanism.
		// But here we don't have the HTTP server running.
		// However, the ControlPlane struct has a Join method, but it's not exposed via gRPC in the proto.
		// Wait, the ControlPlane struct in `internal/controlplane/controlplane.go` has a `Join` method?
		// Let's check `internal/controlplane/raft.go`. Yes, it has `Join`.
		// But we can't call it remotely easily without the HTTP server or gRPC extension.
		// In `cmd/controlplane/main.go`, there is a management HTTP server.
		// In this test, we are embedding the ControlPlane.
		// We can't easily call `cp.Join` on the leader instance because we don't have reference to it here easily
		// unless we return the CP instances.

		// Actually, `startControlPlane` returns `*grpc.Server`. We should probably return the CP instances too if we want to manipulate them.
		// OR, we can just rely on the static peer list if it exists?
		// In `internal/controlplane/controlplane.go`:
		// var raftPeers = map[string]string{
		// 	"node1": "localhost:60051",
		// 	"node2": "localhost:60052",
		// 	"node3": "localhost:60053",
		// }
		// The code has a reconciliation loop that tries to add peers from this list!
		// So if we start them on the correct ports, they might auto-join?
		// The ports in `raftPeers` are 60051, 60052, 60053.
		// But here we are using `port + 1000 + i`.
		// If we want them to auto-join, we should match the ports or update the map.
		// Since we can't easily update the map (it's a global var in the package), we should try to match the ports if possible.
		// But `raftPeers` seems to map nodeID to *Raft Address*? Or gRPC address?
		// In `reconcileRaftPeers`: `conn, err := net.DialTimeout("tcp", addr, 1*time.Second)` and then `cp.raft.AddVoter(..., addr, ...)`.
		// So it expects Raft addresses.
		// The default `raftPeers` has "localhost:60051".

		// Let's try to use the ports that match `raftPeers` if possible, or just rely on manual joining if we can access the CP object.
		// Let's modify `startControlPlane` to return the CP objects so we can call Join manually.
	}

	// Wait for cluster to form
	time.Sleep(5 * time.Second)

	// Return comma-separated list of addresses for the client
	return servers, fmt.Sprintf("%s,%s,%s", addrs[0], addrs[1], addrs[2])
}

// Helper to start a control plane with manual joining
func startControlPlaneCluster(t *testing.T, basePort int) ([]*grpc.Server, string) {
	var servers []*grpc.Server
	var cps []*controlplane.ControlPlane
	var addrs []string

	// Start 3 control plane nodes
	for i := 0; i < 3; i++ {
		nodeID := fmt.Sprintf("node%d", i+1)
		nodePort := basePort + i
		raftPort := basePort + 1000 + i

		lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", nodePort))
		if err != nil {
			t.Fatalf("failed to listen: %v", err)
		}

		raftDir, err := os.MkdirTemp("", fmt.Sprintf("raft-integration-test-%s-*", nodeID))
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
		cps = append(cps, cp)
		addrs = append(addrs, lis.Addr().String())
	}

	// Wait for leader election on node1
	time.Sleep(2 * time.Second)

	// Join node2 and node3 to node1 (assuming node1 is leader)
	// We call Join on the leader's CP instance
	leaderCP := cps[0]
	for i := 1; i < 3; i++ {
		nodeID := fmt.Sprintf("node%d", i+1)
		raftAddr := fmt.Sprintf("localhost:%d", basePort+1000+i)

		// We need to retry joining because leader election might take time
		// or the leader might not be ready.
		// Also, `Join` in `raft.go` checks if it is leader.

		// Simple retry loop
		joined := false
		for attempt := 0; attempt < 10; attempt++ {
			// We try to join via the leader.
			// Note: In a real scenario, we would send a request to the leader.
			// Here we have direct access to the leader's struct.
			// However, `Join` method in `raft.go` calls `cp.raft.AddVoter` eventually (via reconciliation or directly if we modify it).
			// The current `Join` implementation in `raft.go` just logs and maybe triggers reconciliation?
			// "Leader received join request for %s. The reconciliation loop will handle it."
			// But the reconciliation loop uses the static `raftPeers` map!
			// It does NOT use the arguments passed to `Join` to add the server directly if it's not in the map.
			// Wait, looking at `reconcileRaftPeers` in `controlplane.go`:
			// It iterates over `raftPeers` and adds them.
			// It does NOT seem to support dynamic joining of nodes not in `raftPeers` map easily unless we modify `raftPeers`?
			// But `raftPeers` is a global variable. We can modify it in the test!

			// Let's modify the global `raftPeers` map in `controlplane` package?
			// We can't easily access it if it's not exported. It is `var raftPeers`. It is NOT exported (lowercase).
			// So we cannot modify it from the test package.

			// However, `Join` implementation:
			// func (cp *ControlPlane) Join(nodeID, addr string) error {
			//     ...
			//     go cp.reconcileRaftPeers()
			//     return nil
			// }
			// This implementation seems to rely on `raftPeers` being correct.
			// If `raftPeers` is hardcoded to 60051, 60052, 60053, we MUST use those ports for Raft.

			// Let's check `controlplane.go` again.
			// var raftPeers = map[string]string{
			// 	"node1": "localhost:60051",
			// 	"node2": "localhost:60052",
			// 	"node3": "localhost:60053",
			// }

			// So, if we want the cluster to form using the existing code, we MUST start our Raft nodes on these exact ports.
			// node1: localhost:60051
			// node2: localhost:60052
			// node3: localhost:60053

			// And we should probably start the gRPC servers on some other ports.
			// The `raftPeers` map values are used for `net.DialTimeout` and `AddVoter`.
			// So these ARE the Raft ports.

			// So, in `startControlPlaneCluster`, we should use these specific ports for Raft.
			break
		}
		if !joined {
			// If we rely on static configuration, we don't need to call Join explicitly if the leader's reconciliation loop picks it up.
			// But the leader needs to know about them. The leader knows about them via `raftPeers`.
		}
	}

	return servers, fmt.Sprintf("%s,%s,%s", addrs[0], addrs[1], addrs[2])
}

// Helper to start a control plane with fixed ports matching the static config
func startControlPlaneFixed(t *testing.T, baseGrpcPort int) ([]*grpc.Server, string) {
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

		raftDir, err := os.MkdirTemp("", fmt.Sprintf("raft-integration-test-%s-*", nodeID))
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
func startNodeWithStopper(t *testing.T, id string, port int, controlAddr string, dbPath string, testing bool) (*grpc.Server, *dataplane.Node, func()) {
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
		// The client handles multiple addresses in Connect, but here we are using a raw grpc client.
		// We need to handle the comma-separated list or just pick one.
		// For simplicity, we'll try to connect to the first one, and if it fails (not leader), we might need logic.
		// BUT, `dataplane.NewNode` takes `controlAddr`.
		// Inside `NewNode`, it doesn't seem to use `controlAddr` for anything other than storing it?
		// Actually, the node logic we see here in the test manually connects!
		// `conn, err := grpc.NewClient(controlAddr, ...)`
		// `controlAddr` here is passed from `startControlPlane`.
		// If `controlAddr` is a list "addr1,addr2,addr3", `grpc.NewClient` might not like it directly unless we use a resolver or split it.
		// The `client.Connect` handles splitting.
		// Here in the test, we should probably pick one or implement retry logic.

		// Let's parse the controlAddr
		// For the test, we can just try the first one (node1) which is likely the leader initially.
		// Or we can implement a simple loop.

		// However, `grpc.NewClient` expects a target.
		// If we pass "host1:port,host2:port", it's invalid.
		// We need to pick one.

		// Let's assume controlAddr contains multiple addresses separated by comma.
		// We will try to connect to them until we find the leader.

		// But wait, `startNodeWithStopper` is used by the test.
		// The `controlAddr` passed to it comes from `startControlPlane`.
		// We should probably pass just one address to `startNodeWithStopper` if we want to keep it simple,
		// OR update `startNodeWithStopper` to handle the list.

		// Let's update `startNodeWithStopper` to handle the list.

		// ...

		// Actually, for the purpose of this test, we can just point the data plane nodes to the first control plane node (node1).
		// If node1 is the leader (which it should be as it bootstraps), it works.
		// If node1 dies, the data plane nodes in this test might lose contact.
		// But the test scenario "Kill Head Node" refers to Data Plane Head Node, not Control Plane.
		// So the Control Plane is stable in this test.
		// So pointing to the first CP node is fine.

		// We'll parse the first address from controlAddr.
		targetCP := controlAddr
		// If comma exists, take first
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

		time.Sleep(2 * time.Second)

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

func TestIntegrationScenario(t *testing.T) {
	// 1. Start Control Plane (Cluster of 3)
	// We use ports 50055, 50056, 50057 for gRPC
	// And 60051, 60052, 60053 for Raft (fixed in startControlPlaneFixed)
	cpServers, cpAddr := startControlPlaneFixed(t, 50055)
	defer func() {
		for _, s := range cpServers {
			s.Stop()
		}
	}()

	// 2. Start 3 Nodes
	// Node 1 (Head initially)
	_, _, stop1 := startNodeWithStopper(t, "node1", 50061, cpAddr, "test_node1.db", false)
	defer func() {
		os.Remove("test_node1.db")
		// stop1() // Don't call stop1 here, we call it manually in the test
	}()

	// Wait a bit for registration
	time.Sleep(1 * time.Second)

	// Node 2
	_, _, stop2 := startNodeWithStopper(t, "node2", 50062, cpAddr, "test_node2.db", false)
	defer func() {
		os.Remove("test_node2.db")
		//stop2()
	}()

	time.Sleep(1 * time.Second)

	// Node 3 (Tail initially)
	_, _, stop3 := startNodeWithStopper(t, "node3", 50063, cpAddr, "test_node3.db", false)
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
	_, _, stop1New := startNodeWithStopper(t, "node1", 50061, cpAddr, "test_node1_recovered.db", false)
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
