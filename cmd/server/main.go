// cmd/server/main.go
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"messageboard/internal/dataplane"
	pb "messageboard/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	port        = flag.Int("port", 50051, "The server port")
	nodeID      = flag.String("id", "", "Node ID")
	dbPath      = flag.String("db", "", "Database path")
	controlAddr = flag.String("control", "localhost:50051,localhost:50052,localhost:50053", "Comma-separated list of Control plane addresses")

	testing = flag.Bool("testing", false, "Set this flag to trigger various sleeps during execution")
)

func main() {
	flag.Parse()

	if *nodeID == "" {
		log.Fatal("Node ID is required")
	}

	if *dbPath == "" {
		*dbPath = fmt.Sprintf("node_%s.db", *nodeID)
	}

	address := fmt.Sprintf("localhost:%d", *port)

	// Create node
	node, err := dataplane.NewNode(*nodeID, address, *dbPath, *controlAddr, *testing)
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
	}
	defer node.Close()

	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterMessageBoardServer(grpcServer, node)
	pb.RegisterReplicationServer(grpcServer, node)

	log.Printf("Node %s starting on %s", *nodeID, address)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	go registerWithControlPlane(*nodeID, address, *controlAddr, node)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	grpcServer.GracefulStop()
}

func registerWithControlPlane(nodeID, address, controlAddrsStr string, node *dataplane.Node) {
	waitForServer(address)

	controlAddrs := strings.Split(controlAddrsStr, ",")
	var controlPlaneClient pb.ControlPlaneClient
	var conn *grpc.ClientConn
	var err error
	var mu sync.Mutex

	connectToLeader := func() error {
		mu.Lock()
		defer mu.Unlock()

		for _, addr := range controlAddrs {
			var tempConn *grpc.ClientConn
			tempConn, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				continue
			}
			client := pb.NewControlPlaneClient(tempConn)

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, err = client.RegisterNode(ctx, &pb.RegisterNodeRequest{
				NodeId:  nodeID,
				Address: address,
			})
			cancel()

			if err == nil {
				if conn != nil {
					conn.Close()
				}
				conn = tempConn
				controlPlaneClient = client
				log.Printf("Registered with control plane at %s", addr)
				return nil
			}

			if leaderAddr := parseLeaderRedirect(err); leaderAddr != "" {
				tempConn.Close() // Close connection to follower
				log.Printf("Redirecting to leader at %s", leaderAddr)

				if conn != nil {
					conn.Close()
				}
				conn, err = grpc.NewClient(leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err == nil {
					client = pb.NewControlPlaneClient(conn)
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					_, err = client.RegisterNode(ctx, &pb.RegisterNodeRequest{
						NodeId:  nodeID,
						Address: address,
					})
					cancel()
					if err == nil {
						controlPlaneClient = client
						log.Printf("Registered with control plane leader at %s", leaderAddr)
						return nil
					}
				}
			}
			tempConn.Close()
		}
		return fmt.Errorf("failed to register with any control plane node")
	}

	for {
		if err := connectToLeader(); err == nil {
			break
		}
		log.Printf("Registration failed, retrying in 3s...")
		time.Sleep(3 * time.Second)
	}

	node.SetControlPlaneClient(controlPlaneClient)

	// Sync with tail
	hasSynced := false
	syncTicker := time.NewTicker(3 * time.Second)
	defer syncTicker.Stop()

	syncFunc := func() {
		log.Printf("Initiating sync with tail")
		syncCtx, syncCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer syncCancel()
		if err := node.SyncWithTail(syncCtx); err != nil {
			log.Printf("Sync failed: %v", err)
		} else {
			hasSynced = true
		}
	}

	syncFunc()
	for ; !hasSynced; <-syncTicker.C {
		syncFunc()
	}

	// Send heartbeats
	heartbeatTicker := time.NewTicker(2 * time.Second)
	defer heartbeatTicker.Stop()

	heartbeatFunc := func() {
		mu.Lock()
		client := controlPlaneClient
		mu.Unlock()

		if client == nil {
			return // Not yet connected
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := client.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: nodeID})
		cancel()

		if err != nil {
			log.Printf("Heartbeat failed: %v. Reconnecting...", err)
			if err := connectToLeader(); err != nil {
				log.Printf("Reconnection failed: %v", err)
			} else {
				mu.Lock()
				node.SetControlPlaneClient(controlPlaneClient)
				mu.Unlock()
			}
		}
	}

	for range heartbeatTicker.C {
		heartbeatFunc()
	}
}

func parseLeaderRedirect(err error) string {
	re := regexp.MustCompile(`current leader is at ([\w.:]+)`)
	matches := re.FindStringSubmatch(err.Error())
	if len(matches) > 1 {
		return matches[1]
	}
	return ""
}

func waitForServer(address string) error {
	for {
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
}
