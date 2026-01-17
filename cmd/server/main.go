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
	"syscall"
	"time"

	"messageboard/internal/dataplane"
	pb "messageboard/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	port        = flag.Int("port", 50051, "The server port")
	nodeID      = flag.String("id", "", "Node ID")
	dbPath      = flag.String("db", "", "Database path")
	controlAddr = flag.String("control", "localhost:50050", "Control plane address")
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
	node, err := dataplane.NewNode(*nodeID, address, *dbPath, *controlAddr)
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
	}
	defer node.Close()

	// Start gRPC server
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterMessageBoardServer(grpcServer, node)
	pb.RegisterReplicationServer(grpcServer, node)

	log.Printf("Node %s starting on %s", *nodeID, address)

	// Register with control plane
	go registerWithControlPlane(*nodeID, address, *controlAddr, node)

	// Start server
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	// Wait for interrupt
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	grpcServer.GracefulStop()
}

func registerWithControlPlane(nodeID, address, controlAddr string, node *dataplane.Node) {
	time.Sleep(1 * time.Second) // Wait for server to start

	conn, err := grpc.NewClient(controlAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Failed to connect to control plane: %v", err)
		return
	}
	defer conn.Close()

	client := pb.NewControlPlaneClient(conn)

	// Register
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.RegisterNode(ctx, &pb.RegisterNodeRequest{
		NodeId:  nodeID,
		Address: address,
	})
	if err != nil {
		log.Printf("Failed to register with control plane: %v", err)
		return
	}

	log.Printf("Registered with control plane")

	// Send heartbeats
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	hasSynced := false

	for range ticker.C {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := client.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: nodeID})
		cancel()

		if err != nil {
			log.Printf("Heartbeat failed: %v", err)
		}

		// Get and update role
		prevNode, err := updateRole(client, node)
		if err != nil {
			log.Printf("Failed to update role: %v", err)
			continue
		}

		if !hasSynced {
			if prevNode != "" {
				log.Printf("Initiating sync with predecessor: %s", prevNode)
				// Use a longer timeout for sync
				syncCtx, syncCancel := context.WithTimeout(context.Background(), 30*time.Second)
				if err := node.SyncWithPredecessor(syncCtx, prevNode); err != nil {
					log.Printf("Sync failed: %v", err)
					syncCancel()
					continue // Retry next tick
				}
				syncCancel()
				hasSynced = true
				log.Printf("Sync completed successfully")
			} else {
				// No predecessor, assume synced (e.g. first node)
				hasSynced = true
			}
		}
	}
}

func updateRole(client pb.ControlPlaneClient, node *dataplane.Node) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	state, err := client.GetClusterState(ctx, &emptypb.Empty{})
	if err != nil {
		return "", err
	}

	var prevNode string
	var nextNode string
	var role dataplane.NodeRole

	// Determine role
	if state.Head.NodeId == node.NodeID() {
		chainState, err := client.GetChainState(ctx, &emptypb.Empty{})
		if err != nil {
			return "", err
		}

		if len(chainState.Chain) > 1 {
			nextNode = chainState.Chain[1].Address
		}
		role = dataplane.RoleHead
		prevNode = ""
	} else if state.Tail.NodeId == node.NodeID() {
		chainState, err := client.GetChainState(ctx, &emptypb.Empty{})
		if err != nil {
			return "", err
		}

		if len(chainState.Chain) > 1 {
			prevNode = chainState.Chain[len(chainState.Chain)-2].Address
		}
		role = dataplane.RoleTail
		nextNode = ""
	} else {
		// Find next node in chain
		chainState, err := client.GetChainState(ctx, &emptypb.Empty{})
		if err != nil {
			return "", err
		}

		for i, n := range chainState.Chain {
			if n.NodeId == node.NodeID() {
				if i < len(chainState.Chain)-1 {
					nextNode = chainState.Chain[i+1].Address
				}
				if i > 0 {
					prevNode = chainState.Chain[i-1].Address
				}
				break
			}
		}
		role = dataplane.RoleMiddle
	}

	node.SetRole(role, nextNode, prevNode)
	return prevNode, nil
}
