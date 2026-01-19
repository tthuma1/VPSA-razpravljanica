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

	// Start server
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	go registerWithControlPlane(*nodeID, address, *controlAddr, node)

	// Wait for interrupt
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	grpcServer.GracefulStop()
}

func registerWithControlPlane(nodeID, address, controlAddr string, node *dataplane.Node) {
	waitForServer(address)

	conn, err := grpc.NewClient(controlAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Failed to connect to control plane: %v", err)
		return
	}
	defer conn.Close()

	controlPlaneClient := pb.NewControlPlaneClient(conn)

	// Register
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = controlPlaneClient.RegisterNode(ctx, &pb.RegisterNodeRequest{
		NodeId:  nodeID,
		Address: address,
	})
	if err != nil {
		log.Printf("Failed to register with control plane: %v", err)
		return
	}

	log.Printf("Registered with control plane")

	// Sync with tail
	hasSynced := false
	ticker := time.NewTicker(3 * time.Second)

	syncFunc := func() {
		log.Printf("Initiating sync with tail")
		// Use a longer timeout for sync
		syncCtx, syncCancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := node.SyncWithTail(syncCtx); err != nil {
			log.Printf("Sync failed: %v", err)
			syncCancel()
		}
		syncCancel()
		hasSynced = true
	}

	syncFunc()
	for range ticker.C {
		if hasSynced {
			break
		}
		syncFunc()
	}

	// Send heartbeats
	ticker = time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	heartbeatFunc := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := controlPlaneClient.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: nodeID})
		cancel()

		if err != nil {
			log.Printf("Heartbeat failed: %v", err)
		}
	}

	heartbeatFunc()

	for range ticker.C {
		heartbeatFunc()
	}
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
