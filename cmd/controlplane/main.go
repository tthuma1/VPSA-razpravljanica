// cmd/controlplane/main.go
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"messageboard/internal/controlplane"
	pb "messageboard/proto"

	"google.golang.org/grpc"
)

var (
	port = flag.Int("port", 50050, "The control plane port")
)

func main() {
	flag.Parse()

	address := fmt.Sprintf("localhost:%d", *port)

	// Create control plane
	cp := controlplane.NewControlPlane()

	// Start gRPC server
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterControlPlaneServer(grpcServer, cp)

	log.Printf("Control plane starting on %s", address)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	// When we send SIGTERM, control plane gracefully shuts down
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	grpcServer.GracefulStop()
}
