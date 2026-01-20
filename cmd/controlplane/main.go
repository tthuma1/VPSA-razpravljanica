// cmd/controlplane/main.go
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"messageboard/internal/controlplane"
	pb "messageboard/proto"

	"google.golang.org/grpc"
)

var (
	port      = flag.Int("port", 50050, "The control plane port")
	raftID    = flag.String("raft-id", "", "Unique ID for this Raft node")
	raftAddr  = flag.String("raft-addr", "localhost:50060", "Address for Raft transport")
	raftDir   = flag.String("raft-dir", "raft-data", "Directory for Raft storage")
	bootstrap = flag.Bool("bootstrap", false, "Bootstrap the Raft cluster (only for the first node)")
	httpPort  = flag.Int("http-port", 0, "Port for management HTTP server (default: port + 1000)")
)

func main() {
	flag.Parse()

	if *raftID == "" {
		log.Fatal("raft-id is required")
	}

	// Create control plane
	cp := controlplane.NewControlPlane()

	// Setup Raft
	if err := os.MkdirAll(*raftDir, 0700); err != nil {
		log.Fatalf("Failed to create raft directory: %v", err)
	}

	if err := cp.SetupRaft(*raftID, *raftAddr, *raftDir, *bootstrap); err != nil {
		log.Fatalf("Failed to setup Raft: %v", err)
	}

	// Start Management HTTP Server
	mgmtPort := *httpPort
	if mgmtPort == 0 {
		mgmtPort = *port + 1000
	}
	go startManagementServer(cp, mgmtPort)

	address := fmt.Sprintf("localhost:%d", *port)
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

func startManagementServer(cp *controlplane.ControlPlane, port int) {
	http.HandleFunc("/join", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			NodeID   string `json:"node_id"`
			RaftAddr string `json:"raft_addr"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if err := cp.Join(req.NodeID, req.RaftAddr); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		log.Printf("Successfully joined node %s at %s", req.NodeID, req.RaftAddr)
	})

	addr := fmt.Sprintf(":%d", port)
	log.Printf("Management HTTP server listening on %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("Failed to start management server: %v", err)
	}
}
