# Project File Analysis

## Overview
This project implements a distributed message board system using Go and gRPC. It consists of a control plane for cluster management and data plane nodes arranged in a chain for replication and fault tolerance.

## Go Files

### cmd/client/main.go
CLI client application that connects to the control plane to discover head and tail nodes, then provides an interactive interface for message board operations. Supports commands for creating users/topics, posting messages, liking messages, listing topics, retrieving messages, and subscribing to real-time topic updates via streaming gRPC.

### cmd/controlplane/main.go
Entry point for the control plane service. Starts a gRPC server that manages cluster topology, node registration, and health monitoring. Runs on port 50050 by default.

### cmd/server/main.go
Entry point for data plane nodes. Creates a Node instance with SQLite storage, registers with control plane, sends heartbeats, and starts gRPC servers for MessageBoard and Replication services. Dynamically updates node role (head/tail/middle) based on cluster state.

### internal/controlplane/controlplane.go
Implements the ControlPlane service managing cluster state. Maintains a chain of nodes, handles registration, monitors health via heartbeats (15s timeout), and reconfigures the chain when nodes fail. Provides cluster state and chain topology information.

### internal/dataplane/node.go
Core data plane implementation. Handles MessageBoard service (writes on head, reads on tail, subscriptions on head) and Replication service for chain replication. Manages subscriptions for real-time event streaming, broadcasts events to subscribers, and forwards writes/events to next node in chain.

### internal/dataplane/storage.go
SQLite-based persistent storage layer. Manages database schema for users, topics, messages, likes, and sequence numbers. Provides thread-safe CRUD operations with foreign key constraints and sequence numbering for event ordering.

### go.mod
Go module definition for the messageboard project. Uses Go 1.25.3 with dependencies: gRPC/protobuf for communication, modernc.org/sqlite for embedded database.

## Protocol Buffer Files

### proto/razpravljalnica.proto
Protocol buffer definitions for the distributed message board system. Defines three services:
- MessageBoard: Client API for CRUD operations and subscriptions
- ControlPlane: Cluster management (registration, heartbeats, state queries)
- Replication: Inter-node replication (write ops, state sync, event notification)

Defines data types: User, Topic, Message, Like, NodeInfo, and operation types.

### proto/razpravljalnica.pb.go
Generated Go code from razpravljalnica.proto. Contains service interfaces, message types, and gRPC client/server implementations.

### proto/razpravljalnica_grpc.pb.go
Generated gRPC stubs from razpravljalnica.proto. Provides client and server code for all three services defined in the proto file.

## Architecture
- **Control Plane**: Centralized cluster coordinator managing node lifecycle and topology
- **Data Plane**: Chain of nodes with head handling writes/replication, tail handling reads, middle nodes forwarding
- **Replication**: Chain replication ensures fault tolerance and consistency
- **Subscriptions**: Real-time event streaming for topic updates
- **Storage**: Per-node SQLite databases with sequence-based ordering