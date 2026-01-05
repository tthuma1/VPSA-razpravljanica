# Distributed MessageBoard System

A distributed message board system implemented in Go using gRPC and chain replication for fault tolerance.

## Architecture Overview

### Components

1. **Control Plane**: Manages cluster membership, health monitoring, and chain configuration
2. **Data Plane Nodes**: Chain-replicated nodes that store and serve data
3. **Client**: Interactive CLI client for all operations

### Chain Replication Model

```
[HEAD] → [MIDDLE] → [TAIL]
  ↓                    ↓
Writes              Reads
```

- **Head Node**: Processes all write operations (CreateUser, CreateTopic, PostMessage, LikeMessage)
- **Tail Node**: Processes all read operations (ListTopics, GetMessages)
- **Middle Nodes**: Replicate state from head to tail
- **All Nodes**: Can handle subscriptions (load balanced by head)

## Features

### Core Functionality
- ✅ User management (create users)
- ✅ Topic management (create topics, list topics)
- ✅ Messaging (post messages with validation)
- ✅ Likes (like messages, track counts)
- ✅ Real-time subscriptions (streaming message events)

### Distributed System Features
- ✅ Chain replication for fault tolerance
- ✅ Automatic node discovery via control plane
- ✅ Health monitoring with heartbeats
- ✅ Dynamic role assignment (head/middle/tail)
- ✅ Sequential state replication
- ✅ Monotonic event ordering

## Project Structure

```
messageboard/
├── proto/
│   └── razpravljalnica.proto      # Protocol buffer definitions
├── razpravljalnica/
│   └── *.pb.go                    # Generated protobuf code
├── internal/
│   ├── dataplane/
│   │   ├── storage.go             # SQLite persistence layer
│   │   └── node.go                # Data plane node logic
│   └── controlplane/
│       └── controlplane.go        # Control plane logic
├── cmd/
│   ├── controlplane/
│   │   └── main.go                # Control plane server
│   ├── server/
│   │   └── main.go                # Data plane node server
│   └── client/
│       └── main.go                # Interactive client
├── go.mod
├── go.sum
├── Makefile
└── README.md
```

## Setup Instructions

### Prerequisites

- Go 1.21 or later
- Protocol Buffers compiler (`protoc`)
- gRPC Go plugins

Install protoc plugins:
```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

### Installation

1. Clone or create the project structure
2. Initialize Go module:
```bash
go mod init messageboard
go mod tidy
```

3. Generate protobuf code:
```bash
make proto
```

4. Build binaries:
```bash
make build
```

## Running the System

### Start Control Plane (Terminal 1)
```bash
make run-control
```

The control plane starts on `localhost:50050`.

### Start Data Plane Nodes

**Terminal 2 - Node 1 (becomes HEAD):**
```bash
make run-node1
```

**Terminal 3 - Node 2 (becomes MIDDLE):**
```bash
make run-node2
```

**Terminal 4 - Node 3 (becomes TAIL):**
```bash
make run-node3
```

Each node:
- Registers with the control plane
- Sends periodic heartbeats
- Automatically receives role assignment
- Stores data in SQLite database

### Start Client (Terminal 5)
```bash
make run-client
```

## Client Usage

### Commands

```
create-user <name>
  Create a new user
  Example: create-user Alice

create-topic <name>
  Create a new topic
  Example: create-topic General

post <topic_id> <user_id> <text>
  Post a message to a topic
  Example: post 1 1 Hello everyone!

like <topic_id> <message_id> <user_id>
  Like a message
  Example: like 1 1 2

list-topics
  List all topics

get-messages <topic_id> [from_id] [limit]
  Get messages from a topic
  Example: get-messages 1
  Example: get-messages 1 5 20

subscribe <user_id> <topic_id1> [topic_id2...]
  Subscribe to topics for real-time updates
  Example: subscribe 1 1 2

quit
  Exit the client
```

### Example Session

```bash
> create-user Alice
Created user: ID=1, Name=Alice

> create-user Bob
Created user: ID=2, Name=Bob

> create-topic General
Created topic: ID=1, Name=General

> create-topic Random
Created topic: ID=2, Name=Random

> post 1 1 Hello everyone!
Posted message: ID=1, User=1, Likes=0

> post 1 2 Hi Alice!
Posted message: ID=2, User=2, Likes=0

> like 1 1 2
Liked message: ID=1, Likes=1

> list-topics
Topics:
  [1] General
  [2] Random

> get-messages 1
Messages in topic 1:
  [1] User 1: Hello everyone! (Likes: 1)
  [2] User 2: Hi Alice! (Likes: 0)

> subscribe 1 1
Subscribed to topics [1]. Waiting for events (Ctrl+C to stop)...
[POST] Seq=3, Msg ID=3, User=1, Text=New message, Likes=0
```

## Chain Replication Logic

### Write Path
1. Client sends write to HEAD node
2. HEAD applies operation to its database
3. HEAD forwards operation to next node in chain
4. Each node applies and forwards until TAIL
5. HEAD returns response to client

### Read Path
1. Client sends read to TAIL node
2. TAIL reads from its local database
3. TAIL returns response (reflects all committed writes)

### Subscription Path
1. Client requests subscription node from HEAD
2. HEAD assigns a node (load balancing)
3. Client connects to assigned node with token
4. Node streams historical + real-time events
5. Events propagate through chain with monotonic sequence numbers

## Failure Handling

### Node Failure Detection
- Control plane monitors heartbeats (15s timeout)
- Failed nodes marked unhealthy
- Chain automatically reconfigured

### Chain Reconfiguration
1. Control plane detects failure
2. Removes failed node from chain
3. Updates predecessor's next pointer
4. Reassigns roles (head/middle/tail)
5. Nodes pick up new configuration on next heartbeat

### State Consistency
- Writes are linearizable through head
- Reads reflect all committed writes at tail
- Sequence numbers ensure monotonic event ordering
- SQLite provides local persistence and recovery

## Load Balancing Strategy

**Current Implementation**: Simple assignment to HEAD node for all subscriptions.

**Production Alternatives**:
- Round-robin across all nodes
- Least-loaded node selection
- Topic-based hashing for locality
- Geographic proximity

## Persistence Model

### SQLite Storage
- Each node maintains its own SQLite database
- Schema includes: users, topics, messages, likes, sequence numbers
- On restart, nodes reload from disk
- Sequence numbers tracked for ordering

### Recovery
1. Node restarts and loads from SQLite
2. Registers with control plane
3. Receives role assignment
4. Begins serving requests (data already persisted)
5. Receives new replicated writes from predecessor

## Design Decisions

### Why SQLite?
- Simple embedded database (no external dependencies)
- ACID guarantees for local operations
- Easy development and testing
- Production would use replicated distributed databases

### Why gRPC?
- Strong typing with Protocol Buffers
- Bidirectional streaming for subscriptions
- Efficient binary protocol
- Built-in code generation

### Linearizability
- All writes go through single HEAD node
- Sequential replication ensures order
- Tail reads reflect committed HEAD writes

### Monotonic Ordering
- Global sequence numbers from HEAD
- Each event assigned increasing sequence
- Subscriptions deliver events in order
- Prevents out-of-order message delivery

## Testing Locally

### Scenario 1: Basic Operations
1. Start control plane + 3 nodes
2. Create users and topics
3. Post messages
4. Verify messages appear on tail reads

### Scenario 2: Subscriptions
1. Open client and subscribe to topic
2. Open second client
3. Post messages from second client
4. Verify first client receives events in real-time

### Scenario 3: Node Failure (Manual)
1. Start 3 nodes
2. Kill middle node (Ctrl+C)
3. Wait 15 seconds for failure detection
4. Verify chain reconfigures (check logs)
5. Verify writes and reads still work

### Scenario 4: Adding Nodes
1. Start with 1 node (becomes HEAD and TAIL)
2. Add second node (first becomes HEAD, second becomes TAIL)
3. Add third node (chain: HEAD → MIDDLE → TAIL)

## Production Considerations

### Current Limitations
- In-memory state during operations (writes to disk)
- No leader election (control plane is single point of failure)
- Simple failure detection (timeout-based)
- No network partition handling
- Limited to single control plane

### Production Improvements
- Use Raft/Paxos for control plane consensus
- Add state synchronization protocol for new nodes
- Implement checkpointing for faster recovery
- Add metrics and observability
- Handle network partitions (split-brain prevention)
- Add authentication and authorization
- Implement rate limiting
- Add comprehensive logging and tracing

## API Reference

See `proto/razpravljalnica.proto` for complete API specification.

### Key Services
- `MessageBoard`: Client-facing operations
- `ControlPlane`: Cluster management
- `Replication`: Internal node-to-node communication

## Troubleshooting

### "Failed to connect to control plane"
- Ensure control plane is running on port 50050
- Check firewall settings

### "Not the head/tail node"
- Wait a few seconds after startup for role assignment
- Check control plane logs for chain configuration

### "User/Topic does not exist"
- Ensure you've created users and topics before posting
- Check entity IDs match what was returned

### Database locked
- Only one node per database file
- Use different -db paths for each node

## License

MIT License - feel free to use and modify for your projects.

## Contributing

This is a reference implementation for educational purposes. Contributions welcome!