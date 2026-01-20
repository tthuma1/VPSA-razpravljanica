#!/bin/bash
# run_cluster.sh - Start a highly available 3-node control plane cluster and 3 data nodes

set -e

echo "=== MessageBoard Distributed System (HA Control Plane) ==="
echo "Starting 3 control plane nodes and 3 data nodes..."
echo ""

# Clean up old data
rm -rf /tmp/raft1 /tmp/raft2 /tmp/raft3
rm -f *.db 2>/dev/null || true
mkdir -p /tmp/raft1 /tmp/raft2 /tmp/raft3
mkdir -p logs

# Build binaries
echo "Building binaries..."
make build

# Start Control Plane Node 1 (Leader)
echo "Starting Control Plane Node 1 (Leader)..."
./bin/controlplane -port 50051 -raft-id node1 -raft-addr localhost:60051 -raft-dir /tmp/raft1 -bootstrap -http-port 51051 > logs/cp1.log 2>&1 &
CP1_PID=$!
sleep 2

# Start Control Plane Node 2
echo "Starting Control Plane Node 2..."
./bin/controlplane -port 50052 -raft-id node2 -raft-addr localhost:60052 -raft-dir /tmp/raft2 -http-port 51052 > logs/cp2.log 2>&1 &
CP2_PID=$!
sleep 2

# Start Control Plane Node 3
echo "Starting Control Plane Node 3..."
./bin/controlplane -port 50053 -raft-id node3 -raft-addr localhost:60053 -raft-dir /tmp/raft3 -http-port 51053 > logs/cp3.log 2>&1 &
CP3_PID=$!
sleep 2

# Join nodes to cluster
echo "Joining Node 2 to cluster..."
curl -s -X POST http://localhost:51051/join -d '{"node_id": "node2", "raft_addr": "localhost:60052"}' > /dev/null
sleep 1

echo "Joining Node 3 to cluster..."
curl -s -X POST http://localhost:51051/join -d '{"node_id": "node3", "raft_addr": "localhost:60053"}' > /dev/null
sleep 2

echo "Control Plane Cluster formed."
echo ""

# Start Data Nodes
echo "Starting Data Node 1 (HEAD)..."
./bin/server -id=dp-node1 -port=8081 -control=localhost:50051 -db=dp1.db > logs/dp1.log 2>&1 &
DP1_PID=$!
sleep 2

echo "Starting Data Node 2 (MIDDLE)..."
./bin/server -id=dp-node2 -port=8082 -control=localhost:50051 -db=dp2.db > logs/dp2.log 2>&1 &
DP2_PID=$!
sleep 2

echo "Starting Data Node 3 (TAIL)..."
./bin/server -id=dp-node3 -port=8083 -control=localhost:50051 -db=dp3.db > logs/dp3.log 2>&1 &
DP3_PID=$!
sleep 2

echo ""
echo "=== Cluster Started ==="
echo "Control Plane PIDs: $CP1_PID, $CP2_PID, $CP3_PID"
echo "Data Node PIDs: $DP1_PID, $DP2_PID, $DP3_PID"
echo ""
echo "Logs available in logs/ directory"
echo "Run client with: ./bin/client"
echo "(Client will automatically connect to the cluster)"
echo ""
echo "To stop all: ./stop_cluster.sh"

# Save PIDs for cleanup
echo "$CP1_PID" > .pids
echo "$CP2_PID" >> .pids
echo "$CP3_PID" >> .pids
echo "$DP1_PID" >> .pids
echo "$DP2_PID" >> .pids
echo "$DP3_PID" >> .pids
