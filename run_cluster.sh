#!/bin/bash
# run_cluster.sh - Start a 3-node cluster locally

set -e

echo "=== MessageBoard Distributed System ==="
echo "Starting control plane and 3 data nodes..."
echo ""

# Clean up old databases
#rm -f *.db 2>/dev/null || true

# Check if binaries exist
if [ ! -f "bin/controlplane" ] || [ ! -f "bin/server" ]; then
    echo "Building binaries..."
    make build
fi

# Start control plane in background
echo "Starting control plane..."
./bin/controlplane > logs/control.log 2>&1 &
CONTROL_PID=$!
sleep 2

# Start 3 data nodes
echo "Starting Node 1 (will become HEAD)..."
./bin/server -id=node1 -port=50051 -db=node1.db > logs/node1.log 2>&1 &
NODE1_PID=$!
sleep 2

echo "Starting Node 2 (will become MIDDLE)..."
./bin/server -id=node2 -port=50052 -db=node2.db > logs/node2.log 2>&1 &
NODE2_PID=$!
sleep 2

echo "Starting Node 3 (will become TAIL)..."
./bin/server -id=node3 -port=50053 -db=node3.db > logs/node3.log 2>&1 &
NODE3_PID=$!
sleep 2

echo ""
echo "=== Cluster Started ==="
echo "Control Plane PID: $CONTROL_PID"
echo "Node 1 PID: $NODE1_PID (HEAD)"
echo "Node 2 PID: $NODE2_PID (MIDDLE)"
echo "Node 3 PID: $NODE3_PID (TAIL)"
echo ""
echo "Logs available in logs/ directory"
echo "Run client with: ./bin/client"
echo ""
echo "To stop all: kill $CONTROL_PID $NODE1_PID $NODE2_PID $NODE3_PID"
echo "Or run: ./stop_cluster.sh"

# Save PIDs for cleanup
echo "$CONTROL_PID" > .pids
echo "$NODE1_PID" >> .pids
echo "$NODE2_PID" >> .pids
echo "$NODE3_PID" >> .pids