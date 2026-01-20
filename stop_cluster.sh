#!/bin/bash
# stop_cluster.sh - Stop all running nodes

echo "Stopping MessageBoard cluster..."

if [ -f ".pids" ]; then
    while read pid; do
        if kill -0 $pid 2>/dev/null; then
            echo "Stopping process $pid..."
            kill $pid
        fi
    done < .pids
    rm .pids
    echo "Cluster stopped."
else
    echo "No .pids file found. Cluster may not be running."
    echo "You can manually check with: ps aux | grep -E 'controlplane|id=node'"
fi