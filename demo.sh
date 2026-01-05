#!/bin/bash
# demo.sh - Automated demo of the message board

echo "=== MessageBoard System Demo ==="
echo "This script demonstrates basic functionality"
echo ""

CLIENT="./bin/client"

# Function to send command and show output
send_cmd() {
    echo "$ $1"
    echo "$1" | $CLIENT
    echo ""
    sleep 1
}

echo "Creating users..."
send_cmd "create-user Alice"
send_cmd "create-user Bob"
send_cmd "create-user Charlie"

echo "Creating topics..."
send_cmd "create-topic General"
send_cmd "create-topic Random"
send_cmd "create-topic Tech"

echo "Listing topics..."
send_cmd "list-topics"

echo "Posting messages..."
send_cmd "post 1 1 Hello everyone! This is Alice."
send_cmd "post 1 2 Hi Alice! Bob here."
send_cmd "post 1 3 Hey folks! Charlie joining."
send_cmd "post 2 1 Random thought of the day..."

echo "Getting messages from General topic..."
send_cmd "get-messages 1 0 10"

echo "Liking messages..."
send_cmd "like 1 1 2"
send_cmd "like 1 1 3"
send_cmd "like 1 2 1"

echo "Getting updated messages..."
send_cmd "get-messages 1 0 10"

echo ""
echo "=== Demo Complete ==="
echo "Try the interactive client with: $CLIENT"
echo "Test subscriptions with: subscribe 1 1"