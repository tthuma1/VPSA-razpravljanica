Project for the Parallel and Distributed Systems and Algorithms course.

Quick start:
```
# Setup
make proto && make build
mkdir -p logs

# Start cluster
./run_cluster.sh

# In another terminal, run client
./bin/client

# Try commands like:
create-user Alice
create-topic General
post 1 1 Hello world!
list-topics
```

Or try running `demo.sh`.