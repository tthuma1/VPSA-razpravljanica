# Makefile

.PHONY: all proto build clean run-control run-node1 run-node2 run-node3 run-client test

all: proto build

# Generate protobuf code
proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/razpravljalnica.proto

# Build all binaries
build:
	go build -o bin/controlplane cmd/controlplane/main.go
	go build -o bin/server cmd/server/main.go
	go build -o bin/client cmd/client/main.go

# Clean build artifacts
clean:
	rm -rf bin/
	rm -f *.db

# Run control plane
run-control:
	./bin/controlplane

# Run data plane nodes
run-node1:
	./bin/server -id=node1 -port=50051 -db=node1.db

run-node2:
	./bin/server -id=node2 -port=50052 -db=node2.db

run-node3:
	./bin/server -id=node3 -port=50053 -db=node3.db

# Run client
run-client:
	./bin/client

# Run tests
test:
	go test ./test/... -v
