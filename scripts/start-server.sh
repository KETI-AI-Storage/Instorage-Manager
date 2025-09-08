#!/bin/bash

# Start the Instorage Manager gRPC server
echo "Starting Instorage Manager gRPC Server..."

# Build the server
echo "Building server..."
go build -o bin/instorage-manager ./cmd/main.go

if [ $? -ne 0 ]; then
    echo "❌ Build failed!"
    exit 1
fi

echo "✅ Build successful!"

# Run the server
echo "Starting server on port 50051..."
echo "Node name: test-node-1"
echo "CSD endpoint: http://localhost:8080"
echo ""

./bin/instorage-manager \
    --node-name="test-node-1" \
    --grpc-port="50051" \
    --csd-endpoint="http://localhost:8080" \
    --enable-reflection=true