# Instorage Manager

Instorage Manager is a gRPC-based service that manages preprocessing jobs for CSD (Computational Storage Device) nodes. It receives job requests from the instorage-operator and handles job execution, monitoring, and status reporting.

## Features

- **gRPC API**: Modern, efficient communication protocol
- **Job Management**: Submit, monitor, and cancel preprocessing jobs
- **Node Management**: Track CSD node resources and availability
- **Health Checks**: Built-in health check endpoint
- **Reflection Support**: gRPC reflection for debugging and testing
- **Graceful Shutdown**: Proper signal handling and graceful server shutdown

## Architecture

```
instorage-operator (Kubernetes Operator)
    ↓ gRPC calls
instorage-manager (This service)
    ↓ HTTP calls (future implementation)  
CSD Processor (Actual preprocessing execution)
```

## API Services

The server implements the following gRPC services:

### InstorageManager Service

- **SubmitJob**: Submit a new preprocessing job
- **GetJobStatus**: Query the status of a running job
- **CancelJob**: Cancel a pending or running job
- **ListNodes**: Get information about available CSD nodes

### Health Service

- Standard gRPC health check service for monitoring

## Quick Start

### 1. Build the Project

```bash
# Build server
go build -o bin/instorage-manager ./cmd/main.go

# Build test client
go build -o bin/test-client ./cmd/test-client/main.go
```

### 2. Start the Server

```bash
# Using the convenience script
./scripts/start-server.sh

# Or manually
./bin/instorage-manager \
    --node-name="worker-node-1" \
    --grpc-port="50051" \
    --csd-endpoint="http://localhost:8080"
```

### 3. Test the Server

```bash
# Using the test script
./scripts/test-client.sh

# Or manually
./bin/test-client
```

## Configuration

The server accepts the following command-line flags:

- `--grpc-port`: gRPC server port (default: "50051")
- `--node-name`: Name of the node this manager runs on (required)
- `--csd-endpoint`: CSD processor endpoint (default: "http://localhost:8080")
- `--enable-reflection`: Enable gRPC reflection (default: true)

Environment variables:
- `NODE_NAME`: Alternative way to set the node name

## Job Lifecycle

1. **Submission**: Operator submits job via `SubmitJob` gRPC call
2. **Validation**: Server validates job parameters
3. **Queuing**: Job is queued for processing
4. **Execution**: Job is sent to CSD processor (simulated for now)
5. **Monitoring**: Status can be queried via `GetJobStatus`
6. **Completion**: Job completes with success or failure

## Job Status Values

- `JOB_STATUS_UNKNOWN`: Invalid or unknown job
- `JOB_STATUS_PENDING`: Job is queued for execution
- `JOB_STATUS_RUNNING`: Job is currently being processed
- `JOB_STATUS_COMPLETED`: Job completed successfully
- `JOB_STATUS_FAILED`: Job failed during execution
- `JOB_STATUS_CANCELLED`: Job was cancelled by user request

## Example Usage

### Submit a Job

```go
client := pb.NewInstorageManagerClient(conn)

response, err := client.SubmitJob(ctx, &pb.SubmitJobRequest{
    JobId:      "job-001",
    JobName:    "preprocessing-task",
    Namespace:  "default",
    Image:      "preprocessing:latest",
    DataPath:   "/data/input",
    OutputPath: "/data/output",
    TargetNode: "worker-node-1",
    Resources: &pb.Resources{
        Requests: &pb.ResourceRequirements{
            Cpu:    "2",
            Memory: "4Gi",
        },
    },
    Csd: &pb.CSDConfig{
        Enabled:    true,
        DevicePath: "/dev/nvme0n1",
    },
})
```

### Monitor Job Status

```go
statusResp, err := client.GetJobStatus(ctx, &pb.GetJobStatusRequest{
    JobId: "job-001",
})

fmt.Printf("Job status: %v\n", statusResp.Status)
fmt.Printf("Message: %s\n", statusResp.Message)
```

### Cancel a Job

```go
cancelResp, err := client.CancelJob(ctx, &pb.CancelJobRequest{
    JobId:  "job-001",
    Reason: "User requested cancellation",
})
```

## Development

### Project Structure

```
├── cmd/
│   ├── main.go              # Server main application
│   └── test-client/         # Test client application
├── pkg/
│   ├── proto/              # Protocol buffer definitions and generated code
│   └── server/             # gRPC server implementation
├── scripts/                # Helper scripts
├── test/                   # Test utilities
└── bin/                    # Built binaries
```

### Building Protocol Buffers

```bash
cd pkg/proto
./build-proto.sh
```

### Running Tests

```bash
# Start server in one terminal
./scripts/start-server.sh

# Run tests in another terminal  
./scripts/test-client.sh
```

## Production Deployment

For production deployment, consider:

1. **Container Deployment**: Create Docker images for the server
2. **Kubernetes Integration**: Deploy as DaemonSet on CSD nodes
3. **TLS Security**: Enable TLS for gRPC communications
4. **Monitoring**: Add metrics and logging integration
5. **Persistence**: Add job state persistence for reliability
6. **Load Balancing**: Configure proper service discovery

## Integration with instorage-operator

The instorage-operator calls this service when CSD mode is enabled:

1. Operator receives InstorageJob CRD
2. Operator selects appropriate CSD node
3. Operator calls `SubmitJob` on the node's manager
4. Operator polls `GetJobStatus` for updates
5. Operator updates job status in Kubernetes

## Troubleshooting

### Server won't start

- Check if port 50051 is available: `netstat -tuln | grep 50051`
- Verify node name is provided: `--node-name` or `NODE_NAME` env var
- Check logs for detailed error messages

### gRPC connection issues

- Verify server is listening: `grpc_health_probe -addr=localhost:50051`
- Test with grpcurl: `grpcurl -plaintext localhost:50051 list`
- Check firewall and network connectivity

### Job processing issues

- Check server logs for job processing details
- Verify CSD endpoint is accessible
- Monitor job status transitions using test client