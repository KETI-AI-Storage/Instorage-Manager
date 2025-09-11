package manager

import (
	pb "instorage-manager/pkg/proto"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// ContainerJobRequest represents the job request sent to container processor
type ContainerJobRequest struct {
	JobID           string              `json:"job_id"`
	JobName         string              `json:"job_name"`
	Namespace       string              `json:"namespace"`
	Image           string              `json:"image"`
	ImagePullPolicy string              `json:"image_pull_policy,omitempty"`
	DataPath        string              `json:"data_path"`
	OutputPath      string              `json:"output_path"`
	Environment     map[string]string   `json:"environment,omitempty"`
	Resources       *ContainerResources `json:"resources,omitempty"`
	Labels          map[string]string   `json:"labels,omitempty"`
}

// ContainerResources represents resource requirements for container
type ContainerResources struct {
	CPULimit      string `json:"cpu_limit,omitempty"`
	MemoryLimit   string `json:"memory_limit,omitempty"`
	CPURequest    string `json:"cpu_request,omitempty"`
	MemoryRequest string `json:"memory_request,omitempty"`
}

// ContainerJobResponse represents the response from container processor
type ContainerJobResponse struct {
	Success     bool   `json:"success"`
	Message     string `json:"message"`
	JobID       string `json:"job_id"`
	ContainerID string `json:"container_id,omitempty"`
}

// ContainerJobStatus represents job status from container processor
type ContainerJobStatus struct {
	JobID        string `json:"job_id"`
	ContainerID  string `json:"container_id,omitempty"`
	Status       string `json:"status"` // pending, running, completed, failed, cancelled
	Message      string `json:"message"`
	StartTime    string `json:"start_time,omitempty"`
	EndTime      string `json:"end_time,omitempty"`
	ExitCode     int    `json:"exit_code,omitempty"`
	OutputPath   string `json:"output_path,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}

// JobState represents the state of a job
type JobState struct {
	Request        *pb.SubmitJobRequest
	Status         pb.JobStatus
	Message        string
	StartTime      *timestamppb.Timestamp
	CompletionTime *timestamppb.Timestamp
	OutputPath     string
	ErrorMessage   string
	BatchExecution *pb.BatchExecutionStatus
}

// BatchResult represents the result of processing a single batch
type BatchResult struct {
	BatchID      string
	JobID        string
	Success      bool
	Message      string
	ErrorMessage string
	StartTime    time.Time
	EndTime      time.Time
	ItemCount    int32
}
