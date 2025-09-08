package manager

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "instorage-manager/pkg/proto"
)

// JobState represents the state of a job
type JobState struct {
	Request        *pb.SubmitJobRequest
	Status         pb.JobStatus
	Message        string
	StartTime      *timestamppb.Timestamp
	CompletionTime *timestamppb.Timestamp
	OutputPath     string
	ErrorMessage   string
}

// InstorageManagerServer implements the InstorageManager gRPC service
type InstorageManagerServer struct {
	pb.UnimplementedInstorageManagerServer

	logger      logr.Logger
	nodeName    string
	csdEndpoint string

	// Job state management
	jobs   map[string]*JobState
	jobMux sync.RWMutex
}

// NewInstorageManagerServer creates a new gRPC server instance
func NewInstorageManagerServer(logger logr.Logger, nodeName, csdEndpoint string) *InstorageManagerServer {
	return &InstorageManagerServer{
		logger:      logger,
		nodeName:    nodeName,
		csdEndpoint: csdEndpoint,
		jobs:        make(map[string]*JobState),
	}
}

// SubmitJob handles job submission requests
func (s *InstorageManagerServer) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	s.logger.Info("Received job submission request",
		"jobId", req.JobId,
		"jobName", req.JobName,
		"namespace", req.Namespace,
		"targetNode", req.TargetNode,
	)

	// Validate request
	if req.JobId == "" {
		return &pb.SubmitJobResponse{
			Success: false,
			Message: "job_id is required",
		}, nil
	}

	if req.JobName == "" {
		return &pb.SubmitJobResponse{
			Success: false,
			Message: "job_name is required",
		}, nil
	}

	if req.Image == "" {
		return &pb.SubmitJobResponse{
			Success: false,
			Message: "image is required",
		}, nil
	}

	// Check if job already exists
	s.jobMux.RLock()
	existingJob, exists := s.jobs[req.JobId]
	s.jobMux.RUnlock()

	if exists {
		return &pb.SubmitJobResponse{
			Success: false,
			Message: fmt.Sprintf("job with ID %s already exists with status %v", req.JobId, existingJob.Status),
		}, nil
	}

	// Create job state
	now := timestamppb.Now()
	jobState := &JobState{
		Request:   req,
		Status:    pb.JobStatus_JOB_STATUS_PENDING,
		Message:   "Job received and queued for processing",
		StartTime: now,
	}

	// Store job state
	s.jobMux.Lock()
	s.jobs[req.JobId] = jobState
	s.jobMux.Unlock()

	// Start job processing asynchronously
	go s.processJob(req.JobId)

	s.logger.Info("Job accepted successfully",
		"jobId", req.JobId,
		"jobName", req.JobName,
	)

	return &pb.SubmitJobResponse{
		Success:     true,
		Message:     "Job submitted successfully",
		JobId:       req.JobId,
		SubmittedAt: now,
	}, nil
}

// GetJobStatus returns the current status of a job
func (s *InstorageManagerServer) GetJobStatus(ctx context.Context, req *pb.GetJobStatusRequest) (*pb.GetJobStatusResponse, error) {
	s.logger.V(1).Info("Job status request", "jobId", req.JobId)

	if req.JobId == "" {
		return &pb.GetJobStatusResponse{
			JobId:        "",
			Status:       pb.JobStatus_JOB_STATUS_UNKNOWN,
			Message:      "job_id is required",
			ErrorMessage: "job_id parameter is missing",
		}, nil
	}

	s.jobMux.RLock()
	jobState, exists := s.jobs[req.JobId]
	s.jobMux.RUnlock()

	if !exists {
		return &pb.GetJobStatusResponse{
			JobId:        req.JobId,
			Status:       pb.JobStatus_JOB_STATUS_UNKNOWN,
			Message:      "Job not found",
			ErrorMessage: fmt.Sprintf("no job found with ID: %s", req.JobId),
		}, nil
	}

	return &pb.GetJobStatusResponse{
		JobId:          req.JobId,
		Status:         jobState.Status,
		Message:        jobState.Message,
		StartTime:      jobState.StartTime,
		CompletionTime: jobState.CompletionTime,
		OutputPath:     jobState.OutputPath,
		ErrorMessage:   jobState.ErrorMessage,
	}, nil
}

// CancelJob cancels a running or pending job
func (s *InstorageManagerServer) CancelJob(ctx context.Context, req *pb.CancelJobRequest) (*pb.CancelJobResponse, error) {
	s.logger.Info("Job cancellation request", "jobId", req.JobId, "reason", req.Reason)

	if req.JobId == "" {
		return &pb.CancelJobResponse{
			Success: false,
			Message: "job_id is required",
		}, nil
	}

	s.jobMux.Lock()
	defer s.jobMux.Unlock()

	jobState, exists := s.jobs[req.JobId]
	if !exists {
		return &pb.CancelJobResponse{
			Success: false,
			Message: fmt.Sprintf("job with ID %s not found", req.JobId),
		}, nil
	}

	// Check if job can be cancelled
	if jobState.Status == pb.JobStatus_JOB_STATUS_COMPLETED ||
		jobState.Status == pb.JobStatus_JOB_STATUS_FAILED ||
		jobState.Status == pb.JobStatus_JOB_STATUS_CANCELLED {
		return &pb.CancelJobResponse{
			Success: false,
			Message: fmt.Sprintf("job cannot be cancelled, current status: %v", jobState.Status),
		}, nil
	}

	// Cancel the job
	now := timestamppb.Now()
	jobState.Status = pb.JobStatus_JOB_STATUS_CANCELLED
	jobState.Message = "Job cancelled by user request"
	if req.Reason != "" {
		jobState.Message = fmt.Sprintf("Job cancelled: %s", req.Reason)
	}
	jobState.CompletionTime = now

	s.logger.Info("Job cancelled successfully", "jobId", req.JobId)

	return &pb.CancelJobResponse{
		Success:     true,
		Message:     "Job cancelled successfully",
		CancelledAt: now,
	}, nil
}

// ListNodes returns information about available CSD nodes
func (s *InstorageManagerServer) ListNodes(ctx context.Context, req *pb.ListNodesRequest) (*pb.ListNodesResponse, error) {
	s.logger.V(1).Info("Node list request", "csdOnly", req.CsdOnly)

	// For now, return information about the current node
	// In a real implementation, this would query the Kubernetes API or a node registry
	nodes := []*pb.NodeResources{
		{
			NodeName:        s.nodeName,
			CpuCapacity:     "4",
			MemoryCapacity:  "8Gi",
			CpuAvailable:    "2",
			MemoryAvailable: "4Gi",
			RunningJobs:     s.getRunningJobCount(),
			CsdEnabled:      true,
			CsdStatus:       "ready",
		},
	}

	return &pb.ListNodesResponse{
		Nodes: nodes,
	}, nil
}

// processJob handles the actual job processing
func (s *InstorageManagerServer) processJob(jobId string) {
	s.logger.Info("Starting job processing", "jobId", jobId)

	// Get job state
	s.jobMux.RLock()
	jobState, exists := s.jobs[jobId]
	s.jobMux.RUnlock()

	if !exists {
		s.logger.Error(fmt.Errorf("job not found"), "Failed to process job", "jobId", jobId)
		return
	}

	// Check if job was cancelled before we started
	if jobState.Status == pb.JobStatus_JOB_STATUS_CANCELLED {
		s.logger.Info("Job was cancelled before processing started", "jobId", jobId)
		return
	}

	// Update job status to running
	s.jobMux.Lock()
	jobState.Status = pb.JobStatus_JOB_STATUS_RUNNING
	jobState.Message = "Job is being processed on CSD"
	s.jobMux.Unlock()

	s.logger.Info("Job status updated to running", "jobId", jobId)

	// Simulate job processing
	// In a real implementation, this would:
	// 1. Send the job to the CSD processor
	// 2. Monitor the job execution
	// 3. Handle job completion or failure

	// For demonstration, simulate processing time
	processingTime := time.Duration(30+(time.Now().UnixNano()%30)) * time.Second // 30-60 seconds
	s.logger.Info("Simulating job processing", "jobId", jobId, "estimatedDuration", processingTime)

	// Wait for processing (with periodic status checks)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	startTime := time.Now()
	for {
		select {
		case <-ticker.C:
			// Check if job was cancelled
			s.jobMux.RLock()
			if jobState.Status == pb.JobStatus_JOB_STATUS_CANCELLED {
				s.jobMux.RUnlock()
				s.logger.Info("Job processing interrupted - job was cancelled", "jobId", jobId)
				return
			}
			s.jobMux.RUnlock()

			// Check if processing is complete
			if time.Since(startTime) >= processingTime {
				// Job completed successfully
				s.jobMux.Lock()
				jobState.Status = pb.JobStatus_JOB_STATUS_COMPLETED
				jobState.Message = "Job completed successfully"
				jobState.CompletionTime = timestamppb.Now()
				jobState.OutputPath = fmt.Sprintf("/csd/output/%s", jobId)
				s.jobMux.Unlock()

				s.logger.Info("Job completed successfully", "jobId", jobId, "outputPath", jobState.OutputPath)
				return
			}

			// Update processing message
			elapsed := time.Since(startTime)
			remaining := processingTime - elapsed
			s.jobMux.Lock()
			jobState.Message = fmt.Sprintf("Job processing... (elapsed: %s, estimated remaining: %s)",
				elapsed.Round(time.Second), remaining.Round(time.Second))
			s.jobMux.Unlock()

		}
	}
}

// getRunningJobCount returns the number of currently running jobs
func (s *InstorageManagerServer) getRunningJobCount() int32 {
	s.jobMux.RLock()
	defer s.jobMux.RUnlock()

	count := int32(0)
	for _, job := range s.jobs {
		if job.Status == pb.JobStatus_JOB_STATUS_RUNNING {
			count++
		}
	}
	return count
}

// GetJobCount returns the total number of jobs managed by this server
func (s *InstorageManagerServer) GetJobCount() int {
	s.jobMux.RLock()
	defer s.jobMux.RUnlock()
	return len(s.jobs)
}
