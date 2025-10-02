package manager

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-logr/logr"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "instorage-manager/pkg/proto"
)

// TODO
// 1. ListNodes에서 CSD 정보 더 자세히

// InstorageManagerServer implements the InstorageManager gRPC service
type InstorageManagerServer struct {
	pb.UnimplementedInstorageManagerServer
	logger     logr.Logger
	nodeName   string
	httpClient *http.Client
	jobs       map[string]*JobState
	jobMux     sync.RWMutex
}

// NewInstorageManagerServer creates a new gRPC server instance
func NewInstorageManagerServer(logger logr.Logger, nodeName string) *InstorageManagerServer {
	return &InstorageManagerServer{
		logger:   logger,
		nodeName: nodeName,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		jobs: make(map[string]*JobState),
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

	// Comprehensive validation
	if err := s.validateSubmitJobRequest(req); err != nil {
		s.logger.Error(err, "Job validation failed", "jobId", req.JobId)
		return &pb.SubmitJobResponse{
			Success: false,
			Message: fmt.Sprintf("Validation failed: %v", err),
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
		BatchExecution: jobState.BatchExecution,
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

	// TODO : CSD 정보 더 구체적으로
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

// processJob handles the actual job processing by communicating with container processor
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

	// Check if job has batch plan
	if jobState.Request.BatchPlan != nil && len(jobState.Request.BatchPlan.Batches) > 0 {
		s.logger.Info("Processing job with batch plan",
			"jobId", jobId,
			"totalBatches", jobState.Request.BatchPlan.TotalBatches,
			"maxParallelJobs", jobState.Request.BatchPlan.MaxParallelJobs,
			"strategy", jobState.Request.BatchPlan.Strategy,
		)

		// Validate batch plan before processing
		if err := s.validateBatchPlan(jobState.Request.BatchPlan); err != nil {
			s.logger.Error(err, "Invalid batch plan", "jobId", jobId)
			s.jobMux.Lock()
			jobState.Status = pb.JobStatus_JOB_STATUS_FAILED
			jobState.Message = fmt.Sprintf("Invalid batch plan: %v", err)
			jobState.CompletionTime = timestamppb.Now()
			jobState.ErrorMessage = err.Error()
			s.jobMux.Unlock()
			return
		}

		s.processBatchJob(jobId)
	} else {
		s.logger.Info("Processing single job", "jobId", jobId)
		s.processSingleJob(jobId)
	}
}

// processSingleJob handles single job processing (original logic)
func (s *InstorageManagerServer) processSingleJob(jobId string) {
	s.jobMux.RLock()
	jobState, exists := s.jobs[jobId]
	s.jobMux.RUnlock()

	if !exists {
		return
	}

	// Submit job to container processor
	err := s.submitToContainerProcessor(jobState.Request)
	if err != nil {
		s.logger.Error(err, "Failed to submit job to container processor", "jobId", jobId)
		s.jobMux.Lock()
		jobState.Status = pb.JobStatus_JOB_STATUS_FAILED
		jobState.Message = fmt.Sprintf("Failed to submit to container processor: %v", err)
		jobState.CompletionTime = timestamppb.Now()
		jobState.ErrorMessage = err.Error()
		s.jobMux.Unlock()
		return
	}

	// Update job status to running
	s.jobMux.Lock()
	jobState.Status = pb.JobStatus_JOB_STATUS_RUNNING
	jobState.Message = "Job submitted to container processor"
	s.jobMux.Unlock()

	s.logger.Info("Job submitted to container processor", "jobId", jobId)

	// Monitor job status
	s.monitorJob(jobId)
}

// processBatchJob handles batch job processing
func (s *InstorageManagerServer) processBatchJob(jobId string) {
	s.jobMux.RLock()
	jobState, exists := s.jobs[jobId]
	s.jobMux.RUnlock()

	if !exists {
		return
	}

	batchPlan := jobState.Request.BatchPlan
	s.logger.Info("Starting batch job processing",
		"jobId", jobId,
		"totalBatches", len(batchPlan.Batches),
		"maxParallel", batchPlan.MaxParallelJobs,
		"strategy", batchPlan.Strategy,
	)

	// Initialize batch execution status
	s.jobMux.Lock()
	jobState.BatchExecution = &pb.BatchExecutionStatus{
		TotalBatches:     int32(len(batchPlan.Batches)),
		CompletedBatches: 0,
		FailedBatches:    0,
		RunningBatches:   0,
		ActiveBatches:    []*pb.BatchStatus{},
		BatchStrategy:    batchPlan.Strategy,
	}
	jobState.Status = pb.JobStatus_JOB_STATUS_RUNNING
	jobState.Message = "Processing batch job"
	s.jobMux.Unlock()

	// Create semaphore for parallel job control
	maxParallel := int(batchPlan.MaxParallelJobs)
	if maxParallel <= 0 {
		maxParallel = 5 // default
	}
	semaphore := make(chan struct{}, maxParallel)

	// Channel for collecting batch results
	batchResults := make(chan BatchResult, len(batchPlan.Batches))

	// Process batches according to strategy
	s.processBatchesByStrategy(jobId, batchPlan, semaphore, batchResults)

	// Monitor batch execution
	s.monitorBatchExecution(jobId, len(batchPlan.Batches), batchResults)
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

// processBatch processes a single batch within a batch job
func (s *InstorageManagerServer) processBatch(jobId string, batch *pb.BatchInfo, semaphore chan struct{}, results chan<- BatchResult) {
	// Acquire semaphore
	semaphore <- struct{}{}
	defer func() { <-semaphore }()

	startTime := time.Now()
	batchResult := BatchResult{
		BatchID:   batch.BatchId,
		JobID:     jobId,
		StartTime: startTime,
		ItemCount: batch.ItemCount,
	}

	s.logger.Info("Starting batch processing",
		"jobId", jobId,
		"batchId", batch.BatchId,
		"itemCount", batch.ItemCount,
		"fileCount", len(batch.FilePaths),
	)

	// Update running batch count
	s.updateBatchStatus(jobId, batch.BatchId, "Running", "Processing batch")

	// Create batch-specific request
	batchReq := s.createBatchRequest(jobId, batch)

	// Submit batch to container processor
	err := s.submitBatchToContainerProcessor(batchReq)
	if err != nil {
		s.logger.Error(err, "Failed to submit batch to container processor",
			"jobId", jobId,
			"batchId", batch.BatchId,
		)
		batchResult.Success = false
		batchResult.ErrorMessage = err.Error()
		batchResult.Message = "Failed to submit batch"
		batchResult.EndTime = time.Now()

		s.updateBatchStatus(jobId, batch.BatchId, "Failed", err.Error())
		results <- batchResult
		return
	}

	// Monitor batch execution
	success, message, errorMsg := s.monitorSingleBatchExecution(jobId, batch.BatchId)

	batchResult.Success = success
	batchResult.Message = message
	batchResult.ErrorMessage = errorMsg
	batchResult.EndTime = time.Now()

	// Update batch status
	status := "Completed"
	if !success {
		status = "Failed"
	}
	s.updateBatchStatus(jobId, batch.BatchId, status, message)

	s.logger.Info("Batch processing completed",
		"jobId", jobId,
		"batchId", batch.BatchId,
		"success", success,
		"duration", time.Since(startTime),
	)

	results <- batchResult
}

// processBatchesByStrategy processes batches according to the specified strategy
func (s *InstorageManagerServer) processBatchesByStrategy(jobId string, batchPlan *pb.BatchPlan, semaphore chan struct{}, results chan<- BatchResult) {
	strategy := batchPlan.Strategy
	if strategy == "" {
		strategy = "auto"
	}

	s.logger.Info("Processing batches with strategy",
		"jobId", jobId,
		"strategy", strategy,
		"totalBatches", len(batchPlan.Batches),
	)

	switch strategy {
	case "auto", "manual":
		// Default concurrent processing
		for _, batch := range batchPlan.Batches {
			go s.processBatch(jobId, batch, semaphore, results)
		}

	case "size-based":
		// Process larger batches first
		s.processBatchesBySizeOrder(jobId, batchPlan.Batches, semaphore, results)

	case "count-based":
		// Process batches with more items first
		s.processBatchesByCountOrder(jobId, batchPlan.Batches, semaphore, results)

	case "memory-based":
		// Process based on estimated memory usage
		s.processBatchesByMemoryOrder(jobId, batchPlan.Batches, semaphore, results)

	default:
		s.logger.Error(fmt.Errorf("unknown strategy"), "Unknown batch strategy", "strategy", strategy)
		// Fallback to default processing
		for _, batch := range batchPlan.Batches {
			go s.processBatch(jobId, batch, semaphore, results)
		}
	}
}

// processBatchesBySizeOrder processes batches ordered by size (largest first)
func (s *InstorageManagerServer) processBatchesBySizeOrder(jobId string, batches []*pb.BatchInfo, semaphore chan struct{}, results chan<- BatchResult) {
	// Sort batches by estimated size (descending)
	sortedBatches := make([]*pb.BatchInfo, len(batches))
	copy(sortedBatches, batches)

	// Simple bubble sort by estimated size bytes
	for i := 0; i < len(sortedBatches)-1; i++ {
		for j := 0; j < len(sortedBatches)-i-1; j++ {
			if sortedBatches[j].EstimatedSizeBytes < sortedBatches[j+1].EstimatedSizeBytes {
				sortedBatches[j], sortedBatches[j+1] = sortedBatches[j+1], sortedBatches[j]
			}
		}
	}

	s.logger.Info("Processing batches by size order (largest first)", "jobId", jobId)
	for _, batch := range sortedBatches {
		go s.processBatch(jobId, batch, semaphore, results)
	}
}

// processBatchesByCountOrder processes batches ordered by item count (highest first)
func (s *InstorageManagerServer) processBatchesByCountOrder(jobId string, batches []*pb.BatchInfo, semaphore chan struct{}, results chan<- BatchResult) {
	// Sort batches by item count (descending)
	sortedBatches := make([]*pb.BatchInfo, len(batches))
	copy(sortedBatches, batches)

	// Simple bubble sort by item count
	for i := 0; i < len(sortedBatches)-1; i++ {
		for j := 0; j < len(sortedBatches)-i-1; j++ {
			if sortedBatches[j].ItemCount < sortedBatches[j+1].ItemCount {
				sortedBatches[j], sortedBatches[j+1] = sortedBatches[j+1], sortedBatches[j]
			}
		}
	}

	s.logger.Info("Processing batches by count order (highest first)", "jobId", jobId)
	for _, batch := range sortedBatches {
		go s.processBatch(jobId, batch, semaphore, results)
	}
}

// processBatchesByMemoryOrder processes batches based on estimated memory usage
func (s *InstorageManagerServer) processBatchesByMemoryOrder(jobId string, batches []*pb.BatchInfo, semaphore chan struct{}, results chan<- BatchResult) {
	// Estimate memory usage based on size and item count
	type batchWithMemory struct {
		batch           *pb.BatchInfo
		estimatedMemory int64
	}

	batchesWithMemory := make([]batchWithMemory, len(batches))
	for i, batch := range batches {
		// Simple estimation: size + (itemCount * 1KB for metadata)
		estimatedMemory := batch.EstimatedSizeBytes + int64(batch.ItemCount*1024)
		batchesWithMemory[i] = batchWithMemory{
			batch:           batch,
			estimatedMemory: estimatedMemory,
		}
	}

	// Sort by estimated memory (descending)
	for i := 0; i < len(batchesWithMemory)-1; i++ {
		for j := 0; j < len(batchesWithMemory)-i-1; j++ {
			if batchesWithMemory[j].estimatedMemory < batchesWithMemory[j+1].estimatedMemory {
				batchesWithMemory[j], batchesWithMemory[j+1] = batchesWithMemory[j+1], batchesWithMemory[j]
			}
		}
	}

	s.logger.Info("Processing batches by memory order (highest first)", "jobId", jobId)
	for _, bwm := range batchesWithMemory {
		go s.processBatch(jobId, bwm.batch, semaphore, results)
	}
}

// SetupWebhookRoutes configures HTTP webhook endpoints
func (s *InstorageManagerServer) SetupWebhookRoutes(router *gin.Engine) {
	webhook := router.Group("/api/v1/webhook")
	{
		webhook.POST("/container/status", s.handleContainerStatusUpdate)
		webhook.GET("/health", s.handleWebhookHealth)
	}
}

// handleContainerStatusUpdate processes container status updates from container-processor
func (s *InstorageManagerServer) handleContainerStatusUpdate(c *gin.Context) {
	var update ContainerStatusUpdate

	if err := c.ShouldBindJSON(&update); err != nil {
		s.logger.Error(err, "Invalid webhook payload")
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Invalid JSON payload",
		})
		return
	}

	s.logger.Info("Received container status update",
		"jobId", update.JobID,
		"status", update.Status,
		"message", update.Message,
		"containerId", update.ContainerID,
	)

	// Validate required fields
	if update.JobID == "" || update.Status == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "job_id and status are required",
		})
		return
	}

	// Find and update job state
	s.jobMux.Lock()
	defer s.jobMux.Unlock()

	jobState, exists := s.jobs[update.JobID]
	if !exists {
		s.logger.Error(fmt.Errorf("job not found"), "Container status update for unknown job", "jobId", update.JobID)
		c.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"error":   fmt.Sprintf("job %s not found", update.JobID),
		})
		return
	}

	// Convert status to protobuf enum
	var pbStatus pb.JobStatus
	switch update.Status {
	case "running":
		pbStatus = pb.JobStatus_JOB_STATUS_RUNNING
	case "completed":
		pbStatus = pb.JobStatus_JOB_STATUS_COMPLETED
		jobState.CompletionTime = timestamppb.Now()
	case "failed":
		pbStatus = pb.JobStatus_JOB_STATUS_FAILED
		jobState.CompletionTime = timestamppb.Now()
		if update.ErrorMessage != "" {
			jobState.ErrorMessage = update.ErrorMessage
		}
	case "cancelled":
		pbStatus = pb.JobStatus_JOB_STATUS_CANCELLED
		jobState.CompletionTime = timestamppb.Now()
	default:
		s.logger.Error(fmt.Errorf("unknown status"), "Unknown container status", "status", update.Status)
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   fmt.Sprintf("unknown status: %s", update.Status),
		})
		return
	}

	// Update job state
	jobState.Status = pbStatus
	jobState.Message = update.Message

	s.logger.Info("Updated job status from container processor webhook",
		"jobId", update.JobID,
		"oldStatus", jobState.Status,
		"newStatus", pbStatus,
		"message", update.Message,
	)

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "Status updated successfully",
		"job_id":  update.JobID,
	})
}

// handleWebhookHealth returns health status for webhook endpoints
func (s *InstorageManagerServer) handleWebhookHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":    "healthy",
		"service":   "instorage-manager-webhook",
		"timestamp": time.Now().Unix(),
		"node":      s.nodeName,
	})
}
