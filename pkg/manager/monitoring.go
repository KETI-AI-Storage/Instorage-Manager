package manager

import (
	"encoding/json"
	"fmt"
	pb "instorage-manager/pkg/proto"
	"net/http"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// monitorJob monitors the job status by polling container processor
func (s *InstorageManagerServer) monitorJob(jobId string) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	s.logger.Info("Starting job monitoring", "jobId", jobId)

	consecutiveErrors := 0
	maxConsecutiveErrors := 5 // Stop monitoring after 5 consecutive errors

	for range ticker.C {
		// Check if job was cancelled locally
		s.jobMux.RLock()
		jobState, exists := s.jobs[jobId]
		if !exists || jobState.Status == pb.JobStatus_JOB_STATUS_CANCELLED {
			s.jobMux.RUnlock()
			s.logger.Info("Job monitoring stopped - job was cancelled", "jobId", jobId)
			return
		}
		s.jobMux.RUnlock()

		// Get status from container processor
		status, err := s.getJobStatusFromContainerProcessor(jobId)
		if err != nil {
			consecutiveErrors++
			s.logger.Error(err, "Failed to get job status from container processor",
				"jobId", jobId,
				"consecutiveErrors", consecutiveErrors,
			)

			// If we have too many consecutive errors, assume the resource was deleted
			if consecutiveErrors >= maxConsecutiveErrors {
				s.logger.Info("Job monitoring stopped due to consecutive errors (resource may have been deleted)",
					"jobId", jobId,
				)

				// Update job state to failed
				s.jobMux.Lock()
				if jobState, exists := s.jobs[jobId]; exists {
					jobState.Status = pb.JobStatus_JOB_STATUS_FAILED
					jobState.Message = "Job monitoring stopped - unable to contact container processor"
					jobState.ErrorMessage = "Resource may have been deleted via kubectl"
					jobState.CompletionTime = timestamppb.Now()
				}
				s.jobMux.Unlock()
				return
			}
			continue
		}

		// Reset error counter on successful status check
		consecutiveErrors = 0

		// Update job state based on container status
		s.updateJobStateFromContainer(jobId, status)

		// If job is completed, failed, or cancelled, stop monitoring
		if status.Status == "completed" || status.Status == "failed" || status.Status == "cancelled" {
			s.logger.Info("Job monitoring completed", "jobId", jobId, "finalStatus", status.Status)
			return
		}
	}
}

// getJobStatusFromContainerProcessor queries container processor for job status
func (s *InstorageManagerServer) getJobStatusFromContainerProcessor(jobId string) (*ContainerJobStatus, error) {
	url := fmt.Sprintf("%s/api/v1/jobs/%s/status", "s.csdEndpoint", jobId)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request to container processor: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("container processor returned status %d", resp.StatusCode)
	}

	var status ContainerJobStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("failed to decode status response: %w", err)
	}

	return &status, nil
}

// updateJobStateFromContainer updates local job state based on container processor status
func (s *InstorageManagerServer) updateJobStateFromContainer(jobId string, containerStatus *ContainerJobStatus) {
	s.jobMux.Lock()
	defer s.jobMux.Unlock()

	jobState, exists := s.jobs[jobId]
	if !exists {
		return
	}

	// Map container status to gRPC status
	var grpcStatus pb.JobStatus
	switch containerStatus.Status {
	case "pending":
		grpcStatus = pb.JobStatus_JOB_STATUS_PENDING
	case "running":
		grpcStatus = pb.JobStatus_JOB_STATUS_RUNNING
	case "completed":
		grpcStatus = pb.JobStatus_JOB_STATUS_COMPLETED
		jobState.CompletionTime = timestamppb.Now()
		jobState.OutputPath = containerStatus.OutputPath
	case "failed":
		grpcStatus = pb.JobStatus_JOB_STATUS_FAILED
		jobState.CompletionTime = timestamppb.Now()
		jobState.ErrorMessage = containerStatus.ErrorMessage
	case "cancelled":
		grpcStatus = pb.JobStatus_JOB_STATUS_CANCELLED
		jobState.CompletionTime = timestamppb.Now()
	default:
		grpcStatus = pb.JobStatus_JOB_STATUS_UNKNOWN
	}

	// Update job state
	jobState.Status = grpcStatus
	jobState.Message = containerStatus.Message

	s.logger.V(1).Info("Updated job state from container processor",
		"jobId", jobId,
		"status", containerStatus.Status,
		"message", containerStatus.Message,
	)
}

// monitorBatchExecution monitors the overall batch job execution
func (s *InstorageManagerServer) monitorBatchExecution(jobId string, totalBatches int, results <-chan BatchResult) {
	completedBatches := 0
	failedBatches := 0

	s.logger.Info("Starting batch execution monitoring",
		"jobId", jobId,
		"totalBatches", totalBatches,
	)

	for completedBatches+failedBatches < totalBatches {
		select {
		case result := <-results:
			s.logger.V(1).Info("Received batch result",
				"jobId", result.JobID,
				"batchId", result.BatchID,
				"success", result.Success,
				"duration", result.EndTime.Sub(result.StartTime),
			)

			if result.Success {
				completedBatches++
			} else {
				failedBatches++
				s.logger.Error(fmt.Errorf(result.ErrorMessage), "Batch failed",
					"jobId", result.JobID,
					"batchId", result.BatchID,
				)
			}

			// Update job state
			s.updateBatchExecutionStatus(jobId, completedBatches, failedBatches)
		}
	}

	// Final status update
	s.finalizeBatchJob(jobId, completedBatches, failedBatches, totalBatches)
}

// monitorSingleBatchExecution monitors a single batch execution
func (s *InstorageManagerServer) monitorSingleBatchExecution(jobId, batchId string) (bool, string, string) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	timeout := time.NewTimer(30 * time.Minute) // 30 minute timeout per batch
	defer timeout.Stop()

	batchJobId := fmt.Sprintf("%s-batch-%s", jobId, batchId)
	consecutiveErrors := 0
	maxConsecutiveErrors := 5 // Stop monitoring after 5 consecutive errors

	for {
		select {
		case <-timeout.C:
			return false, "Batch execution timeout", "Batch processing exceeded 30 minute timeout"

		case <-ticker.C:
			// Check container processor for batch status
			status, err := s.getJobStatusFromContainerProcessor(batchJobId)
			if err != nil {
				consecutiveErrors++
				s.logger.V(1).Info("Failed to get batch status",
					"error", err.Error(),
					"consecutiveErrors", consecutiveErrors,
					"batchJobId", batchJobId,
				)

				// If we have too many consecutive errors, assume the resource was deleted
				if consecutiveErrors >= maxConsecutiveErrors {
					s.logger.Info("Batch monitoring stopped due to consecutive errors (resource may have been deleted)",
						"jobId", jobId,
						"batchId", batchId,
						"batchJobId", batchJobId,
					)
					return false, "Batch monitoring failed", "Unable to contact container processor - resource may have been deleted"
				}
				continue
			}

			// Reset error counter on successful status check
			consecutiveErrors = 0

			switch status.Status {
			case "completed":
				return true, "Batch completed successfully", ""
			case "failed":
				return false, "Batch processing failed", status.ErrorMessage
			case "cancelled":
				return false, "Batch was cancelled", ""
			case "running", "pending":
				// Continue monitoring
				continue
			default:
				s.logger.V(1).Info("Unknown batch status", "status", status.Status)
				continue
			}
		}
	}
}

// updateBatchStatus updates the status of a specific batch
func (s *InstorageManagerServer) updateBatchStatus(jobId, batchId, status, message string) {
	s.jobMux.Lock()
	defer s.jobMux.Unlock()

	jobState, exists := s.jobs[jobId]
	if !exists || jobState.BatchExecution == nil {
		return
	}

	// Find and update the batch status
	found := false
	for _, activeBatch := range jobState.BatchExecution.ActiveBatches {
		if activeBatch.BatchId == batchId {
			activeBatch.Status = status
			activeBatch.Message = message
			if status == "Running" && activeBatch.StartTime == nil {
				activeBatch.StartTime = timestamppb.Now()
			}
			found = true
			break
		}
	}

	// If not found in active batches, add it
	if !found {
		newBatch := &pb.BatchStatus{
			BatchId:   batchId,
			Status:    status,
			Message:   message,
			StartTime: timestamppb.Now(),
		}
		jobState.BatchExecution.ActiveBatches = append(jobState.BatchExecution.ActiveBatches, newBatch)
	}

	// Update running batch count
	runningCount := int32(0)
	for _, batch := range jobState.BatchExecution.ActiveBatches {
		if batch.Status == "Running" {
			runningCount++
		}
	}
	jobState.BatchExecution.RunningBatches = runningCount
}

// updateBatchExecutionStatus updates the batch execution status
func (s *InstorageManagerServer) updateBatchExecutionStatus(jobId string, completed, failed int) {
	s.jobMux.Lock()
	defer s.jobMux.Unlock()

	jobState, exists := s.jobs[jobId]
	if !exists || jobState.BatchExecution == nil {
		return
	}

	jobState.BatchExecution.CompletedBatches = int32(completed)
	jobState.BatchExecution.FailedBatches = int32(failed)

	// Calculate progress percentage
	total := int(jobState.BatchExecution.TotalBatches)
	progressPercentage := float64(completed+failed) / float64(total) * 100

	// Update job message with more details
	jobState.Message = fmt.Sprintf("Batch processing: %d completed, %d failed of %d total (%.1f%% progress)",
		completed, failed, total, progressPercentage)

	s.logger.V(1).Info("Updated batch execution status",
		"jobId", jobId,
		"completed", completed,
		"failed", failed,
		"total", jobState.BatchExecution.TotalBatches,
	)
}

// finalizeBatchJob finalizes the batch job based on results
func (s *InstorageManagerServer) finalizeBatchJob(jobId string, completed, failed, total int) {
	s.jobMux.Lock()
	defer s.jobMux.Unlock()

	jobState, exists := s.jobs[jobId]
	if !exists {
		return
	}

	now := timestamppb.Now()

	if failed == 0 {
		// All batches completed successfully
		jobState.Status = pb.JobStatus_JOB_STATUS_COMPLETED
		jobState.Message = fmt.Sprintf("All %d batches completed successfully", completed)
		s.logger.Info("Batch job completed successfully",
			"jobId", jobId,
			"completedBatches", completed,
		)
	} else if completed > 0 {
		// Partial success
		jobState.Status = pb.JobStatus_JOB_STATUS_COMPLETED
		jobState.Message = fmt.Sprintf("Batch job completed with partial success: %d completed, %d failed", completed, failed)
		s.logger.Info("Batch job completed with partial success",
			"jobId", jobId,
			"completedBatches", completed,
			"failedBatches", failed,
		)
	} else {
		// All batches failed
		jobState.Status = pb.JobStatus_JOB_STATUS_FAILED
		jobState.Message = fmt.Sprintf("All %d batches failed", total)
		jobState.ErrorMessage = "All batch executions failed"
		s.logger.Error(fmt.Errorf("all batches failed"), "Batch job failed completely",
			"jobId", jobId,
			"failedBatches", failed,
		)
	}

	jobState.CompletionTime = now
	if jobState.BatchExecution != nil {
		jobState.BatchExecution.EstimatedCompletion = now
	}
}
