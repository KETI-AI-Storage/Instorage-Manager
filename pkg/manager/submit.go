package manager

import (
	// "bytes"  // Commented out for test mode
	"encoding/json"
	"fmt"
	pb "instorage-manager/pkg/proto"
	// "net/http"  // Commented out for test mode
	"strings"
)

// createBatchRequest creates a container processor request for a specific batch
func (s *InstorageManagerServer) createBatchRequest(jobId string, batch *pb.BatchInfo) *pb.SubmitJobRequest {
	s.jobMux.RLock()
	originalReq := s.jobs[jobId].Request
	s.jobMux.RUnlock()

	// Create batch-specific job request
	batchReq := &pb.SubmitJobRequest{
		JobId:           fmt.Sprintf("%s-batch-%s", jobId, batch.BatchId),
		JobName:         fmt.Sprintf("%s-batch-%s", originalReq.JobName, batch.BatchId),
		Namespace:       originalReq.Namespace,
		Image:           originalReq.Image,
		ImagePullPolicy: originalReq.ImagePullPolicy,
		DataPath:        originalReq.DataPath,
		OutputPath:      fmt.Sprintf("%s/batch-%s", originalReq.OutputPath, batch.BatchId),
		TargetNode:      originalReq.TargetNode,
		Resources:       originalReq.Resources,
		Preprocessing:   originalReq.Preprocessing,
		DataLocations:   originalReq.DataLocations,
		Csd:             originalReq.Csd,
		NodeScheduling:  originalReq.NodeScheduling,
		JobConfig:       originalReq.JobConfig,
		Labels:          originalReq.Labels,
		Annotations:     originalReq.Annotations,
	}

	// Add batch-specific environment variables
	if batchReq.Labels == nil {
		batchReq.Labels = make(map[string]string)
	}
	batchReq.Labels["batch-id"] = batch.BatchId
	batchReq.Labels["parent-job"] = jobId
	batchReq.Labels["batch-item-count"] = fmt.Sprintf("%d", batch.ItemCount)

	// Add batch file paths as environment variables
	if len(batch.FilePaths) > 0 {
		if batchReq.Annotations == nil {
			batchReq.Annotations = make(map[string]string)
		}
		batchReq.Annotations["batch-file-paths"] = strings.Join(batch.FilePaths, ",")
	}

	// Add batch environment variables to annotations
	for key, value := range batch.BatchEnv {
		if batchReq.Annotations == nil {
			batchReq.Annotations = make(map[string]string)
		}
		batchReq.Annotations[fmt.Sprintf("batch-env-%s", key)] = value
	}

	return batchReq
}

// submitBatchToContainerProcessor submits a batch to the container processor
func (s *InstorageManagerServer) submitBatchToContainerProcessor(req *pb.SubmitJobRequest) error {
	// Reuse the existing submitToContainerProcessor function
	return s.submitToContainerProcessor(req)
}

// submitToContainerProcessor sends job to instorage-container-processor
func (s *InstorageManagerServer) submitToContainerProcessor(req *pb.SubmitJobRequest) error {
	// Convert gRPC request to container processor request
	containerReq := &ContainerJobRequest{
		JobID:           req.JobId,
		JobName:         req.JobName,
		Namespace:       req.Namespace,
		Image:           req.Image,
		ImagePullPolicy: req.ImagePullPolicy,
		DataPath:        req.DataPath,
		OutputPath:      req.OutputPath,
		Environment:     s.buildEnvironmentVariables(req),
		Resources:       s.convertResources(req.Resources),
		Labels:          req.Labels,
	}

	// Marshal request to JSON
	jsonData, err := json.Marshal(containerReq)
	if err != nil {
		return fmt.Errorf("failed to marshal container request: %w", err)
	}

	// [TEST MODE] Print request instead of sending HTTP request
	url := fmt.Sprintf("%s/api/v1/jobs", "s.csdEndpoint")
	
	s.logger.Info("TEST MODE: Would submit job to container processor",
		"url", url,
		"jobId", containerReq.JobID,
		"jobName", containerReq.JobName,
		"namespace", containerReq.Namespace,
		"image", containerReq.Image,
		"dataPath", containerReq.DataPath,
		"outputPath", containerReq.OutputPath,
		"environment", fmt.Sprintf("%+v", containerReq.Environment),
		"resources", fmt.Sprintf("%+v", containerReq.Resources),
		"labels", fmt.Sprintf("%+v", containerReq.Labels),
		"requestPayload", string(jsonData),
	)

	// [COMMENTED OUT] Actual HTTP request code for testing
	/*
	httpReq, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("failed to send request to container processor: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("container processor returned status %d", resp.StatusCode)
	}

	// Parse response
	var containerResp ContainerJobResponse
	if err := json.NewDecoder(resp.Body).Decode(&containerResp); err != nil {
		return fmt.Errorf("failed to decode container processor response: %w", err)
	}

	if !containerResp.Success {
		return fmt.Errorf("container processor rejected job: %s", containerResp.Message)
	}

	s.logger.Info("Job successfully submitted to container processor",
		"jobId", containerReq.JobID,
		"containerId", containerResp.ContainerID,
		"message", containerResp.Message,
	)
	*/

	return nil
}

// buildEnvironmentVariables creates environment variables from gRPC request
func (s *InstorageManagerServer) buildEnvironmentVariables(req *pb.SubmitJobRequest) map[string]string {
	env := map[string]string{
		"DATA_PATH":   req.DataPath,
		"OUTPUT_PATH": req.OutputPath,
	}

	if req.Preprocessing != nil {
		if req.Preprocessing.BatchSize > 0 {
			env["BATCH_SIZE"] = fmt.Sprintf("%d", req.Preprocessing.BatchSize)
		}
		if req.Preprocessing.MaxLength > 0 {
			env["MAX_LENGTH"] = fmt.Sprintf("%d", req.Preprocessing.MaxLength)
		}
		if req.Preprocessing.NSamples > 0 {
			env["N_SAMPLES"] = fmt.Sprintf("%d", req.Preprocessing.NSamples)
		}
		if req.Preprocessing.ParallelWorkers > 0 {
			env["PARALLEL_WORKERS"] = fmt.Sprintf("%d", req.Preprocessing.ParallelWorkers)
		}
		if req.Preprocessing.ChunkSize > 0 {
			env["CHUNK_SIZE"] = fmt.Sprintf("%d", req.Preprocessing.ChunkSize)
		}
	}

	if req.DataLocations != nil {
		if len(req.DataLocations.Locations) > 0 {
			locationsJson, _ := json.Marshal(req.DataLocations.Locations)
			env["DATA_LOCATIONS"] = string(locationsJson)
		}
		if req.DataLocations.Strategy != "" {
			env["DATA_STRATEGY"] = req.DataLocations.Strategy
		}
	}

	if req.Csd != nil && req.Csd.Enabled {
		env["CSD_ENABLED"] = "true"
		if req.Csd.DevicePath != "" {
			env["CSD_DEVICE_PATH"] = req.Csd.DevicePath
		}
	}

	return env
}

// convertResources converts gRPC resources to container resources
func (s *InstorageManagerServer) convertResources(res *pb.Resources) *ContainerResources {
	if res == nil {
		return nil
	}

	containerRes := &ContainerResources{}

	if res.Requests != nil {
		containerRes.CPURequest = res.Requests.Cpu
		containerRes.MemoryRequest = res.Requests.Memory
	}

	if res.Limits != nil {
		containerRes.CPULimit = res.Limits.Cpu
		containerRes.MemoryLimit = res.Limits.Memory
	}

	return containerRes
}
