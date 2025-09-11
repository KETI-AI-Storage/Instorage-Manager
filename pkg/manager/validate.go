package manager

import (
	"fmt"
	pb "instorage-manager/pkg/proto"
	"regexp"
	"strings"
)

// validateSubmitJobRequest validates the SubmitJobRequest comprehensively
func (s *InstorageManagerServer) validateSubmitJobRequest(req *pb.SubmitJobRequest) error {
	if req == nil {
		return fmt.Errorf("request is nil")
	}

	// Validate required fields
	if req.JobId == "" {
		return fmt.Errorf("job_id is required")
	}
	if req.JobName == "" {
		return fmt.Errorf("job_name is required")
	}
	if req.Namespace == "" {
		return fmt.Errorf("namespace is required")
	}
	if req.Image == "" {
		return fmt.Errorf("image is required")
	}

	// Validate ID format (UUID-like)
	if len(req.JobId) < 8 || len(req.JobId) > 64 {
		return fmt.Errorf("job_id must be between 8-64 characters, got: %d", len(req.JobId))
	}

	// Validate job name (Kubernetes name format)
	if !s.isValidKubernetesName(req.JobName) {
		return fmt.Errorf("invalid job_name format: %s", req.JobName)
	}

	// Validate namespace (Kubernetes name format)
	if !s.isValidKubernetesName(req.Namespace) {
		return fmt.Errorf("invalid namespace format: %s", req.Namespace)
	}

	// Validate container image
	if !s.isValidImageName(req.Image) {
		return fmt.Errorf("invalid container image format: %s", req.Image)
	}

	// Validate paths if specified
	if req.DataPath != "" && !s.isValidPath(req.DataPath) {
		return fmt.Errorf("invalid data_path: %s", req.DataPath)
	}
	if req.OutputPath != "" && !s.isValidPath(req.OutputPath) {
		return fmt.Errorf("invalid output_path: %s", req.OutputPath)
	}

	// Validate target node if specified
	if req.TargetNode != "" && !s.isValidKubernetesName(req.TargetNode) {
		return fmt.Errorf("invalid target_node format: %s", req.TargetNode)
	}

	// Validate resources if specified
	if req.Resources != nil {
		if err := s.validateResources(req.Resources); err != nil {
			return fmt.Errorf("invalid resources: %w", err)
		}
	}

	// Validate preprocessing config if specified
	if req.Preprocessing != nil {
		if err := s.validatePreprocessingConfig(req.Preprocessing); err != nil {
			return fmt.Errorf("invalid preprocessing config: %w", err)
		}
	}

	// Validate data locations if specified
	if req.DataLocations != nil {
		if err := s.validateDataLocations(req.DataLocations); err != nil {
			return fmt.Errorf("invalid data locations: %w", err)
		}
	}

	// Validate CSD config if specified
	if req.Csd != nil {
		if err := s.validateCSDConfig(req.Csd); err != nil {
			return fmt.Errorf("invalid CSD config: %w", err)
		}
	}

	// Validate node scheduling if specified
	if req.NodeScheduling != nil {
		if err := s.validateNodeScheduling(req.NodeScheduling); err != nil {
			return fmt.Errorf("invalid node scheduling: %w", err)
		}
	}

	// Validate job config if specified
	if req.JobConfig != nil {
		if err := s.validateJobConfig(req.JobConfig); err != nil {
			return fmt.Errorf("invalid job config: %w", err)
		}
	}

	return nil
}

// isValidKubernetesName validates Kubernetes resource names
func (s *InstorageManagerServer) isValidKubernetesName(name string) bool {
	if len(name) == 0 || len(name) > 63 {
		return false
	}
	// DNS-1123 subdomain format
	re := regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
	return re.MatchString(name)
}

// isValidImageName validates container image names
func (s *InstorageManagerServer) isValidImageName(image string) bool {
	if len(image) == 0 || len(image) > 255 {
		return false
	}
	// Basic image validation (registry/namespace/name:tag)
	re := regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?(/[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?)*(:([a-zA-Z0-9._-]+))?$`)
	return re.MatchString(image)
}

// isValidPath validates file/directory paths
func (s *InstorageManagerServer) isValidPath(path string) bool {
	if len(path) == 0 || len(path) > 4096 {
		return false
	}
	// Must be absolute path
	if !strings.HasPrefix(path, "/") {
		return false
	}
	// Basic path validation (no null bytes, control characters)
	for _, char := range path {
		if char < 32 || char == 127 {
			return false
		}
	}
	return true
}

// validateResources validates resource specifications
func (s *InstorageManagerServer) validateResources(res *pb.Resources) error {
	if res.Requests != nil {
		if err := s.validateResourceRequirements(res.Requests, "requests"); err != nil {
			return err
		}
	}
	if res.Limits != nil {
		if err := s.validateResourceRequirements(res.Limits, "limits"); err != nil {
			return err
		}
	}
	return nil
}

// validateResourceRequirements validates individual resource requirements
func (s *InstorageManagerServer) validateResourceRequirements(req *pb.ResourceRequirements, reqType string) error {
	if req.Cpu != "" && !s.isValidResourceQuantity(req.Cpu) {
		return fmt.Errorf("invalid CPU %s: %s", reqType, req.Cpu)
	}
	if req.Memory != "" && !s.isValidResourceQuantity(req.Memory) {
		return fmt.Errorf("invalid Memory %s: %s", reqType, req.Memory)
	}
	return nil
}

// isValidResourceQuantity validates Kubernetes resource quantity format
func (s *InstorageManagerServer) isValidResourceQuantity(quantity string) bool {
	// Basic quantity validation (e.g., "100m", "1Gi", "2")
	re := regexp.MustCompile(`^(\d+(\.\d+)?)(m|k|M|G|T|P|E|Ki|Mi|Gi|Ti|Pi|Ei)?$`)
	return re.MatchString(quantity)
}

// validatePreprocessingConfig validates preprocessing configuration
func (s *InstorageManagerServer) validatePreprocessingConfig(config *pb.PreprocessingConfig) error {
	if config.BatchSize <= 0 || config.BatchSize > 10000 {
		return fmt.Errorf("batchSize must be between 1-10000, got: %d", config.BatchSize)
	}
	if config.MaxLength <= 0 {
		return fmt.Errorf("maxLength must be greater than 0, got: %d", config.MaxLength)
	}
	if config.NSamples < 0 {
		return fmt.Errorf("nSamples cannot be negative, got: %d", config.NSamples)
	}
	if config.ParallelWorkers <= 0 || config.ParallelWorkers > 100 {
		return fmt.Errorf("parallelWorkers must be between 1-100, got: %d", config.ParallelWorkers)
	}
	if config.ChunkSize <= 0 {
		return fmt.Errorf("chunkSize must be greater than 0, got: %d", config.ChunkSize)
	}
	return nil
}

// validateDataLocations validates data locations configuration
func (s *InstorageManagerServer) validateDataLocations(config *pb.DataLocations) error {
	if len(config.Locations) == 0 {
		return fmt.Errorf("at least one data location must be specified")
	}

	validStrategies := map[string]bool{
		"round-robin": true,
		"random":      true,
		"first-fit":   true,
		"best-fit":    true,
	}

	if config.Strategy != "" && !validStrategies[config.Strategy] {
		return fmt.Errorf("invalid strategy '%s', must be one of: round-robin, random, first-fit, best-fit", config.Strategy)
	}

	for i, location := range config.Locations {
		if !s.isValidPath(location) {
			return fmt.Errorf("invalid data location at index %d: %s", i, location)
		}
	}

	return nil
}

// validateCSDConfig validates CSD configuration
func (s *InstorageManagerServer) validateCSDConfig(config *pb.CSDConfig) error {
	if config.Enabled && config.DevicePath == "" {
		return fmt.Errorf("devicePath is required when CSD is enabled")
	}

	if config.DevicePath != "" {
		if !s.isValidPath(config.DevicePath) {
			return fmt.Errorf("invalid devicePath: %s", config.DevicePath)
		}
		if !strings.HasPrefix(config.DevicePath, "/dev/") {
			return fmt.Errorf("devicePath should typically start with /dev/: %s", config.DevicePath)
		}
	}

	return nil
}

// validateNodeScheduling validates node scheduling configuration
func (s *InstorageManagerServer) validateNodeScheduling(config *pb.NodeScheduling) error {
	if config.NodeName != "" && !s.isValidKubernetesName(config.NodeName) {
		return fmt.Errorf("invalid nodeName format: %s", config.NodeName)
	}

	for key, value := range config.NodeSelector {
		if key == "" {
			return fmt.Errorf("nodeSelector key cannot be empty")
		}
		if len(key) > 63 || len(value) > 63 {
			return fmt.Errorf("nodeSelector key/value too long (max: 63 chars): %s=%s", key, value)
		}
	}

	return nil
}

// validateJobConfig validates job configuration
func (s *InstorageManagerServer) validateJobConfig(config *pb.JobConfig) error {
	if config.Parallelism < 0 || config.Parallelism > 1000 {
		return fmt.Errorf("parallelism must be between 0-1000, got: %d", config.Parallelism)
	}
	if config.Completions < 0 {
		return fmt.Errorf("completions cannot be negative, got: %d", config.Completions)
	}
	if config.BackoffLimit < 0 || config.BackoffLimit > 10 {
		return fmt.Errorf("backoffLimit must be between 0-10, got: %d", config.BackoffLimit)
	}
	if config.TtlSecondsAfterFinished < 0 {
		return fmt.Errorf("ttlSecondsAfterFinished cannot be negative, got: %d", config.TtlSecondsAfterFinished)
	}
	return nil
}

// validateBatchPlan validates the batch plan configuration
func (s *InstorageManagerServer) validateBatchPlan(plan *pb.BatchPlan) error {
	if plan == nil {
		return fmt.Errorf("batch plan is nil")
	}

	if plan.TotalBatches <= 0 {
		return fmt.Errorf("total batches must be greater than 0, got: %d", plan.TotalBatches)
	}

	if int(plan.TotalBatches) != len(plan.Batches) {
		return fmt.Errorf("total batches (%d) doesn't match actual batch count (%d)", plan.TotalBatches, len(plan.Batches))
	}

	if plan.MaxParallelJobs <= 0 || plan.MaxParallelJobs > 100 {
		return fmt.Errorf("max parallel jobs must be between 1-100, got: %d", plan.MaxParallelJobs)
	}

	// Validate strategy
	validStrategies := map[string]bool{
		"auto":         true,
		"manual":       true,
		"size-based":   true,
		"count-based":  true,
		"memory-based": true,
	}

	if plan.Strategy != "" && !validStrategies[plan.Strategy] {
		return fmt.Errorf("invalid batch strategy '%s'", plan.Strategy)
	}

	// Validate individual batches
	for i, batch := range plan.Batches {
		if err := s.validateBatch(batch, i); err != nil {
			return fmt.Errorf("invalid batch at index %d: %w", i, err)
		}
	}

	return nil
}

// validateBatch validates a single batch
func (s *InstorageManagerServer) validateBatch(batch *pb.BatchInfo, index int) error {
	if batch == nil {
		return fmt.Errorf("batch is nil")
	}

	if batch.BatchId == "" {
		return fmt.Errorf("batch ID is required")
	}

	if batch.ItemCount <= 0 {
		return fmt.Errorf("item count must be greater than 0, got: %d", batch.ItemCount)
	}

	if batch.EstimatedSizeBytes < 0 {
		return fmt.Errorf("estimated size bytes cannot be negative, got: %d", batch.EstimatedSizeBytes)
	}

	return nil
}
