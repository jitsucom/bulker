package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/uuid"
)

// Compile regex once at package level for performance
var timestampRegex = regexp.MustCompile(`(\d{4}_\d{2}_\d{2}T\d{2}_\d{2}_\d{2})`)

// ReprocessingJobStatus represents the status of a reprocessing job
type ReprocessingJobStatus string

const (
	JobStatusPending   ReprocessingJobStatus = "pending"
	JobStatusRunning   ReprocessingJobStatus = "running"
	JobStatusCompleted ReprocessingJobStatus = "completed"
	JobStatusFailed    ReprocessingJobStatus = "failed"
	JobStatusCancelled ReprocessingJobStatus = "cancelled"
)

// ReprocessingJobConfig contains configuration for starting a reprocessing job
type ReprocessingJobConfig struct {
	S3Path        string    `json:"s3_path,omitempty"`    // S3 path (s3://bucket/prefix)
	LocalPath     string    `json:"local_path,omitempty"` // Local filesystem path
	StreamIds     []string  `json:"stream_ids,omitempty"`
	ConnectionIds []string  `json:"connection_ids,omitempty"`
	Files         []string  `json:"files,omitempty"` // Optional list of specific files to process
	DryRun        bool      `json:"dry_run"`
	StartFile     string    `json:"start_file,omitempty"`
	StartLine     int64     `json:"start_line,omitempty"`
	BatchSize     int       `json:"batch_size,omitempty"`
	RetryAttempts int       `json:"retry_attempts,omitempty"` // Number of retry attempts for file processing (default: 3)
	DateFrom      time.Time `json:"date_from,omitempty"`      // Filter messages created after this date
	DateTo        time.Time `json:"date_to,omitempty"`        // Filter messages created before this date
	Limit         int64     `json:"limit,omitempty"`          // Maximum number of events to process
}

// ReprocessingJob represents a failover reprocessing job
type ReprocessingJob struct {
	ID               string                `json:"id"`
	Config           ReprocessingJobConfig `json:"config"`
	Status           ReprocessingJobStatus `json:"status"`
	CreatedAt        time.Time             `json:"created_at"`
	StartedAt        *time.Time            `json:"started_at,omitempty"`
	CompletedAt      *time.Time            `json:"completed_at,omitempty"`
	CurrentFile      string                `json:"current_file,omitempty"`
	CurrentLine      int64                 `json:"current_line"`
	CurrentTimestamp string                `json:"current_timestamp,omitempty"` // Based on IngestMessage.MessageCreated
	TotalFiles       int                   `json:"total_files"`
	ProcessedFiles   int                   `json:"processed_files"`
	TotalLines       int64                 `json:"total_lines"`
	SuccessCount     int64                 `json:"success_count"`
	ErrorCount       int64                 `json:"error_count"`
	SkippedCount     int64                 `json:"skipped_count"`
	ProcessedBytes   int64                 `json:"processed_bytes"`
	LastError        string                `json:"last_error,omitempty"`
	K8sJobName       string                `json:"k8s_job_name,omitempty"`
	TotalWorkers     int                   `json:"total_workers"`
}

// ReprocessingJobManager manages failover reprocessing jobs
type ReprocessingJobManager struct {
	appbase.Service
	s3Client  *s3.Client
	config    *Config
	dbpool    *pgxpool.Pool
	k8sClient *K8sJobClient
}

// NewReprocessingJobManager creates a new reprocessing job manager
func NewReprocessingJobManager(config *Config, dbpool *pgxpool.Pool, k8sClient *K8sJobClient) (*ReprocessingJobManager, error) {
	base := appbase.NewServiceBase("reprocessing-job-manager")

	// Database and K8s client are required
	if dbpool == nil {
		return nil, fmt.Errorf("database connection is required for reprocessing")
	}
	if k8sClient == nil {
		return nil, fmt.Errorf("kubernetes client is required for reprocessing")
	}

	// Initialize AWS config and S3 client
	awsConfig, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	s3Client := s3.NewFromConfig(awsConfig)

	return &ReprocessingJobManager{
		Service:   base,
		s3Client:  s3Client,
		config:    config,
		dbpool:    dbpool,
		k8sClient: k8sClient,
	}, nil
}

// StartJob starts a new reprocessing job
func (m *ReprocessingJobManager) StartJob(config ReprocessingJobConfig) (*ReprocessingJob, error) {
	// Validate that either S3 or local path is provided
	if config.S3Path == "" && config.LocalPath == "" {
		return nil, fmt.Errorf("either s3_path or local_path must be provided")
	}
	if config.S3Path != "" && config.LocalPath != "" {
		return nil, fmt.Errorf("only one of s3_path or local_path can be provided")
	}

	// Set defaults
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}

	// Create Kubernetes Job for reprocessing
	ctx := context.Background()

	// List and prepare files with sizes
	fileItems, err := m.prepareFileList(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare file list: %w", err)
	}

	if len(fileItems) == 0 {
		return nil, fmt.Errorf("no files found to process")
	}

	// Determine number of workers (default: 1 worker per 10 files, max 50 workers)
	workerCount := (len(fileItems) + 9) / 10
	if workerCount > 50 {
		workerCount = 50
	}
	if workerCount < 1 {
		workerCount = 1
	}

	// Create job record
	job := &ReprocessingJob{
		ID:           uuid.New(),
		Config:       config,
		Status:       JobStatusPending,
		CreatedAt:    time.Now(),
		TotalFiles:   len(fileItems),
		TotalWorkers: workerCount,
	}

	// Insert job into database
	err = InsertReprocessingJob(m.dbpool, job)
	if err != nil {
		return nil, fmt.Errorf("failed to insert job into database: %w", err)
	}

	// Distribute files across workers
	filesPerWorker := make([]int, workerCount)
	for i := range fileItems {
		workerIndex := i % workerCount
		filesPerWorker[workerIndex]++
	}

	// Initialize worker records
	err = InitializeWorkers(m.dbpool, job.ID, workerCount, filesPerWorker)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize workers: %w", err)
	}

	// Create K8s Indexed Job
	k8sJobName, err := m.k8sClient.CreateReprocessingJob(ctx, job.ID, fileItems, config, workerCount)
	if err != nil {
		_ = UpdateReprocessingJobStatus(m.dbpool, job.ID, JobStatusFailed, nil, nil, err.Error())
		return nil, fmt.Errorf("failed to create k8s job: %w", err)
	}

	job.K8sJobName = k8sJobName
	job.Status = JobStatusRunning
	now := time.Now()
	job.StartedAt = &now

	// Update job with K8s job name and status
	err = UpdateReprocessingJobK8sName(m.dbpool, job.ID, k8sJobName)
	if err != nil {
		m.Warnf("Failed to update k8s job name: %v", err)
	}

	err = UpdateReprocessingJobStatus(m.dbpool, job.ID, JobStatusRunning, &now, nil, "")
	if err != nil {
		m.Warnf("Failed to update job status: %v", err)
	}

	m.Infof("Created K8s reprocessing job %s with %d workers processing %d files", job.ID, workerCount, len(fileItems))

	return job, nil
}

// prepareFileList lists files and gets their sizes
func (m *ReprocessingJobManager) prepareFileList(ctx context.Context, config ReprocessingJobConfig) ([]FileItem, error) {
	var fileItems []FileItem
	var err error

	// List files based on source type (with sizes)
	if config.S3Path != "" {
		fileItems, err = m.listS3Files(ctx, config.S3Path)
		if err != nil {
			return nil, fmt.Errorf("failed to list S3 files: %w", err)
		}
	} else if config.LocalPath != "" {
		fileItems, err = m.listLocalFiles(config.LocalPath)
		if err != nil {
			return nil, fmt.Errorf("failed to list local files: %w", err)
		}
	}

	// Filter files by date range if specified
	if !config.DateFrom.IsZero() || !config.DateTo.IsZero() {
		fileItems, err = m.filterFilesByDateRange(ctx, fileItems, config.DateFrom, config.DateTo)
		if err != nil {
			return nil, fmt.Errorf("failed to filter files by date range: %w", err)
		}
	}

	// Filter files by specific file list if provided
	if len(config.Files) > 0 {
		fileItems = m.filterFilesByList(fileItems, config.Files)
	}

	return fileItems, nil
}

// GetJob returns a job by ID with enriched K8s status
func (m *ReprocessingJobManager) GetJob(id string) (*ReprocessingJob, error) {
	job, err := GetReprocessingJob(m.dbpool, id)
	if err != nil {
		return nil, err
	}

	// Enrich with K8s job status if available
	if job.Status == JobStatusRunning && job.K8sJobName != "" && m.k8sClient != nil {
		if err := m.enrichJobWithK8sStatus(job); err != nil {
			// Log warning but don't fail the request
			m.Warnf("Failed to get K8s status for job %s: %v", id, err)
		}
	}

	return job, nil
}

// enrichJobWithK8sStatus adds Kubernetes job status information to the job
func (m *ReprocessingJobManager) enrichJobWithK8sStatus(job *ReprocessingJob) error {
	ctx := context.Background()
	k8sStatus, err := m.k8sClient.GetJobStatus(ctx, job.K8sJobName)
	if err != nil {
		// If K8s Job not found, check worker statuses to determine completion
		if strings.Contains(err.Error(), "job not found") {
			m.Infof("K8s Job %s not found (likely cleaned up), checking worker statuses", job.K8sJobName)
			return m.checkCompletionFromWorkers(job)
		}
		return err
	}

	// K8s Job has CompletionTime when all pods are done (succeeded or failed)
	k8sJobCompleted := k8sStatus.CompletionTime != nil || (k8sStatus.Failed == int32(job.TotalWorkers) && k8sStatus.Active == 0)

	if !k8sJobCompleted {
		return nil
	}
	// Update job status based on K8s job status if our status is stale
	// Only update if job is in running state and K8s reports completion
	hasFailures := k8sStatus.Failed > 0 && k8sStatus.Active == 0
	allSucceeded := k8sStatus.Succeeded == int32(job.TotalWorkers) && k8sStatus.Active == 0

	if hasFailures {
		// Job has failed pods and no active pods - mark as failed
		now := time.Now()
		if k8sStatus.CompletionTime != nil {
			now = k8sStatus.CompletionTime.Time
		}
		job.CompletedAt = &now
		job.Status = JobStatusFailed
		err := UpdateReprocessingJobStatus(m.dbpool, job.ID, JobStatusFailed, nil, &now, "K8s job has failed pods")
		if err != nil {
			m.Warnf("Failed to update job status to failed: %v", err)
		} else {
			m.Infof("Job %s marked as failed based on K8s status (failed=%d, active=%d)", job.ID, k8sStatus.Failed, k8sStatus.Active)
		}
	} else if allSucceeded || k8sJobCompleted {
		// All workers succeeded OR K8s job is complete - mark as completed
		now := time.Now()
		if k8sStatus.CompletionTime != nil {
			now = k8sStatus.CompletionTime.Time
		}
		job.CompletedAt = &now
		job.Status = JobStatusCompleted
		err := UpdateReprocessingJobStatus(m.dbpool, job.ID, JobStatusCompleted, nil, &now, "")
		if err != nil {
			m.Warnf("Failed to update job status to completed: %v", err)
		} else {
			m.Infof("Job %s marked as completed based on K8s status (succeeded=%d, active=%d, completion_time=%v)",
				job.ID, k8sStatus.Succeeded, k8sStatus.Active, k8sStatus.CompletionTime != nil)
		}
	}

	return nil
}

// checkCompletionFromWorkers checks worker statuses to determine if job is complete
func (m *ReprocessingJobManager) checkCompletionFromWorkers(job *ReprocessingJob) error {
	workers, err := GetAllWorkerStatuses(m.dbpool, job.ID)
	if err != nil {
		return fmt.Errorf("failed to get worker statuses: %w", err)
	}

	if len(workers) == 0 {
		// No workers found - cannot determine completion
		return nil
	}

	completedCount := 0
	failedCount := 0

	for _, worker := range workers {
		status, ok := worker["status"].(string)
		if !ok {
			continue
		}
		if status == "completed" {
			completedCount++
		} else if status == "failed" {
			failedCount++
		}
	}

	m.Infof("Worker status for job %s: completed=%d, failed=%d, total=%d", job.ID, completedCount, failedCount, len(workers))

	// If all workers are completed or failed, mark the job accordingly
	if completedCount+failedCount == job.TotalWorkers {
		now := time.Now()
		job.CompletedAt = &now

		if failedCount > 0 {
			job.Status = JobStatusFailed
			err := UpdateReprocessingJobStatus(m.dbpool, job.ID, JobStatusFailed, nil, &now, fmt.Sprintf("%d workers failed", failedCount))
			if err != nil {
				m.Warnf("Failed to update job status to failed: %v", err)
			} else {
				m.Infof("Job %s marked as failed based on worker statuses (failed=%d)", job.ID, failedCount)
			}
		} else {
			job.Status = JobStatusCompleted
			err := UpdateReprocessingJobStatus(m.dbpool, job.ID, JobStatusCompleted, nil, &now, "")
			if err != nil {
				m.Warnf("Failed to update job status to completed: %v", err)
			} else {
				m.Infof("Job %s marked as completed based on worker statuses (completed=%d)", job.ID, completedCount)
			}
		}
	}

	return nil
}

// GetJobWorkers returns worker status for a job
func (m *ReprocessingJobManager) GetJobWorkers(id string) ([]map[string]interface{}, error) {
	return GetAllWorkerStatuses(m.dbpool, id)
}

// ListJobs returns all jobs with enriched K8s status
func (m *ReprocessingJobManager) ListJobs() []*ReprocessingJob {
	jobs, err := ListReprocessingJobs(m.dbpool)
	if err != nil {
		m.Errorf("Failed to list jobs from database: %v", err)
		return []*ReprocessingJob{}
	}

	// Enrich running jobs with K8s status to detect completion
	if m.k8sClient != nil {
		for _, job := range jobs {
			if job.Status == JobStatusRunning && job.K8sJobName != "" {
				if err := m.enrichJobWithK8sStatus(job); err != nil {
					// Log but don't fail
					m.Warnf("Failed to get K8s status for job %s: %v", job.ID, err)
				}
			}
		}
	}

	return jobs
}

// CancelJob cancels a job
func (m *ReprocessingJobManager) CancelJob(id string) error {
	job, err := m.GetJob(id)
	if err != nil {
		return err
	}

	if job.Status == JobStatusCompleted || job.Status == JobStatusCancelled {
		return fmt.Errorf("job %s is already finished", id)
	}

	// Delete the K8s job
	ctx := context.Background()
	if job.K8sJobName == "" {
		m.Warnf("Job %s has no K8s job name, cannot delete K8s resources", id)
	} else {
		err = m.k8sClient.DeleteJob(ctx, job.K8sJobName, job.ID)
		if err != nil {
			m.Warnf("Failed to delete K8s job: %v", err)
		}
	}

	// Update status in database
	now := time.Now()
	err = UpdateReprocessingJobStatus(m.dbpool, id, JobStatusCancelled, nil, &now, "")
	if err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}

	return nil
}

// Close shuts down the reprocessing job manager
func (m *ReprocessingJobManager) Close() error {
	m.Infof("Shutting down reprocessing job manager")
	return nil
}

// listS3Files lists all NDJSON files in the S3 path
func (m *ReprocessingJobManager) listS3Files(ctx context.Context, s3Path string) ([]FileItem, error) {
	// Parse S3 path (s3://bucket/prefix)
	parts := strings.SplitN(strings.TrimPrefix(s3Path, "s3://"), "/", 2)
	if len(parts) < 1 {
		return nil, fmt.Errorf("invalid S3 path: %s", s3Path)
	}

	bucket := parts[0]
	prefix := ""
	if len(parts) > 1 {
		prefix = parts[1]
	}

	var files []FileItem
	paginator := s3.NewListObjectsV2Paginator(m.s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, obj := range page.Contents {
			key := *obj.Key
			if strings.HasSuffix(key, ".ndjson") || strings.HasSuffix(key, ".ndjson.gz") {
				files = append(files, FileItem{
					Path:         fmt.Sprintf("s3://%s/%s", bucket, key),
					Size:         *obj.Size,
					LastModified: *obj.LastModified,
				})
			}
		}
	}

	// Sort by path for consistent processing order
	sort.Slice(files, func(i, j int) bool {
		return files[i].Path < files[j].Path
	})

	return files, nil
}

// listLocalFiles lists all NDJSON files in the local path recursively
func (m *ReprocessingJobManager) listLocalFiles(localPath string) ([]FileItem, error) {
	var files []FileItem

	err := filepath.Walk(localPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Only include ndjson and ndjson.gz files
		if strings.HasSuffix(path, ".ndjson") || strings.HasSuffix(path, ".ndjson.gz") {
			files = append(files, FileItem{
				Path:         path,
				Size:         info.Size(),
				LastModified: info.ModTime(),
			})
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Sort files by path for consistent processing order
	sort.Slice(files, func(i, j int) bool {
		return files[i].Path < files[j].Path
	})

	return files, nil
}

// parseFilenameTimestamp extracts timestamp from failover log filename using regex
// Looks for pattern: YYYY_MM_DDTHH_MM_SS anywhere in the filename
func parseFilenameTimestamp(filename string) (time.Time, error) {
	base := filepath.Base(filename)

	// Find timestamp pattern in filename
	matches := timestampRegex.FindStringSubmatch(base)
	if matches == nil || len(matches) != 2 {
		return time.Time{}, fmt.Errorf("no timestamp pattern (YYYY_MM_DDTHH_MM_SS) found in filename: %s", filename)
	}

	// Parse the timestamp
	t, err := time.Parse("2006_01_02T15_04_05", matches[1])
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse timestamp from filename %s: %w", filename, err)
	}

	return t, nil
}

// filterFilesByDateRange filters files based on date range overlap
// A file is included if its time span overlaps with the requested date range:
// - File creation time (from filename) <= dateTo
// - File modification time >= dateFrom
func (m *ReprocessingJobManager) filterFilesByDateRange(ctx context.Context, files []FileItem, dateFrom, dateTo time.Time) ([]FileItem, error) {
	var filteredFiles []FileItem

	for _, fileItem := range files {
		// Parse timestamp from filename (file creation time)
		createdTime, err := parseFilenameTimestamp(fileItem.Path)
		if err != nil {
			// If we can't parse the filename, skip this file
			m.Warnf("Skipping file with unparseable filename: %s: %v", fileItem.Path, err)
			continue
		}

		// Skip if file was created day later than the end of requested range
		// batches that was created after end of the period may still contain events from the period
		if !dateTo.IsZero() && createdTime.Add(time.Hour*-24).After(dateTo) {
			continue
		}

		// Skip if file was last modified before the start of requested range
		if !dateFrom.IsZero() && fileItem.LastModified.Before(dateFrom) {
			continue
		}

		filteredFiles = append(filteredFiles, fileItem)
	}

	return filteredFiles, nil
}

// filterFilesByList filters files to only include those in the provided list
// Matches either the full path or just the filename
func (m *ReprocessingJobManager) filterFilesByList(files []FileItem, fileList []string) []FileItem {
	// Build a map for fast lookup (both full paths and basenames)
	fileMap := make(map[string]bool)
	for _, f := range fileList {
		f = strings.TrimSpace(f)
		if f != "" {
			fileMap[f] = true
			// Also add just the basename for convenience
			fileMap[filepath.Base(f)] = true
		}
	}

	var filteredFiles []FileItem
	for _, fileItem := range files {
		// Check if the full path matches
		if fileMap[fileItem.Path] {
			filteredFiles = append(filteredFiles, fileItem)
			continue
		}
		// Check if just the filename matches
		if fileMap[filepath.Base(fileItem.Path)] {
			filteredFiles = append(filteredFiles, fileItem)
			continue
		}
	}

	m.Infof("Filtered %d files to %d files based on provided file list", len(files), len(filteredFiles))
	return filteredFiles
}
