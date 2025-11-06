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
	DryRun        bool      `json:"dry_run"`
	StartFile     string    `json:"start_file,omitempty"`
	StartLine     int64     `json:"start_line,omitempty"`
	BatchSize     int       `json:"batch_size,omitempty"`
	DateFrom      time.Time `json:"date_from,omitempty"` // Filter messages created after this date
	DateTo        time.Time `json:"date_to,omitempty"`   // Filter messages created before this date
	Limit         int64     `json:"limit,omitempty"`     // Maximum number of events to process
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
	var files []string
	var err error

	// List files based on source type
	if config.S3Path != "" {
		files, err = m.listS3Files(ctx, config.S3Path)
		if err != nil {
			return nil, fmt.Errorf("failed to list S3 files: %w", err)
		}
	} else if config.LocalPath != "" {
		files, err = m.listLocalFiles(config.LocalPath)
		if err != nil {
			return nil, fmt.Errorf("failed to list local files: %w", err)
		}
	}

	// Filter files by date range if specified
	if !config.DateFrom.IsZero() || !config.DateTo.IsZero() {
		files, err = m.filterFilesByDateRange(ctx, files, config.DateFrom, config.DateTo)
		if err != nil {
			return nil, fmt.Errorf("failed to filter files by date range: %w", err)
		}
	}

	// Get file sizes
	fileItems := make([]FileItem, 0, len(files))
	for _, filePath := range files {
		size, err := m.getFileSize(ctx, filePath)
		if err != nil {
			m.Warnf("Failed to get size for file %s: %v", filePath, err)
			size = 0
		}
		fileItems = append(fileItems, FileItem{
			Path: filePath,
			Size: size,
		})
	}

	return fileItems, nil
}

// getFileSize returns the size of a file
func (m *ReprocessingJobManager) getFileSize(ctx context.Context, filePath string) (int64, error) {
	if strings.HasPrefix(filePath, "s3://") {
		// Parse S3 path
		parts := strings.SplitN(strings.TrimPrefix(filePath, "s3://"), "/", 2)
		if len(parts) != 2 {
			return 0, fmt.Errorf("invalid S3 path: %s", filePath)
		}

		bucket := parts[0]
		key := parts[1]

		// Get object metadata
		headObj, err := m.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return 0, err
		}

		return *headObj.ContentLength, nil
	} else {
		// Local file
		info, err := os.Stat(filePath)
		if err != nil {
			return 0, err
		}
		return info.Size(), nil
	}
}

// GetJob returns a job by ID
func (m *ReprocessingJobManager) GetJob(id string) (*ReprocessingJob, error) {
	return GetReprocessingJob(m.dbpool, id)
}

// GetJobWorkers returns worker status for a job
func (m *ReprocessingJobManager) GetJobWorkers(id string) ([]map[string]interface{}, error) {
	return GetAllWorkerStatuses(m.dbpool, id)
}

// ListJobs returns all jobs
func (m *ReprocessingJobManager) ListJobs() []*ReprocessingJob {
	jobs, err := ListReprocessingJobs(m.dbpool)
	if err != nil {
		m.Errorf("Failed to list jobs from database: %v", err)
		return []*ReprocessingJob{}
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
func (m *ReprocessingJobManager) listS3Files(ctx context.Context, s3Path string) ([]string, error) {
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

	var files []string
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
				files = append(files, fmt.Sprintf("s3://%s/%s", bucket, key))
			}
		}
	}
	sort.Strings(files)

	return files, nil
}

// listLocalFiles lists all NDJSON files in the local path recursively
func (m *ReprocessingJobManager) listLocalFiles(localPath string) ([]string, error) {
	var files []string

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
			files = append(files, path)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Sort files for consistent processing order
	sort.Strings(files)

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
func (m *ReprocessingJobManager) filterFilesByDateRange(ctx context.Context, files []string, dateFrom, dateTo time.Time) ([]string, error) {
	var filteredFiles []string

	for _, file := range files {
		// Parse timestamp from filename (file creation time)
		createdTime, err := parseFilenameTimestamp(file)
		if err != nil {
			// If we can't parse the filename, skip this file
			m.Warnf("Skipping file with unparseable filename: %s: %v", file, err)
			continue
		}

		// Get file modification time
		var modTime time.Time
		if strings.HasPrefix(file, "s3://") {
			// For S3 files, get the object metadata
			parts := strings.SplitN(strings.TrimPrefix(file, "s3://"), "/", 2)
			if len(parts) != 2 {
				continue
			}

			bucket := parts[0]
			key := parts[1]

			headObj, err := m.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			if err != nil {
				m.Warnf("Failed to get metadata for S3 file %s: %v", file, err)
				continue
			}

			modTime = *headObj.LastModified
		} else {
			// For local files, get file info
			info, err := os.Stat(file)
			if err != nil {
				m.Warnf("Failed to get file info for %s: %v", file, err)
				continue
			}
			modTime = info.ModTime()
		}

		// Skip if file was created day later than the end of requested range
		// batches that was created after end of the period may still contain events from the period
		if !dateTo.IsZero() && createdTime.Add(time.Hour*-24).After(dateTo) {
			continue
		}

		// Skip if file was last modified before the start of requested range
		if !dateFrom.IsZero() && modTime.Before(dateFrom) {
			continue
		}

		filteredFiles = append(filteredFiles, file)
	}

	return filteredFiles, nil
}
