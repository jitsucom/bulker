package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jitsucom/bulker/jitsubase/jsonorder"
	"github.com/jitsucom/bulker/jitsubase/utils"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jitsucom/bulker/jitsubase/appbase"
	"github.com/jitsucom/bulker/jitsubase/safego"
	"github.com/jitsucom/bulker/jitsubase/uuid"
	"github.com/jitsucom/bulker/kafkabase"
)

// Compile regex once at package level for performance
var timestampRegex = regexp.MustCompile(`(\d{4}_\d{2}_\d{2}T\d{2}_\d{2}_\d{2})`)

// ReprocessingJobStatus represents the status of a reprocessing job
type ReprocessingJobStatus string

const (
	JobStatusPending   ReprocessingJobStatus = "pending"
	JobStatusRunning   ReprocessingJobStatus = "running"
	JobStatusPaused    ReprocessingJobStatus = "paused"
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
	LastError        string                `json:"last_error,omitempty"`

	// Internal fields
	ctx        context.Context
	cancel     context.CancelFunc
	pauseChan  chan struct{}
	resumeChan chan struct{}
	mu         sync.RWMutex
}

// ReprocessingJobManager manages failover reprocessing jobs
type ReprocessingJobManager struct {
	appbase.Service
	jobs       map[string]*ReprocessingJob
	mu         sync.RWMutex
	producer   *kafkabase.Producer
	repository appbase.Repository[Streams]
	s3Client   *s3.Client
	config     *Config
}

// NewReprocessingJobManager creates a new reprocessing job manager
func NewReprocessingJobManager(producer *kafkabase.Producer, repository appbase.Repository[Streams], config *Config) (*ReprocessingJobManager, error) {
	base := appbase.NewServiceBase("reprocessing-job-manager")

	// Initialize AWS config and S3 client
	awsConfig, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	s3Client := s3.NewFromConfig(awsConfig)

	return &ReprocessingJobManager{
		Service:    base,
		jobs:       make(map[string]*ReprocessingJob),
		producer:   producer,
		repository: repository,
		s3Client:   s3Client,
		config:     config,
	}, nil
}

// StartJob starts a new reprocessing job
func (m *ReprocessingJobManager) StartJob(config ReprocessingJobConfig) (*ReprocessingJob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

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

	// Create new job
	ctx, cancel := context.WithCancel(context.Background())
	job := &ReprocessingJob{
		ID:         uuid.New(),
		Config:     config,
		Status:     JobStatusPending,
		CreatedAt:  time.Now(),
		ctx:        ctx,
		cancel:     cancel,
		pauseChan:  make(chan struct{}, 1),
		resumeChan: make(chan struct{}, 1),
	}

	m.jobs[job.ID] = job

	// Start job processing in background
	safego.RunWithRestart(func() {
		m.processJob(job)
	})

	return job, nil
}

// GetJob returns a job by ID
func (m *ReprocessingJobManager) GetJob(id string) (*ReprocessingJob, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	job, exists := m.jobs[id]
	if !exists {
		return nil, fmt.Errorf("job not found: %s", id)
	}

	return job, nil
}

// ListJobs returns all jobs
func (m *ReprocessingJobManager) ListJobs() []*ReprocessingJob {
	m.mu.RLock()
	defer m.mu.RUnlock()

	jobs := make([]*ReprocessingJob, 0, len(m.jobs))
	for _, job := range m.jobs {
		jobs = append(jobs, job)
	}

	return jobs
}

// PauseJob pauses a running job
func (m *ReprocessingJobManager) PauseJob(id string) error {
	job, err := m.GetJob(id)
	if err != nil {
		return err
	}

	job.mu.Lock()
	defer job.mu.Unlock()

	if job.Status != JobStatusRunning {
		return fmt.Errorf("job %s is not running", id)
	}

	select {
	case job.pauseChan <- struct{}{}:
		job.Status = JobStatusPaused
	default:
	}

	return nil
}

// ResumeJob resumes a paused job
func (m *ReprocessingJobManager) ResumeJob(id string) error {
	job, err := m.GetJob(id)
	if err != nil {
		return err
	}

	job.mu.Lock()
	defer job.mu.Unlock()

	if job.Status != JobStatusPaused {
		return fmt.Errorf("job %s is not paused", id)
	}

	select {
	case job.resumeChan <- struct{}{}:
		job.Status = JobStatusRunning
	default:
	}

	return nil
}

// CancelJob cancels a job
func (m *ReprocessingJobManager) CancelJob(id string) error {
	job, err := m.GetJob(id)
	if err != nil {
		return err
	}

	job.mu.Lock()
	defer job.mu.Unlock()

	if job.Status == JobStatusCompleted || job.Status == JobStatusCancelled {
		return fmt.Errorf("job %s is already finished", id)
	}

	job.cancel()
	job.Status = JobStatusCancelled
	now := time.Now()
	job.CompletedAt = &now

	return nil
}

// processJob processes a reprocessing job
func (m *ReprocessingJobManager) processJob(job *ReprocessingJob) {
	job.mu.Lock()
	job.Status = JobStatusRunning
	now := time.Now()
	job.StartedAt = &now
	job.mu.Unlock()

	defer func() {
		job.mu.Lock()
		if job.Status == JobStatusRunning {
			job.Status = JobStatusCompleted
		}
		now := time.Now()
		job.CompletedAt = &now
		job.mu.Unlock()
	}()

	// List files based on source type
	var files []string
	var err error
	if job.Config.S3Path != "" {
		files, err = m.listS3Files(job.ctx, job.Config.S3Path)
		if err != nil {
			m.handleJobError(job, fmt.Errorf("failed to list S3 files: %w", err))
			return
		}
	} else if job.Config.LocalPath != "" {
		files, err = m.listLocalFiles(job.Config.LocalPath)
		if err != nil {
			m.handleJobError(job, fmt.Errorf("failed to list local files: %w", err))
			return
		}
	}

	// Filter files by date range if specified
	if !job.Config.DateFrom.IsZero() || !job.Config.DateTo.IsZero() {
		files, err = m.filterFilesByDateRange(job.ctx, files, job.Config.DateFrom, job.Config.DateTo)
		if err != nil {
			m.handleJobError(job, fmt.Errorf("failed to filter files by date range: %w", err))
			return
		}
	}

	job.mu.Lock()
	job.TotalFiles = len(files)
	job.mu.Unlock()

	// Find starting position
	startIdx := 0
	if job.Config.StartFile != "" {
		for i, file := range files {
			if job.Config.StartFile == filepath.Base(file) {
				startIdx = i
				break
			}
		}
	}

	// Process files
	for i := startIdx; i < len(files); i++ {
		// Check if we've reached the limit
		if job.Config.Limit > 0 && atomic.LoadInt64(&job.SuccessCount) >= job.Config.Limit {
			m.Infof("Reached event limit of %d, stopping processing", job.Config.Limit)
			break
		}

		select {
		case <-job.ctx.Done():
			return
		case <-job.pauseChan:
			// Wait for resume
			select {
			case <-job.resumeChan:
				// Continue processing
			case <-job.ctx.Done():
				return
			}
		default:
		}

		file := files[i]
		if err := m.processFile(job, file); err != nil {
			m.handleJobError(job, fmt.Errorf("failed to process file %s: %w", file, err))
			if !m.shouldContinueOnError() {
				return
			}
		}

		job.mu.Lock()
		job.ProcessedFiles++
		job.mu.Unlock()
	}
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

// processFile processes a single file
func (m *ReprocessingJobManager) processFile(job *ReprocessingJob, filePath string) error {
	job.mu.Lock()
	job.CurrentFile = filePath
	job.CurrentLine = 0
	job.mu.Unlock()

	// Get file path to read from
	var fileToRead string
	var tempFile string

	if strings.HasPrefix(filePath, "s3://") {
		// Download file from S3
		var err error
		tempFile, err = m.downloadS3File(job.ctx, filePath)
		if err != nil {
			return err
		}
		defer os.Remove(tempFile)
		fileToRead = tempFile
	} else {
		// Use local file directly
		fileToRead = filePath
	}

	// Open file
	file, err := os.Open(fileToRead)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create reader (handle gzip if needed)
	var reader io.Reader = file
	if strings.HasSuffix(filePath, ".gz") {
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			return err
		}
		defer gzReader.Close()
		reader = gzReader
	}

	// Skip to start line if specified
	scanner := bufio.NewScanner(reader)
	lineNum := int64(0)

	if job.Config.StartFile == filepath.Base(filePath) && job.Config.StartLine > 0 {
		for lineNum < job.Config.StartLine && scanner.Scan() {
			lineNum++
		}
	}

	// Process lines
	batch := make([]*IngestMessage, 0, job.Config.BatchSize)
	for scanner.Scan() {
		select {
		case <-job.ctx.Done():
			return nil
		case <-job.pauseChan:
			// Wait for resume
			select {
			case <-job.resumeChan:
				// Continue processing
			case <-job.ctx.Done():
				return nil
			}
		default:
		}
		atomic.AddInt64(&job.TotalLines, 1)
		lineNum++

		// Check if we've reached the limit
		if job.Config.Limit > 0 && atomic.LoadInt64(&job.SuccessCount) >= job.Config.Limit {
			m.Infof("Reached event limit of %d, stopping processing", job.Config.Limit)
			break
		}

		line := scanner.Bytes()
		// Parse message as IngestMessage
		var ingestMsg IngestMessage
		if err := jsonorder.Unmarshal(line, &ingestMsg); err != nil {
			m.Errorf("Failed to parse line %d in file %s: %v", lineNum, filePath, err)
			atomic.AddInt64(&job.ErrorCount, 1)
			continue
		}
		// Parse and filter message
		shouldProcess := m.filterMessage(job, &ingestMsg)
		if !shouldProcess {
			atomic.AddInt64(&job.SkippedCount, 1)
			continue
		}

		batch = append(batch, &ingestMsg)

		if len(batch) >= job.Config.BatchSize {
			if err := m.processBatch(job, batch); err != nil {
				m.Errorf("Failed to process batch: %v", err)
				atomic.AddInt64(&job.ErrorCount, int64(len(batch)))
			}
			batch = batch[:0]
		}

		job.mu.Lock()
		job.CurrentLine = lineNum
		job.CurrentTimestamp = ingestMsg.MessageCreated // Update current timestamp based on message
		job.mu.Unlock()
	}

	// Process remaining batch
	if len(batch) > 0 {
		if err := m.processBatch(job, batch); err != nil {
			m.Errorf("Failed to process final batch: %v", err)
			atomic.AddInt64(&job.ErrorCount, int64(len(batch)))
		}
	}

	return scanner.Err()
}

// filterMessage filters a message based on job configuration
func (m *ReprocessingJobManager) filterMessage(job *ReprocessingJob, ingestMsg *IngestMessage) bool {
	// Filter by date range if specified
	if !job.Config.DateFrom.IsZero() || !job.Config.DateTo.IsZero() {
		// Parse MessageCreated timestamp
		messageTime, err := time.Parse(time.RFC3339Nano, ingestMsg.MessageCreated)
		if err != nil {
			// If we can't parse the timestamp, skip date filtering for this message
			m.Warnf("Failed to parse MessageCreated timestamp %s: %v", ingestMsg.MessageCreated, err)
		} else {
			if !job.Config.DateFrom.IsZero() && messageTime.Before(job.Config.DateFrom) {
				return false
			}
			if !job.Config.DateTo.IsZero() && messageTime.After(job.Config.DateTo) {
				return false
			}
		}
	}

	// Filter by streamIds if specified
	if len(job.Config.StreamIds) > 0 {
		return utils.ArrayContains(job.Config.StreamIds, ingestMsg.Origin.SourceId) ||
			utils.ArrayContains(job.Config.StreamIds, ingestMsg.Origin.Slug)
	}

	return true
}

// processBatch processes a batch of messages
func (m *ReprocessingJobManager) processBatch(job *ReprocessingJob, batch []*IngestMessage) error {
	if job.Config.DryRun {
		// In dry run mode, just count the messages
		atomic.AddInt64(&job.SuccessCount, int64(len(batch)))
		return nil
	}
	streams := m.repository.GetData()
	// Send messages to Kafka
	for _, message := range batch {
		// Get connection IDs
		connectionIds := job.Config.ConnectionIds
		if len(connectionIds) == 0 {
			streamId := utils.DefaultString(message.Origin.SourceId, message.Origin.Slug)
			// Get from repository
			stream := streams.GetStreamByPlainKeyOrId(streamId)
			if stream == nil {
				m.Errorf("Stream not found for source id: %s", streamId)
				atomic.AddInt64(&job.ErrorCount, 1)
				continue
			} else {
				if len(stream.AsynchronousDestinations) == 0 {
					m.Errorf("No destinations found for stream: %s", streamId)
					atomic.AddInt64(&job.ErrorCount, 1)
					continue
				}
				for _, dest := range stream.AsynchronousDestinations {
					connectionIds = append(connectionIds, dest.ConnectionId)
				}
			}
		}

		// Send to Kafka with connection_ids header
		headers := map[string]string{
			ConnectionIdsHeader: strings.Join(connectionIds, ","),
		}
		ingestMessageBytes, err1 := jsonorder.Marshal(message)
		if err1 != nil {
			m.Errorf("Failed to marshal message: %v", err1)
			atomic.AddInt64(&job.ErrorCount, 1)
			continue
		}

		if err := m.producer.ProduceAsync(m.config.KafkaDestinationsTopicName, uuid.New(), ingestMessageBytes, headers, kafka.PartitionAny, "", false); err != nil {
			atomic.AddInt64(&job.ErrorCount, 1)
			return fmt.Errorf("failed to produce message: %w", err)
		}

		successCount := atomic.AddInt64(&job.SuccessCount, 1)
		if job.Config.Limit > 0 && successCount >= job.Config.Limit {
			m.Infof("Reached event limit of %d, stopping processing", job.Config.Limit)
			return nil
		}
	}

	return nil
}

// downloadS3File downloads a file from S3 to a temporary location
func (m *ReprocessingJobManager) downloadS3File(ctx context.Context, s3Path string) (string, error) {
	// Parse S3 path
	parts := strings.SplitN(strings.TrimPrefix(s3Path, "s3://"), "/", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid S3 path: %s", s3Path)
	}

	bucket := parts[0]
	key := parts[1]

	// Create temp file
	tempFile, err := os.CreateTemp("", "reprocess-*.ndjson")
	if err != nil {
		return "", err
	}
	defer tempFile.Close()

	// Download from S3
	result, err := m.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		os.Remove(tempFile.Name())
		return "", err
	}
	defer result.Body.Close()

	// Copy to temp file
	if _, err := io.Copy(tempFile, result.Body); err != nil {
		os.Remove(tempFile.Name())
		return "", err
	}

	return tempFile.Name(), nil
}

// handleJobError handles a job error
func (m *ReprocessingJobManager) handleJobError(job *ReprocessingJob, err error) {
	job.mu.Lock()
	job.Status = JobStatusFailed
	job.LastError = err.Error()
	job.mu.Unlock()

	m.Errorf("Job %s failed: %v", job.ID, err)
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
		filenameTime, err := parseFilenameTimestamp(file)
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

		// Check for date range overlap:
		// File time span: [filenameTime, modTime]
		// Requested range: [dateFrom, dateTo]
		// Overlap exists if: filenameTime <= dateTo && modTime >= dateFrom

		// Skip if file was created after the end of requested range
		if !dateTo.IsZero() && filenameTime.After(dateTo) {
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

// shouldContinueOnError determines if processing should continue after an error
func (m *ReprocessingJobManager) shouldContinueOnError() bool {
	// For now, always continue on error
	// This could be made configurable
	return true
}

// Close shuts down the reprocessing job manager
func (m *ReprocessingJobManager) Close() error {
	m.mu.Lock()
	jobCount := len(m.jobs)
	m.mu.Unlock()

	if jobCount == 0 {
		m.Infof("No reprocessing jobs to shutdown")
		return nil
	}

	m.Infof("Shutting down reprocessing job manager with %d jobs", jobCount)

	// First, cancel all running jobs
	m.mu.Lock()
	var runningJobs []*ReprocessingJob
	for _, job := range m.jobs {
		job.mu.Lock()
		if job.Status == JobStatusRunning || job.Status == JobStatusPaused {
			job.cancel()
			runningJobs = append(runningJobs, job)
		}
		job.mu.Unlock()
	}
	m.mu.Unlock()

	// Give jobs a moment to respond to cancellation
	if len(runningJobs) > 0 {
		m.Infof("Waiting for %d running jobs to stop gracefully...", len(runningJobs))
		time.Sleep(2 * time.Second)
	}

	// Now log final status of all jobs
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, job := range m.jobs {
		job.mu.Lock()

		// Log final status
		status := map[string]interface{}{
			"job_id":            job.ID,
			"status":            job.Status,
			"current_file":      job.CurrentFile,
			"current_line":      job.CurrentLine,
			"current_timestamp": job.CurrentTimestamp,
			"total_files":       job.TotalFiles,
			"processed_files":   job.ProcessedFiles,
			"success_count":     atomic.LoadInt64(&job.SuccessCount),
			"error_count":       atomic.LoadInt64(&job.ErrorCount),
			"skipped_count":     atomic.LoadInt64(&job.SkippedCount),
			"created_at":        job.CreatedAt.Format("2006-01-02T15:04:05Z"),
		}

		if job.StartedAt != nil {
			status["started_at"] = job.StartedAt.Format("2006-01-02T15:04:05Z")
		}

		if job.CompletedAt != nil {
			status["completed_at"] = job.CompletedAt.Format("2006-01-02T15:04:05Z")
		} else {
			// Job is being cancelled during shutdown
			now := time.Now()
			job.CompletedAt = &now
			status["completed_at"] = now.Format("2006-01-02T15:04:05Z")
		}

		if job.LastError != "" {
			status["last_error"] = job.LastError
		}

		// Calculate progress
		var progress float64
		if job.TotalFiles > 0 {
			progress = float64(job.ProcessedFiles) / float64(job.TotalFiles)
		}
		status["progress"] = progress

		// Update status for jobs that were running during shutdown
		originalStatus := job.Status
		if originalStatus == JobStatusRunning || originalStatus == JobStatusPaused {
			job.Status = JobStatusCancelled
			status["status"] = JobStatusCancelled
		}

		// Log the final status
		if job.Status == JobStatusCompleted {
			m.Infof("Final job status - COMPLETED: %+v", status)
		} else if job.Status == JobStatusFailed {
			m.Errorf("Final job status - FAILED: %+v", status)
		} else if job.Status == JobStatusCancelled {
			if originalStatus == JobStatusRunning || originalStatus == JobStatusPaused {
				m.Warnf("Final job status - CANCELLED (shutdown): %+v", status)
			} else {
				m.Infof("Final job status - CANCELLED (user): %+v", status)
			}
		} else {
			m.Infof("Final job status - %s: %+v", job.Status, status)
		}

		job.mu.Unlock()
	}

	m.Infof("Reprocessing job manager shutdown complete - logged status for %d jobs", len(m.jobs))
	return nil
}
