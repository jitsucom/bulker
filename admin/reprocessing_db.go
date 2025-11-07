package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jitsucom/bulker/jitsubase/jsonorder"
)

// Database access layer for reprocessing jobs

// InsertReprocessingJob creates a new reprocessing job record
func InsertReprocessingJob(pool *pgxpool.Pool, job *ReprocessingJob) error {
	configJSON, err := jsonorder.Marshal(job.Config)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	_, err = pool.Exec(context.Background(),
		`INSERT INTO reprocessing_jobs (id, config, status, created_at, k8s_job_name, total_files, total_workers)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		job.ID, configJSON, job.Status, job.CreatedAt, job.K8sJobName, job.TotalFiles, job.TotalWorkers)
	return err
}

// UpdateReprocessingJobStatus updates the job status and timestamps
func UpdateReprocessingJobStatus(pool *pgxpool.Pool, jobID string, status ReprocessingJobStatus, startedAt, completedAt *time.Time, errorMsg string) error {
	_, err := pool.Exec(context.Background(),
		`UPDATE reprocessing_jobs
		 SET status = $2, started_at = $3, completed_at = $4, error = $5
		 WHERE id = $1`,
		jobID, status, startedAt, completedAt, errorMsg)
	return err
}

// UpdateReprocessingJobK8sName updates the k8s_job_name field
func UpdateReprocessingJobK8sName(pool *pgxpool.Pool, jobID string, k8sJobName string) error {
	_, err := pool.Exec(context.Background(),
		`UPDATE reprocessing_jobs
		 SET k8s_job_name = $2
		 WHERE id = $1`,
		jobID, k8sJobName)
	return err
}

// GetReprocessingJob retrieves a job by ID with aggregated worker stats
func GetReprocessingJob(pool *pgxpool.Pool, jobID string) (*ReprocessingJob, error) {
	var job ReprocessingJob
	var configJSON []byte
	var startedAt, completedAt *time.Time
	var errorMsg *string

	err := pool.QueryRow(context.Background(),
		`SELECT id, config, status, created_at, started_at, completed_at, k8s_job_name, total_files, total_workers, error
		 FROM reprocessing_jobs WHERE id = $1`,
		jobID).Scan(&job.ID, &configJSON, &job.Status, &job.CreatedAt, &startedAt, &completedAt,
		&job.K8sJobName, &job.TotalFiles, &job.TotalWorkers, &errorMsg)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("job not found: %s", jobID)
		}
		return nil, err
	}

	job.StartedAt = startedAt
	job.CompletedAt = completedAt
	if errorMsg != nil {
		job.LastError = *errorMsg
	}

	if err := json.Unmarshal(configJSON, &job.Config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Get aggregated worker stats
	err = pool.QueryRow(context.Background(),
		`SELECT
			COALESCE(SUM(processed_files), 0),
			COALESCE(SUM(total_lines), 0),
			COALESCE(SUM(success_count), 0),
			COALESCE(SUM(error_count), 0),
			COALESCE(SUM(skipped_count), 0),
			COALESCE(SUM(processed_bytes), 0)
		 FROM reprocessing_workers WHERE job_id = $1`,
		jobID).Scan(&job.ProcessedFiles, &job.TotalLines, &job.SuccessCount, &job.ErrorCount, &job.SkippedCount, &job.ProcessedBytes)

	if err != nil && err != pgx.ErrNoRows {
		return nil, fmt.Errorf("failed to get worker stats: %w", err)
	}

	// Get current file from one of the running workers
	var currentFile *string
	var currentLine *int64
	var currentTimestamp *string
	err = pool.QueryRow(context.Background(),
		`SELECT current_file, current_line, current_ts
		 FROM reprocessing_workers
		 WHERE job_id = $1 AND status = 'running'
		 ORDER BY updated_at DESC LIMIT 1`,
		jobID).Scan(&currentFile, &currentLine, &currentTimestamp)

	if err == nil {
		if currentFile != nil {
			job.CurrentFile = *currentFile
		}
		if currentLine != nil {
			job.CurrentLine = *currentLine
		}
		if currentTimestamp != nil {
			job.CurrentTimestamp = *currentTimestamp
		}
	}

	return &job, nil
}

// ListReprocessingJobs lists all reprocessing jobs
func ListReprocessingJobs(ctx context.Context, pool *pgxpool.Pool) ([]*ReprocessingJob, error) {
	rows, err := pool.Query(ctx,
		`SELECT id, config, status, created_at, started_at, completed_at, k8s_job_name, total_files, total_workers, error
		 FROM reprocessing_jobs
		 ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []*ReprocessingJob
	for rows.Next() {
		var job ReprocessingJob
		var configJSON []byte
		var startedAt, completedAt *time.Time
		var errorMsg *string

		err := rows.Scan(&job.ID, &configJSON, &job.Status, &job.CreatedAt, &startedAt, &completedAt,
			&job.K8sJobName, &job.TotalFiles, &job.TotalWorkers, &errorMsg)
		if err != nil {
			return nil, err
		}

		job.StartedAt = startedAt
		job.CompletedAt = completedAt
		if errorMsg != nil {
			job.LastError = *errorMsg
		}

		if err := json.Unmarshal(configJSON, &job.Config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal config: %w", err)
		}

		// Get aggregated stats for this job
		err = pool.QueryRow(ctx,
			`SELECT
				COALESCE(SUM(processed_files), 0),
				COALESCE(SUM(total_lines), 0),
				COALESCE(SUM(success_count), 0),
				COALESCE(SUM(error_count), 0),
				COALESCE(SUM(skipped_count), 0),
				COALESCE(SUM(processed_bytes), 0)
			 FROM reprocessing_workers WHERE job_id = $1`,
			job.ID).Scan(&job.ProcessedFiles, &job.TotalLines, &job.SuccessCount, &job.ErrorCount, &job.SkippedCount, &job.ProcessedBytes)

		if err != nil && err != pgx.ErrNoRows {
			return nil, fmt.Errorf("failed to get worker stats: %w", err)
		}

		jobs = append(jobs, &job)
	}

	return jobs, rows.Err()
}

// UpsertWorkerStatus creates or updates a worker status record
func UpsertWorkerStatus(pool *pgxpool.Pool, jobID string, workerIndex int, status ReprocessingJobStatus,
	currentFile string, currentLine int64, currentTimestamp string,
	processedFiles, totalLines, successCount, errorCount, skippedCount, processedBytes int64, errorMsg string) error {

	_, err := pool.Exec(context.Background(),
		`INSERT INTO reprocessing_workers
		 (job_id, worker_index, status, started_at, updated_at, current_file, current_line, current_ts,
		  processed_files, total_lines, success_count, error_count, skipped_count, processed_bytes, error)
		 VALUES ($1, $2, $3, NOW(), NOW(), $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		 ON CONFLICT (job_id, worker_index)
		 DO UPDATE SET
		   status = $3,
		   updated_at = NOW(),
		   current_file = $4,
		   current_line = $5,
		   current_ts = $6,
		   processed_files = $7,
		   total_lines = $8,
		   success_count = $9,
		   error_count = $10,
		   skipped_count = $11,
		   processed_bytes = $12,
		   error = $13,
		   completed_at = CASE WHEN $3 IN ('completed', 'failed', 'cancelled') THEN NOW() ELSE reprocessing_workers.completed_at END`,
		jobID, workerIndex, status, currentFile, currentLine, currentTimestamp,
		processedFiles, totalLines, successCount, errorCount, skippedCount, processedBytes, errorMsg)

	return err
}

// InitializeWorkers creates initial worker records for a job
func InitializeWorkers(pool *pgxpool.Pool, jobID string, workerCount int, filesPerWorker []int) error {
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	for i := 0; i < workerCount; i++ {
		assignedFiles := 0
		if i < len(filesPerWorker) {
			assignedFiles = filesPerWorker[i]
		}
		_, err := tx.Exec(ctx,
			`INSERT INTO reprocessing_workers
			 (job_id, worker_index, status, updated_at, assigned_files)
			 VALUES ($1, $2, 'pending', NOW(), $3)`,
			jobID, i, assignedFiles)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// GetWorkerStatus retrieves status for a specific worker
func GetWorkerStatus(pool *pgxpool.Pool, jobID string, workerIndex int) (map[string]interface{}, error) {
	var status string
	var startedAt, updatedAt, completedAt *time.Time
	var currentFile, currentTimestamp, errorMsg *string
	var currentLine, assignedFiles, processedFiles, totalLines, successCount, errorCount, skippedCount, processedBytes int64

	err := pool.QueryRow(context.Background(),
		`SELECT status, started_at, updated_at, completed_at, current_file, current_line, current_ts,
		        assigned_files, processed_files, total_lines, success_count, error_count, skipped_count, processed_bytes, error
		 FROM reprocessing_workers
		 WHERE job_id = $1 AND worker_index = $2`,
		jobID, workerIndex).Scan(&status, &startedAt, &updatedAt, &completedAt, &currentFile, &currentLine, &currentTimestamp,
		&assignedFiles, &processedFiles, &totalLines, &successCount, &errorCount, &skippedCount, &processedBytes, &errorMsg)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("worker not found: job=%s, index=%d", jobID, workerIndex)
		}
		return nil, err
	}

	result := map[string]interface{}{
		"status":          status,
		"worker_index":    workerIndex,
		"assigned_files":  assignedFiles,
		"processed_files": processedFiles,
		"total_lines":     totalLines,
		"success_count":   successCount,
		"error_count":     errorCount,
		"skipped_count":   skippedCount,
		"processed_bytes": processedBytes,
		"current_line":    currentLine,
	}

	if startedAt != nil {
		result["started_at"] = startedAt.Format(time.RFC3339)
	}
	if updatedAt != nil {
		result["updated_at"] = updatedAt.Format(time.RFC3339)
	}
	if completedAt != nil {
		result["completed_at"] = completedAt.Format(time.RFC3339)
	}
	if currentFile != nil {
		result["current_file"] = *currentFile
	}
	if currentTimestamp != nil {
		result["current_ts"] = *currentTimestamp
	}
	if errorMsg != nil {
		result["error"] = *errorMsg
	}

	return result, nil
}

// GetAllWorkerStatuses retrieves status for all workers of a job
func GetAllWorkerStatuses(pool *pgxpool.Pool, jobID string) ([]map[string]interface{}, error) {
	rows, err := pool.Query(context.Background(),
		`SELECT worker_index, status, started_at, updated_at, completed_at, current_file, current_line, current_ts,
		        assigned_files, processed_files, total_lines, success_count, error_count, skipped_count, processed_bytes, error
		 FROM reprocessing_workers
		 WHERE job_id = $1
		 ORDER BY worker_index`,
		jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var workerIndex int
		var status string
		var startedAt, updatedAt, completedAt *time.Time
		var currentFile, currentTimestamp, errorMsg *string
		var currentLine, assignedFiles, processedFiles, totalLines, successCount, errorCount, skippedCount, processedBytes int64

		err := rows.Scan(&workerIndex, &status, &startedAt, &updatedAt, &completedAt, &currentFile, &currentLine, &currentTimestamp,
			&assignedFiles, &processedFiles, &totalLines, &successCount, &errorCount, &skippedCount, &processedBytes, &errorMsg)
		if err != nil {
			return nil, err
		}

		result := map[string]interface{}{
			"worker_index":    workerIndex,
			"status":          status,
			"assigned_files":  assignedFiles,
			"processed_files": processedFiles,
			"total_lines":     totalLines,
			"success_count":   successCount,
			"error_count":     errorCount,
			"skipped_count":   skippedCount,
			"processed_bytes": processedBytes,
			"current_line":    currentLine,
		}

		if startedAt != nil {
			result["started_at"] = startedAt.Format(time.RFC3339)
		}
		if updatedAt != nil {
			result["updated_at"] = updatedAt.Format(time.RFC3339)
		}
		if completedAt != nil {
			result["completed_at"] = completedAt.Format(time.RFC3339)
		}
		if currentFile != nil {
			result["current_file"] = *currentFile
		}
		if currentTimestamp != nil {
			result["current_ts"] = *currentTimestamp
		}
		if errorMsg != nil {
			result["error"] = *errorMsg
		}

		results = append(results, result)
	}

	return results, rows.Err()
}
