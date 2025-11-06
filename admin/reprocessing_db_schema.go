package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Database schema for reprocessing jobs and their status

var reprocessingDDLs = []string{
	// Main reprocessing jobs table
	`CREATE TABLE IF NOT EXISTS reprocessing_jobs (
		id VARCHAR(64) PRIMARY KEY,
		config JSONB NOT NULL,
		status VARCHAR(32) NOT NULL,
		created_at TIMESTAMP WITH TIME ZONE NOT NULL,
		started_at TIMESTAMP WITH TIME ZONE,
		completed_at TIMESTAMP WITH TIME ZONE,
		k8s_job_name VARCHAR(255),
		total_files INTEGER DEFAULT 0,
		total_workers INTEGER DEFAULT 0,
		error TEXT
	)`,

	// Index for listing jobs by creation date
	`CREATE INDEX IF NOT EXISTS reprocessing_jobs_created_at_idx ON reprocessing_jobs(created_at DESC)`,

	// Worker status table - one row per indexed job pod
	`CREATE TABLE IF NOT EXISTS reprocessing_workers (
		job_id VARCHAR(64) NOT NULL,
		worker_index INTEGER NOT NULL,
		status VARCHAR(32) NOT NULL,
		started_at TIMESTAMP WITH TIME ZONE,
		updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
		completed_at TIMESTAMP WITH TIME ZONE,
		current_file TEXT,
		current_line BIGINT DEFAULT 0,
		current_ts TEXT,
		assigned_files INTEGER DEFAULT 0,
		processed_files INTEGER DEFAULT 0,
		total_lines BIGINT DEFAULT 0,
		success_count BIGINT DEFAULT 0,
		error_count BIGINT DEFAULT 0,
		skipped_count BIGINT DEFAULT 0,
		total_bytes BIGINT DEFAULT 0,
		processed_bytes BIGINT DEFAULT 0,
		error TEXT,
		PRIMARY KEY (job_id, worker_index),
		FOREIGN KEY (job_id) REFERENCES reprocessing_jobs(id) ON DELETE CASCADE
	)`,

	// Index for querying workers by job
	`CREATE INDEX IF NOT EXISTS reprocessing_workers_job_id_idx ON reprocessing_workers(job_id)`,

	// Index for querying worker status
	`CREATE INDEX IF NOT EXISTS reprocessing_workers_status_idx ON reprocessing_workers(job_id, status)`,
}

// InitReprocessingDBSchema initializes the database schema for reprocessing
func InitReprocessingDBSchema(dbpool *pgxpool.Pool) error {
	for _, ddl := range reprocessingDDLs {
		_, err := dbpool.Exec(context.Background(), ddl)
		if err != nil {
			return fmt.Errorf("error running DDL query: %w", err)
		}
	}
	return nil
}
