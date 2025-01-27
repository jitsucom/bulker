package db

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

const (
	upsertSpecSQL = `INSERT INTO source_spec as s (package, version, specs, timestamp, error ) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT ON CONSTRAINT source_spec_pkey DO UPDATE SET specs = $3, timestamp = $4, error=$5 where s.specs is null`

	insertSpecErrorSQL = `INSERT INTO source_spec as s (package, version, timestamp, error ) VALUES ($1, $2, $3, $4)
ON CONFLICT ON CONSTRAINT source_spec_pkey DO UPDATE SET timestamp = $3, error=$4 where s.specs is null`

	upsertCatalogStatusSQL = `INSERT INTO source_catalog (package, version, key, timestamp, status, description) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT ON CONSTRAINT source_catalog_pkey DO UPDATE SET timestamp = $4, status=$5, description=$6`

	upsertRunningCatalogStatusSQL = `INSERT INTO source_catalog as sc (package, version, key, timestamp, status, description) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT ON CONSTRAINT source_catalog_pkey DO UPDATE SET timestamp = $4, status=$5, description=$6 where sc.status = 'RUNNING'`

	upsertCatalogSuccessSQL = `INSERT INTO source_catalog (package, version, key, catalog, timestamp, status, description) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT ON CONSTRAINT source_catalog_pkey DO UPDATE SET catalog=$4, timestamp = $5, status=$6, description=$7`

	upsertStateSQL = `INSERT INTO source_state (sync_id, stream, state, timestamp) VALUES ($1, $2, $3, $4)
ON CONFLICT ON CONSTRAINT source_state_pkey DO UPDATE SET state=$3, timestamp = $4`

	upsertTaskDescriptionSQL = `INSERT INTO source_task (sync_id, task_id, package, version, started_at, updated_at, status, description) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT ON CONSTRAINT source_task_pkey DO UPDATE SET updated_at=$6, status = $7, description=$8`

	upsertTaskErrorSQL = `INSERT INTO source_task (sync_id, task_id, package, version, started_at, updated_at, status, error) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT ON CONSTRAINT source_task_pkey DO UPDATE SET updated_at=$6, status = $7, error=$8`

	upsertRunningTaskSQL = `INSERT INTO source_task as st (sync_id, task_id, package, version, started_at, updated_at, status, error, started_by) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT ON CONSTRAINT source_task_pkey DO UPDATE SET updated_at=$6, status = $7, error=$8, started_by=$9 where st.status = 'RUNNING'`

	updateRunningTaskDateSQL = `UPDATE source_task SET updated_at=$2 where task_id=$1 and status = 'RUNNING'`

	updateRunningTaskMetricsSQL = `UPDATE source_task SET updated_at=$2, metrics=$3 where task_id=$1 and status = 'RUNNING'`

	updateRunningTaskStatusSQL = `UPDATE source_task SET status=$2 where task_id=$1 and status = 'RUNNING'`

	upsertCheckSQL = `INSERT INTO source_check (package, version, key, status, description, timestamp) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT ON CONSTRAINT source_check_pkey DO UPDATE SET status = $4, description=$5, timestamp=$6`

	insertCheckErrorSQL = `INSERT INTO source_check (package, version, key, status, description, timestamp) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT ON CONSTRAINT source_check_pkey DO NOTHING`

	insertIntoTaskLog = `INSERT INTO task_log (id, level, logger, message, sync_id, task_id, timestamp) VALUES ($1, $2, $3, $4, $5, $6, $7)`

	closeStaleTasksSQL = `UPDATE source_task SET status = 'FAILED', description = 'The sync task was interrupted unexpectedly. Please contact support@jitsu.com' WHERE status = 'RUNNING' AND updated_at < $1`
)

func UpsertSpec(dbpool *pgxpool.Pool, packageName, packageVersion, specs any, timestamp time.Time, error string) error {
	_, err := dbpool.Exec(context.Background(), upsertSpecSQL, packageName, packageVersion, specs, timestamp, error)
	return err
}

func InsertSpecError(dbpool *pgxpool.Pool, packageName, packageVersion string, timestamp time.Time, error string) error {
	_, err := dbpool.Exec(context.Background(), insertSpecErrorSQL, packageName, packageVersion, timestamp, error)
	return err
}

func UpsertCatalogStatus(dbpool *pgxpool.Pool, packageName, packageVersion, storageKey string, timestamp time.Time, status, description string) error {
	_, err := dbpool.Exec(context.Background(), upsertCatalogStatusSQL, packageName, packageVersion, storageKey, timestamp, status, description)
	return err
}

func UpsertRunningCatalogStatus(dbpool *pgxpool.Pool, packageName, packageVersion, storageKey string, timestamp time.Time, status, description string) error {
	_, err := dbpool.Exec(context.Background(), upsertRunningCatalogStatusSQL, packageName, packageVersion, storageKey, timestamp, status, description)
	return err
}

func UpsertCatalogSuccess(dbpool *pgxpool.Pool, packageName, packageVersion, storageKey string, catalog any, timestamp time.Time, status, description string) error {
	_, err := dbpool.Exec(context.Background(), upsertCatalogSuccessSQL, packageName, packageVersion, storageKey, catalog, timestamp, status, description)
	return err
}

func UpsertState(dbpool *pgxpool.Pool, syncId, stream string, state any, timestamp time.Time) error {
	_, err := dbpool.Exec(context.Background(), upsertStateSQL, syncId, stream, state, timestamp)
	return err
}

func UpsertTaskDescription(dbpool *pgxpool.Pool, syncId, taskId, packageName, packageVersion string, startedAt time.Time, status, description string) error {
	_, err := dbpool.Exec(context.Background(), upsertTaskDescriptionSQL, syncId, taskId, packageName, packageVersion, startedAt, time.Now(), status, description)
	return err
}

func UpsertTaskError(dbpool *pgxpool.Pool, syncId, taskId, packageName, packageVersion string, startedAt time.Time, status, error string) error {
	_, err := dbpool.Exec(context.Background(), upsertTaskErrorSQL, syncId, taskId, packageName, packageVersion, startedAt, time.Now(), status, error)
	return err
}

func UpsertRunningTask(dbpool *pgxpool.Pool, syncId, taskId, packageName, packageVersion string, startedAt time.Time, status, error, startedBy string) error {
	_, err := dbpool.Exec(context.Background(), upsertRunningTaskSQL, syncId, taskId, packageName, packageVersion, startedAt, time.Now(), status, error, startedBy)
	return err
}

func UpdateRunningTaskDate(dbpool *pgxpool.Pool, taskId string) error {
	_, err := dbpool.Exec(context.Background(), updateRunningTaskDateSQL, taskId, time.Now())
	return err
}

func UpdateRunningTaskMetrics(dbpool *pgxpool.Pool, taskId string, metrics map[string]any) error {
	_, err := dbpool.Exec(context.Background(), updateRunningTaskMetricsSQL, taskId, time.Now(), metrics)
	return err
}

func UpdateRunningTaskStatus(dbpool *pgxpool.Pool, taskId, status string) error {
	_, err := dbpool.Exec(context.Background(), updateRunningTaskStatusSQL, taskId, status)
	return err
}

func UpsertCheck(dbpool *pgxpool.Pool, packageName, packageVersion, storageKey, status, description string, timestamp time.Time) error {
	_, err := dbpool.Exec(context.Background(), upsertCheckSQL, packageName, packageVersion, storageKey, status, description, timestamp)
	return err
}

func InsertCheckError(dbpool *pgxpool.Pool, packageName, packageVersion, storageKey, status, description string, timestamp time.Time) error {
	_, err := dbpool.Exec(context.Background(), insertCheckErrorSQL, packageName, packageVersion, storageKey, status, description, timestamp)
	return err
}

func InsertTaskLog(dbpool *pgxpool.Pool, id, level, logger, message, syncId, taskId string, timestamp time.Time) error {
	_, err := dbpool.Exec(context.Background(), insertIntoTaskLog, id, level, logger, message, syncId, taskId, timestamp)
	return err
}

func CloseStaleTasks(dbpool *pgxpool.Pool, timestamp time.Time) error {
	_, err := dbpool.Exec(context.Background(), closeStaleTasksSQL, timestamp)
	return err
}
