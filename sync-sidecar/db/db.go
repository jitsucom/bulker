package db

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

const (
	upsertSpecSQL = `INSERT INTO source_spec (package, version, specs, timestamp ,error ) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT ON CONSTRAINT source_spec_pkey DO UPDATE SET specs = $3, timestamp = $4, error=$5`

	upsertCatalogSQL = `INSERT INTO source_catalog (package, version, key, catalog, timestamp, status, description) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT ON CONSTRAINT source_catalog_pkey DO UPDATE SET catalog=$4, timestamp = $5, status=$6, description=$7`

	upsertStateSQL = `INSERT INTO source_state (sync_id, state, timestamp) VALUES ($1, $2, $3)
ON CONFLICT ON CONSTRAINT source_state_pkey DO UPDATE SET state=$2, timestamp = $3`

	upsertTaskSQL = `INSERT INTO source_task (sync_id, task_id, package, version, started_at, updated_at, status, description) VALUES ($1, $2, $3, $4, $5, $6, $7, $8 )
ON CONFLICT ON CONSTRAINT source_task_pkey DO UPDATE SET updated_at=$6, status = $7, description=$8`

	upsertRunningTaskSQL = `INSERT INTO source_task as st (sync_id, task_id, package, version, started_at, updated_at, status, description) VALUES ($1, $2, $3, $4, $5, $6, $7, $8 )
ON CONFLICT ON CONSTRAINT source_task_pkey DO UPDATE SET updated_at=$6, status = $7, description=$8 where st.status not in ('FAILED', 'SUCCESS', $7)`

	updateRunningTaskDateSQL = `UPDATE source_task SET updated_at=$2 where task_id=$1 and status = 'RUNNING'`

	upsertCheckSQL = `INSERT INTO source_check (package, version, key, status, description, timestamp) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT ON CONSTRAINT source_check_pkey DO UPDATE SET status = $4, description=$5, timestamp=$6`
)

func UpsertSpec(dbpool *pgxpool.Pool, packageName, packageVersion, specs any, timestamp time.Time, error string) error {
	_, err := dbpool.Exec(context.Background(), upsertSpecSQL, packageName, packageVersion, specs, timestamp, error)
	return err
}

func UpsertCatalog(dbpool *pgxpool.Pool, packageName, packageVersion, storageKey, catalog any, timestamp time.Time, status, description string) error {
	_, err := dbpool.Exec(context.Background(), upsertCatalogSQL, packageName, packageVersion, storageKey, catalog, timestamp, status, description)
	return err
}

func UpsertState(dbpool *pgxpool.Pool, syncId, state any, timestamp time.Time) error {
	_, err := dbpool.Exec(context.Background(), upsertStateSQL, syncId, state, timestamp)
	return err
}

func UpsertTask(dbpool *pgxpool.Pool, syncId, taskId, packageName, packageVersion string, startedAt time.Time, status, description string) error {
	_, err := dbpool.Exec(context.Background(), upsertTaskSQL, syncId, taskId, packageName, packageVersion, startedAt, time.Now(), status, description)
	return err
}

func UpsertRunningTask(dbpool *pgxpool.Pool, syncId, taskId, packageName, packageVersion string, startedAt time.Time, status, description string) error {
	_, err := dbpool.Exec(context.Background(), upsertRunningTaskSQL, syncId, taskId, packageName, packageVersion, startedAt, time.Now(), status, description)
	return err
}

func UpdateRunningTaskDate(dbpool *pgxpool.Pool, taskId string) error {
	_, err := dbpool.Exec(context.Background(), updateRunningTaskDateSQL, taskId, time.Now())
	return err
}

func UpsertCheck(dbpool *pgxpool.Pool, packageName, packageVersion, storageKey, status, description string, timestamp time.Time) error {
	_, err := dbpool.Exec(context.Background(), upsertCheckSQL, packageName, packageVersion, storageKey, status, description, timestamp)
	return err
}
