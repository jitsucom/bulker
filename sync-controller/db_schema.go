package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"strings"
)

var ddls = []string{
	`create table if not exists source_spec()`,
	`alter table source_spec add column if not exists package varchar not null`,
	`alter table source_spec add column if not exists version varchar not null`,
	`alter table source_spec add column if not exists specs json`,
	`alter table source_spec add column if not exists timestamp timestamp with time zone`,
	`alter table source_spec add column if not exists error varchar`,
	`alter table source_spec add constraint source_spec_pk primary key (package, version)`,
	`create table if not exists source_catalog()`,
	`alter table source_catalog add column if not exists source_id varchar not null`,
	`alter table source_catalog add column if not exists package varchar not null`,
	`alter table source_catalog add column if not exists version varchar not null`,
	`alter table source_catalog add column if not exists config_hash varchar not null`,
	`alter table source_catalog add column if not exists catalog json`,
	`alter table source_catalog add column if not exists timestamp timestamp with time zone`,
	`alter table source_catalog add column if not exists error varchar`,
	`alter table source_catalog add constraint source_catalog_pk primary key (source_id, package, version, config_hash)`,
	`create table if not exists source_check()`,
	`alter table source_check add column if not exists source_id varchar not null`,
	`alter table source_check add column if not exists package varchar not null`,
	`alter table source_check add column if not exists version varchar not null`,
	`alter table source_check add column if not exists config_hash varchar not null`,
	`alter table source_check add column if not exists status varchar`,
	`alter table source_check add column if not exists description varchar`,
	`alter table source_check add column if not exists timestamp timestamp with time zone`,
	`alter table source_check add constraint source_check_pk primary key (source_id, package, version, config_hash)`,
	`create table if not exists source_state()`,
	`alter table source_state add column if not exists sync_id varchar not null`,
	`alter table source_state add column if not exists state json`,
	`alter table source_state add column if not exists timestamp timestamp with time zone`,
	`alter table source_state add constraint source_state_pk primary key (sync_id)`,
	`create table if not exists source_task()`,
	`alter table source_task add column if not exists sync_id varchar not null`,
	`alter table source_task add column if not exists task_id varchar not null`,
	`alter table source_task add column if not exists package varchar not null`,
	`alter table source_task add column if not exists version varchar not null`,
	`alter table source_task add column if not exists started_at timestamp with time zone`,
	`alter table source_task add column if not exists updated_at timestamp with time zone`,
	`alter table source_task add column if not exists status varchar`,
	`alter table source_task add column if not exists description varchar`,
	`alter table source_task add constraint source_task_pk primary key (task_id)`,
	`create index if not exists source_task_sync_id_index on source_task (sync_id)`,
	`create index if not exists source_task_started_at_index on source_task (started_at desc)`,
}

func InitDBSchema(dbpool *pgxpool.Pool) error {
	for _, ddl := range ddls {
		_, err := dbpool.Exec(context.Background(), ddl)
		if err != nil && !IsConstraintExistsError(err) {
			return fmt.Errorf("Error running DDL query '%s': %v", ddl, err)
		}
	}
	return nil
}

func IsConstraintExistsError(err error) bool {
	return strings.Contains(err.Error(), "multiple primary keys for table")
}
