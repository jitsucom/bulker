package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
)

const SpecTableDDL = `create table if not exists source_spec
(
    package   varchar not null,
    version   varchar not null,
    specs     json,
    timestamp timestamp with time zone,
    error     varchar,
    constraint source_spec_pk
        primary key (package, version)
)`

const CatalogTableDDL = `create table if not exists source_catalog
(
    source_id   varchar not null,
    package     varchar not null,
    version     varchar not null,
    config_hash varchar not null,
    catalog     json,
    timestamp   timestamp with time zone,
    error       varchar,
    constraint source_catalog_pk
        primary key (source_id, package, version, config_hash)
)`

const StateTableDDL = `create table if not exists source_state
(
    source_id varchar not null
        constraint source_state_pk
            primary key,
    state     json,
    timestamp timestamp with time zone,
    constraint source_state_pk
        primary key (source_id)
);`

func InitDBSchema(dbpool *pgxpool.Pool) error {
	_, err := dbpool.Exec(context.Background(), SpecTableDDL)
	if err != nil {
		return fmt.Errorf("Error creating source_spec table: %v", err)
	}
	_, err = dbpool.Exec(context.Background(), CatalogTableDDL)
	if err != nil {
		return fmt.Errorf("Error creating source_catalog table: %v", err)
	}
	_, err = dbpool.Exec(context.Background(), StateTableDDL)
	if err != nil {
		return fmt.Errorf("Error creating source_state table: %v", err)
	}
	return nil
}
