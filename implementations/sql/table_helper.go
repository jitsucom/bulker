package sql

import (
	"context"
	"errors"
	"fmt"
	"github.com/jitsucom/bulker/base/coordination"
	"github.com/jitsucom/bulker/base/locks"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/types"
	"sync"
	"time"
)

const tableLockTimeout = time.Minute

// TableHelper keeps tables schema state inmemory and update it according to incoming new data
// consider that all tables are in one destination schema.
// note: Assume that after any outer changes in db we need to increment table version in Service
type TableHelper struct {
	sync.RWMutex

	sqlAdapter          SQLAdapter
	coordinationService coordination.Service
	tables              map[string]*Table

	pkFields           utils.Set[string]
	timestampColumn    string
	columnTypesMapping map[types.DataType]string

	destinationType string
	streamMode      bool
	maxColumns      int
}

// NewTableHelper returns configured TableHelper instance
// Note: columnTypesMapping must be not empty (or fields will be ignored)
func NewTableHelper(sqlAdapter SQLAdapter, coordinationService coordination.Service, pkFields utils.Set[string],
	timestampColumn string, maxColumns int) *TableHelper {
	return &TableHelper{
		sqlAdapter:          sqlAdapter,
		coordinationService: coordinationService,
		tables:              map[string]*Table{},

		pkFields:           pkFields,
		timestampColumn:    timestampColumn,
		columnTypesMapping: sqlAdapter.GetTypesMapping(),

		destinationType: sqlAdapter.Type(),
		maxColumns:      maxColumns,
	}
}

// MapTableSchema maps types.BatchHeader (JSON structure with json data types) into types.Table (structure with SQL types)
// applies column types mapping
func (th *TableHelper) MapTableSchema(batchHeader *BatchHeader) *Table {
	table := &Table{
		Name:            th.sqlAdapter.TableName(batchHeader.TableName),
		Columns:         Columns{},
		Partition:       batchHeader.Partition,
		PKFields:        th.pkFields,
		TimestampColumn: th.timestampColumn,
	}

	//pk fields from the configuration
	if len(th.pkFields) > 0 {
		table.PrimaryKeyName = BuildConstraintName(table.Name)
	}

	for fieldName, field := range batchHeader.Fields {
		suggestedSQLType, ok := field.GetSuggestedSQLType()
		if ok {
			table.Columns[fieldName] = suggestedSQLType
			continue
		}

		//map Jitsu type -> SQL type
		sqlType, ok := th.columnTypesMapping[field.GetType()]
		if ok {
			table.Columns[fieldName] = SQLColumn{Type: sqlType}
		} else {
			logging.SystemErrorf("Unknown column type mapping for %s mapping: %v", field.GetType(), th.columnTypesMapping)
		}
	}

	return table
}

// EnsureTableWithCaching calls EnsureTable with cacheTable = true
// it is used in stream destinations (because we don't have time to select table schema, but there is retry on error)
func (th *TableHelper) EnsureTableWithCaching(ctx context.Context, destinationID string, dataSchema *Table) (*Table, error) {
	return th.EnsureTable(ctx, destinationID, dataSchema, true)
}

// EnsureTableWithoutCaching calls EnsureTable with cacheTable = true
// it is used in batch destinations and syncStore (because we have time to select table schema)
func (th *TableHelper) EnsureTableWithoutCaching(ctx context.Context, destinationID string, dataSchema *Table) (*Table, error) {
	return th.EnsureTable(ctx, destinationID, dataSchema, false)
}

// EnsureTable returns DB table schema and err if occurred
// if table doesn't exist - create a new one and increment version
// if exists - calculate diff, patch existing one with diff and increment version
// returns actual db table schema (with actual db types)
func (th *TableHelper) EnsureTable(ctx context.Context, destinationID string, desiredSchema *Table, cacheTable bool) (actualSchema *Table, err error) {
	defer func() {
		if err != nil && cacheTable {
			th.ClearCache(desiredSchema.Name)
		}
	}()

	if cacheTable {
		actualSchema, err = th.getCachedOrCreateTableSchema(ctx, destinationID, desiredSchema)
	} else {
		actualSchema, err = th.getOrCreateWithLock(ctx, destinationID, desiredSchema)
	}
	if err != nil {
		return nil, err
	}

	if actualSchema.Cached {
		actualSchema, err = th.patchTableIfNeeded(ctx, destinationID, actualSchema, desiredSchema)
		if err == nil {
			return
		}
		// if patching of cached table failed - that may mean table was changed outside of bulker
		// get fresh table schema from db and try again
		actualSchema, err = th.getOrCreateWithLock(ctx, destinationID, desiredSchema)
	}

	return th.patchTableIfNeeded(ctx, destinationID, actualSchema, desiredSchema)
}

func (th *TableHelper) patchTableIfNeeded(ctx context.Context, destinationID string, currentSchema, desiredSchema *Table) (*Table, error) {
	//if diff doesn't exist - do nothing
	diff := currentSchema.Diff(desiredSchema)
	if !diff.Exists() {
		return currentSchema, nil
	}

	//check if max columns error
	if th.maxColumns > 0 {
		columnsCount := len(currentSchema.Columns) + len(diff.Columns)
		if columnsCount > th.maxColumns {
			//return nil, fmt.Errorf("Count of columns %d should be less or equal 'server.max_columns' (or destination.data_layout.max_columns) setting %d", columnsCount, th.maxColumns)
			logging.Warnf("[%s] Count of columns %d should be less or equal 'server.max_columns' (or destination.data_layout.max_columns) setting %d", destinationID, columnsCount, th.maxColumns)
		}
	}

	//** Diff exists **
	//patch table schema
	return th.patchTableWithLock(ctx, destinationID, desiredSchema)
}

// patchTable locks table, get from DWH and patch
func (th *TableHelper) patchTableWithLock(ctx context.Context, destinationID string, dataSchema *Table) (*Table, error) {
	tableIdentifier := th.getTableIdentifier(destinationID, dataSchema.Name)
	tableLock, err := th.lockTable(destinationID, dataSchema.Name, tableIdentifier)
	if err != nil {
		return nil, err
	}
	defer tableLock.Unlock()

	dbSchema, err := th.getOrCreate(ctx, dataSchema)
	if err != nil {
		return nil, err
	}

	//handle table schema local changes (patching was in another goroutine)
	diff := dbSchema.Diff(dataSchema)
	if !diff.Exists() {
		return dbSchema, nil
	}

	if err := th.sqlAdapter.PatchTableSchema(ctx, diff); err != nil {
		return nil, err
	}

	//** Save **
	//columns
	for k, v := range diff.Columns {
		dbSchema.Columns[k] = v
	}
	//pk fields
	if len(diff.PKFields) > 0 {
		dbSchema.PKFields = diff.PKFields
	}
	//remove pk fields if a deletion was
	if diff.DeletePkFields {
		dbSchema.PKFields = utils.Set[string]{}
	}

	th.UpdateCached(dbSchema.Name, dbSchema)

	return dbSchema, nil
}

func (th *TableHelper) getCachedOrCreateTableSchema(ctx context.Context, destinationName string, dataSchema *Table) (*Table, error) {
	dbSchema, ok := th.GetCached(dataSchema.Name)
	if ok {
		return dbSchema, nil
	}

	// Get data schema from DWH or create
	dbSchema, err := th.getOrCreateWithLock(ctx, destinationName, dataSchema)
	if err != nil {
		return nil, err
	}

	th.UpdateCached(dataSchema.Name, dbSchema)

	return dbSchema, nil
}

// RefreshTableSchema force get (or create) db table schema and update it in-memory
func (th *TableHelper) RefreshTableSchema(ctx context.Context, destinationName string, dataSchema *Table) (*Table, error) {
	dbTableSchema, err := th.getOrCreateWithLock(ctx, destinationName, dataSchema)
	if err != nil {
		return nil, err
	}

	th.UpdateCached(dataSchema.Name, dbTableSchema)

	return dbTableSchema, nil
}

// lock table -> get existing schema -> create a new one if doesn't exist -> return schema with version
func (th *TableHelper) getOrCreateWithLock(ctx context.Context, destinationID string, dataSchema *Table) (*Table, error) {
	tableIdentifier := th.getTableIdentifier(destinationID, dataSchema.Name)
	tableLock, err := th.lockTable(destinationID, dataSchema.Name, tableIdentifier)
	if err != nil {
		return nil, err
	}
	defer tableLock.Unlock()

	return th.getOrCreate(ctx, dataSchema)
}

func (th *TableHelper) getOrCreate(ctx context.Context, dataSchema *Table) (*Table, error) {
	//Get schema
	dbTableSchema, err := th.sqlAdapter.GetTableSchema(ctx, dataSchema.Name)
	if err != nil {
		return nil, err
	}

	//create new
	if !dbTableSchema.Exists() {
		if err := th.sqlAdapter.CreateTable(context.Background(), dataSchema); err != nil {
			return nil, err
		}

		dbTableSchema.Name = dataSchema.Name
		dbTableSchema.Columns = dataSchema.Columns
		dbTableSchema.PKFields = dataSchema.PKFields
		dbTableSchema.PrimaryKeyName = dataSchema.PrimaryKeyName
	}

	return dbTableSchema, nil
}

func (th *TableHelper) lockTable(destinationID, tableName, tableIdentifier string) (locks.Lock, error) {
	tableLock := th.coordinationService.CreateLock(tableIdentifier)
	locked, err := tableLock.TryLock(tableLockTimeout)
	if err != nil {
		msg := fmt.Sprintf("System error: Unable to lock destination [%s] table %s: %v", destinationID, tableName, err)
		//TODO: implement system error notification
		//notifications.SystemError(msg)
		return nil, errors.New(msg)
	}

	if !locked {
		return nil, fmt.Errorf("unable to lock table %s. Table has been already locked: timeout after %s", tableIdentifier, tableLockTimeout.String())
	}

	return tableLock, nil
}

func (th *TableHelper) getTableIdentifier(destinationID, tableName string) string {
	return destinationID + "_" + tableName
}

func (th *TableHelper) SetSQLAdapter(adapter SQLAdapter) {
	th.sqlAdapter = adapter
}

func (th *TableHelper) GetCached(tableName string) (*Table, bool) {
	th.RLock()
	dbSchema, ok := th.tables[tableName]
	th.RUnlock()

	if ok {
		return dbSchema.Clone(), true
	}
	return nil, false
}

func (th *TableHelper) UpdateCached(tableName string, dbSchema *Table) {
	th.Lock()
	cloned := dbSchema.Clone()
	cloned.Cached = true
	th.tables[tableName] = cloned
	th.Unlock()
}

// ClearCache removes cached table schema for cache for provided table
func (th *TableHelper) ClearCache(tableName string) {
	th.Lock()
	delete(th.tables, tableName)
	th.Unlock()
}
