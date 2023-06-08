package sql

import (
	"context"
	"errors"
	"fmt"
	types2 "github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/coordination"
	"github.com/jitsucom/bulker/jitsubase/locks"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"regexp"
	"sync"
	"time"
)

const tableLockTimeout = time.Minute

// IdentifierFunction adapts identifier name to format required by database e.g. masks or escapes special characters
type IdentifierFunction func(identifier string) (adapted string, needQuotes bool)

var (
	// Generally unsupported characters in SQL identifiers: all except letters(any languages), underscore, numbers, space, dollar sign, hyphen
	sqlIdentifierUnsupportedCharacters = regexp.MustCompile(`[^\p{L}_\d $-]`)
	// SQL identifier that can be used without quotes: starts with latin letter or underscore, contains only lowercase latin letters, numbers and underscores
	sqlUnquotedIdentifierPattern = regexp.MustCompile(`^[a-z_][0-9a-z_]*$`)
)

// TableHelper keeps tables schema state inmemory and update it according to incoming new data
// consider that all tables are in one destination schema.
// note: Assume that after any outer changes in db we need to increment table version in Service
type TableHelper struct {
	sync.RWMutex

	sqlAdapter          SQLAdapter
	coordinationService coordination.Service
	tablesCache         map[string]*Table

	maxColumns int

	maxIdentifierLength int
	identifierQuoteChar rune

	tableNameFunc  IdentifierFunction
	columnNameFunc IdentifierFunction
}

// NewTableHelper returns configured TableHelper instance
// Note: columnTypesMapping must be not empty (or fields will be ignored)
func NewTableHelper(sqlAdapter SQLAdapter, maxIdentifierLength int, identifierQuoteChar rune) TableHelper {
	return TableHelper{
		sqlAdapter:          sqlAdapter,
		coordinationService: coordination.DummyCoordinationService{},
		tablesCache:         map[string]*Table{},

		maxColumns: 1000,

		maxIdentifierLength: maxIdentifierLength,
		identifierQuoteChar: identifierQuoteChar,
	}
}

// MapTableSchema maps types.TypesHeader (JSON structure with json data types) into types.Table (structure with SQL types)
// applies column types mapping
// adjusts object properties names to column names
func (th *TableHelper) MapTableSchema(batchHeader *TypesHeader, object types2.Object, pkFields utils.Set[string], timestampColumn string) (*Table, types2.Object) {
	adaptedPKFields := utils.NewSet[string]()
	for pkField := range pkFields {
		adaptedPKFields.Put(th.ColumnName(pkField))
	}
	table := &Table{
		Name:      th.sqlAdapter.TableName(batchHeader.TableName),
		Columns:   Columns{},
		Partition: batchHeader.Partition,
		PKFields:  adaptedPKFields,
	}
	if timestampColumn != "" {
		table.TimestampColumn = th.ColumnName(timestampColumn)
	}

	//pk fields from the configuration
	if len(adaptedPKFields) > 0 {
		table.PrimaryKeyName = BuildConstraintName(table.Name)
	}

	//need to adapt object properties to column names
	needAdapt := false
	for fieldName, field := range batchHeader.Fields {
		colName := th.ColumnName(fieldName)
		if !needAdapt && colName != fieldName {
			needAdapt = true
		}
		suggestedSQLType, ok := field.GetSuggestedSQLType()
		if ok {
			table.Columns[colName] = suggestedSQLType
			continue
		}

		//map Jitsu type -> SQL type
		sqlType, ok := th.sqlAdapter.GetSQLType(field.GetType())
		if ok {
			table.Columns[colName] = types2.SQLColumn{DataType: field.GetType(), Type: sqlType, New: true}
		} else {
			logging.SystemErrorf("Unknown column type %s mapping for %s", field.GetType(), th.sqlAdapter.Type())
		}
	}
	if needAdapt {
		adaptedObject := make(types2.Object, len(object))
		for fieldName, field := range object {
			adaptedObject[th.ColumnName(fieldName)] = field
		}
		return table, adaptedObject
	} else {
		return table, object
	}
}

// EnsureTableWithCaching calls ensureTable with cacheTable = true
// it is used in stream destinations (because we don't have time to select table schema, but there is retry on error)
func (th *TableHelper) EnsureTableWithCaching(ctx context.Context, destinationID string, dataSchema *Table) (*Table, error) {
	return th.ensureTable(ctx, destinationID, dataSchema, true)
}

// EnsureTableWithoutCaching calls ensureTable with cacheTable = true
// it is used in batch destinations and syncStore (because we have time to select table schema)
func (th *TableHelper) EnsureTableWithoutCaching(ctx context.Context, destinationID string, dataSchema *Table) (*Table, error) {
	return th.ensureTable(ctx, destinationID, dataSchema, false)
}

// ensureTable returns DB table schema and err if occurred
// if table doesn't exist - create a new one and increment version
// if exists - calculate diff, patch existing one with diff and increment version
// returns actual db table schema (with actual db types)
func (th *TableHelper) ensureTable(ctx context.Context, destinationID string, desiredSchema *Table, cacheTable bool) (actualSchema *Table, err error) {
	defer func() {
		if err != nil {
			th.clearCache(desiredSchema.Name)
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

	th.updateCached(dbSchema.Name, dbSchema)

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

	th.updateCached(dataSchema.Name, dbSchema)

	return dbSchema, nil
}

// refreshTableSchema force get (or create) db table schema and update it in-memory
func (th *TableHelper) refreshTableSchema(ctx context.Context, destinationName string, dataSchema *Table) (*Table, error) {
	dbTableSchema, err := th.getOrCreateWithLock(ctx, destinationName, dataSchema)
	if err != nil {
		return nil, err
	}

	th.updateCached(dataSchema.Name, dbTableSchema)

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
	dbSchema, ok := th.tablesCache[tableName]
	th.RUnlock()

	if ok {
		return dbSchema.Clone(), true
	}
	return nil, false
}

func (th *TableHelper) updateCached(tableName string, dbSchema *Table) {
	th.Lock()
	cloned := dbSchema.Clone()
	cloned.Cached = true
	th.tablesCache[tableName] = cloned
	th.Unlock()
}

// clearCache removes cached table schema for cache for provided table
func (th *TableHelper) clearCache(tableName string) {
	th.Lock()
	delete(th.tablesCache, tableName)
	th.Unlock()
}

// quotedColumnName adapts table name to sql identifier rules of database and quotes accordingly (if needed)
func (th *TableHelper) quotedTableName(tableName string) string {
	quoted, _ := th.adaptTableName(tableName)
	return quoted
}

// quotedColumnName adapts column name to sql identifier rules of database and quotes accordingly (if needed)
func (th *TableHelper) quotedColumnName(columnName string) string {
	quoted, _ := th.adaptColumnName(columnName)
	return quoted
}

func (th *TableHelper) adaptTableName(tableName string) (quotedIfNeeded string, unquoted string) {
	return th.adaptSqlIdentifier(tableName, "table", th.tableNameFunc)
}

func (th *TableHelper) adaptColumnName(columnName string) (quotedIfNeeded string, unquoted string) {
	return th.adaptSqlIdentifier(columnName, "column", th.columnNameFunc)
}

// adaptSqlIdentifier adapts the given identifier to basic rules derived from the SQL standard and injection protection:
// - must only contain letters, numbers, underscores, hyphen, and spaces - all other characters are removed
// - identifiers are that use different character cases, space, hyphen or don't begin with letter or underscore get quoted
func (th *TableHelper) adaptSqlIdentifier(identifier string, kind string, idFunc IdentifierFunction) (quotedIfNeeded string, unquoted string) {
	useQuoting := th.identifierQuoteChar != rune(0)
	cleanIdentifier := sqlIdentifierUnsupportedCharacters.ReplaceAllString(identifier, "")
	if cleanIdentifier == "" {
		cleanIdentifier = fmt.Sprintf("%s_%x", kind, utils.HashString(identifier))
	}
	result := utils.ShortenString(cleanIdentifier, th.maxIdentifierLength)
	if idFunc != nil {
		result, useQuoting = idFunc(result)
	}

	if useQuoting {
		return fmt.Sprintf(`%c%s%c`, th.identifierQuoteChar, result, th.identifierQuoteChar), result
	} else {
		return result, result
	}
}

func (th *TableHelper) TableName(tableName string) string {
	_, unquoted := th.adaptTableName(tableName)
	return unquoted
}

func (th *TableHelper) ColumnName(columnName string) string {
	_, unquoted := th.adaptColumnName(columnName)
	return unquoted
}
