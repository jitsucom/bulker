package sql

import (
	"context"
	"errors"
	"fmt"
	"github.com/jitsucom/bulker/bulkerlib"
	types2 "github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/coordination"
	"github.com/jitsucom/bulker/jitsubase/locks"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/jitsucom/bulker/jitsubase/types"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"regexp"
	"sync"
	"time"
)

const tableLockTimeout = time.Minute

// IdentifierFunction adapts identifier name to format required by database e.g. masks or escapes special characters
type IdentifierFunction func(identifier string, alphanumeric bool) (adapted string, needQuotes bool)

var (
	// Generally unsupported characters in SQL identifiers: all except letters(any languages), underscore, numbers, space, dollar sign, hyphen
	sqlIdentifierUnsupportedCharacters = regexp.MustCompile(`[^\p{L}_\d $-]+`)
)

// TableHelper keeps tables schema state inmemory and update it according to incoming new data
// consider that all tables are in one destination schema.
// note: Assume that after any outer changes in db we need to increment table version in Service
type TableHelper struct {
	sync.RWMutex

	coordinationService coordination.Service
	tablesCache         map[string]*Table

	maxIdentifierLength int
	identifierQuoteStr  string
	useQuoting          bool

	tableNameFunc  IdentifierFunction
	columnNameFunc IdentifierFunction

	columnNameCache *utils.SyncMapCache[bulkerlib.ColumnName]
}

// NewTableHelper returns configured TableHelper instance
// Note: columnTypesMapping must be not empty (or fields will be ignored)
func NewTableHelper(bulkerTypeId string, maxIdentifierLength int, identifierQuoteChar rune) TableHelper {
	return TableHelper{
		coordinationService: coordination.DummyCoordinationService{},
		tablesCache:         map[string]*Table{},

		maxIdentifierLength: maxIdentifierLength,
		identifierQuoteStr:  string(identifierQuoteChar),
		useQuoting:          identifierQuoteChar != 0,

		columnNameCache: bulkerlib.ColumnNameCache[bulkerTypeId],
	}
}

// MapTableSchema maps types.TypesHeader (JSON structure with json data types) into types.Table (structure with SQL types)
// applies column types mapping
// adjusts object properties names to column names
func (th *TableHelper) MapTableSchema(sqlAdapter SQLAdapter, batchHeader *TypesHeader, object types2.Object, pkColumns []string, timestampColumn string, namespace string) (*Table, types2.Object) {
	adaptedPKFields := types.NewOrderedSet[string]()
	for _, pkField := range pkColumns {
		adaptedPKFields.Put(pkField)
	}
	if namespace != "" {
		namespace = th.TableName(namespace)
	}
	table := &Table{
		Name:      sqlAdapter.TableName(batchHeader.TableName),
		Namespace: namespace,
		Columns:   NewColumns(),
		Partition: batchHeader.Partition,
		PKFields:  adaptedPKFields,
	}
	table.TimestampColumn = timestampColumn

	//pk fields from the configuration
	if !adaptedPKFields.Empty() {
		table.PrimaryKeyName = sqlAdapter.BuildConstraintName(table.Name)
	}

	for _, field := range batchHeader.Fields {
		fieldName := field.Name
		colName := th.ColumnName(fieldName)
		if colName != fieldName {
			object.Rename(fieldName, colName)
		}
		suggestedSQLType, ok := field.GetSuggestedSQLType()
		if ok {
			dt, ok := sqlAdapter.GetDataType(suggestedSQLType.Type)
			if ok {
				suggestedSQLType.DataType = dt
			}
			table.Columns.Set(colName, suggestedSQLType)
			continue
		}

		//map Jitsu type -> SQL type
		sqlType, ok := sqlAdapter.GetSQLType(field.GetType())
		if ok {
			table.Columns.Set(colName, types2.SQLColumn{DataType: field.GetType(), Type: sqlType, New: true})
		} else {
			logging.SystemErrorf("Unknown column type %s mapping for %s", field.GetType(), sqlAdapter.Type())
		}
	}
	return table, object
}

// MapSchema maps types.Schema into types.Table (structure with SQL types)
func (th *TableHelper) MapSchema(sqlAdapter SQLAdapter, schema types2.Schema, nameTransformer func(string) string) *Table {
	table := &Table{
		Name:    th.TableName(nameTransformer(schema.Name)),
		Columns: NewColumns(),
	}

	for _, field := range schema.Fields {
		colName := th.ColumnName(nameTransformer(field.Name))
		//map Jitsu type -> SQL type
		sqlType, ok := sqlAdapter.GetSQLType(field.Type)
		if ok {
			table.Columns.Set(colName, types2.SQLColumn{DataType: field.Type, Type: sqlType, Important: true})
		} else {
			logging.SystemErrorf("Unknown column type %s mapping for %s", field.Type, sqlAdapter.Type())
		}
	}
	return table
}

// EnsureTableWithCaching calls ensureTable with cacheTable = true
// it is used in stream destinations (because we don't have time to select table schema, but there is retry on error)
func (th *TableHelper) EnsureTableWithCaching(ctx context.Context, sqlAdapter SQLAdapter, destinationID string, dataSchema *Table) (*Table, error) {
	return th.ensureTable(ctx, sqlAdapter, destinationID, dataSchema, true)
}

// EnsureTableWithoutCaching calls ensureTable with cacheTable = true
// it is used in batch destinations and syncStore (because we have time to select table schema)
func (th *TableHelper) EnsureTableWithoutCaching(ctx context.Context, sqlAdapter SQLAdapter, destinationID string, dataSchema *Table) (*Table, error) {
	return th.ensureTable(ctx, sqlAdapter, destinationID, dataSchema, false)
}

// ensureTable returns DB table schema and err if occurred
// if table doesn't exist - create a new one and increment version
// if exists - calculate diff, patch existing one with diff and increment version
// returns actual db table schema (with actual db types)
func (th *TableHelper) ensureTable(ctx context.Context, sqlAdapter SQLAdapter, destinationID string, desiredSchema *Table, cacheTable bool) (actualSchema *Table, err error) {
	defer func() {
		if err != nil {
			th.clearCache(desiredSchema.Name)
		}
	}()

	if cacheTable {
		actualSchema, err = th.getCachedOrCreateTableSchema(ctx, sqlAdapter, destinationID, desiredSchema)
	} else {
		actualSchema, err = th.getOrCreateWithLock(ctx, sqlAdapter, destinationID, desiredSchema)
	}
	if err != nil {
		return nil, err
	}

	if actualSchema.Cached {
		actualSchema, err = th.patchTableIfNeeded(ctx, sqlAdapter, destinationID, actualSchema, desiredSchema)
		if err == nil {
			return
		}
		// if patching of cached table failed - that may mean table was changed outside of bulker
		// get fresh table schema from db and try again
		actualSchema, err = th.getOrCreateWithLock(ctx, sqlAdapter, destinationID, desiredSchema)
	}

	return th.patchTableIfNeeded(ctx, sqlAdapter, destinationID, actualSchema, desiredSchema)
}

func (th *TableHelper) patchTableIfNeeded(ctx context.Context, sqlAdapter SQLAdapter, destinationID string, currentSchema, desiredSchema *Table) (*Table, error) {
	//if diff doesn't exist - do nothing
	diff := currentSchema.Diff(sqlAdapter, desiredSchema)
	if !diff.Exists() {
		return currentSchema, nil
	}

	//** Diff exists **
	//patch table schema
	return th.patchTableWithLock(ctx, sqlAdapter, destinationID, currentSchema, diff)
}

// patchTable locks table, get from DWH and patch
func (th *TableHelper) patchTableWithLock(ctx context.Context, sqlAdapter SQLAdapter, destinationID string, currentSchema, diff *Table) (*Table, error) {
	tableIdentifier := th.getTableIdentifier(destinationID, diff.Name)
	tableLock, err := th.lockTable(destinationID, diff.Name, tableIdentifier)
	if err != nil {
		return nil, err
	}
	defer tableLock.Unlock()

	if err := sqlAdapter.PatchTableSchema(ctx, diff); err != nil {
		return nil, err
	}

	//** Save **
	//columns
	diff.Columns.ForEach(func(k string, v types2.SQLColumn) {
		currentSchema.Columns.Set(k, v)
	})
	//pk fields
	if !diff.PKFields.Empty() {
		currentSchema.PKFields = diff.PKFields
		currentSchema.PrimaryKeyName = diff.PrimaryKeyName
	} else if diff.DeletePrimaryKeyNamed != "" {
		currentSchema.PKFields = types.OrderedSet[string]{}
		currentSchema.PrimaryKeyName = ""
	}

	th.updateCached(diff.Name, currentSchema)

	return currentSchema, nil
}

func (th *TableHelper) getCachedOrCreateTableSchema(ctx context.Context, sqlAdapter SQLAdapter, destinationName string, dataSchema *Table) (*Table, error) {
	dbSchema, ok := th.GetCached(dataSchema.Name)
	if ok {
		return dbSchema, nil
	}

	// Get data schema from DWH or create
	dbSchema, err := th.getOrCreateWithLock(ctx, sqlAdapter, destinationName, dataSchema)
	if err != nil {
		return nil, err
	}

	th.updateCached(dataSchema.Name, dbSchema)

	return dbSchema, nil
}

// refreshTableSchema force get (or create) db table schema and update it in-memory
func (th *TableHelper) refreshTableSchema(ctx context.Context, sqlAdapter SQLAdapter, destinationName string, dataSchema *Table) (*Table, error) {
	dbTableSchema, err := th.getOrCreateWithLock(ctx, sqlAdapter, destinationName, dataSchema)
	if err != nil {
		return nil, err
	}

	th.updateCached(dataSchema.Name, dbTableSchema)

	return dbTableSchema, nil
}

// lock table -> get existing schema -> create a new one if doesn't exist -> return schema with version
func (th *TableHelper) getOrCreateWithLock(ctx context.Context, sqlAdapter SQLAdapter, destinationID string, dataSchema *Table) (*Table, error) {
	tableIdentifier := th.getTableIdentifier(destinationID, dataSchema.Name)
	tableLock, err := th.lockTable(destinationID, dataSchema.Name, tableIdentifier)
	if err != nil {
		return nil, err
	}
	defer tableLock.Unlock()

	return th.getOrCreate(ctx, sqlAdapter, dataSchema)
}

func (th *TableHelper) getOrCreate(ctx context.Context, sqlAdapter SQLAdapter, dataSchema *Table) (*Table, error) {
	//Get schema
	dbTableSchema, err := sqlAdapter.GetTableSchema(ctx, dataSchema.Namespace, dataSchema.Name)
	if err != nil {
		return nil, err
	}

	//create new
	if !dbTableSchema.Exists() {
		if err := sqlAdapter.CreateTable(ctx, dataSchema); err != nil {
			return nil, err
		}

		dbTableSchema.Name = dataSchema.Name
		dbTableSchema.Columns = dataSchema.Columns
		dbTableSchema.PKFields = dataSchema.PKFields
		dbTableSchema.PrimaryKeyName = dataSchema.PrimaryKeyName
		dbTableSchema.TimestampColumn = dataSchema.TimestampColumn
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

func (th *TableHelper) Get(ctx context.Context, sqlAdapter SQLAdapter, namespace string, tableName string, cacheTable bool) (*Table, error) {
	var table *Table
	var ok bool
	if cacheTable {
		table, ok = th.GetCached(tableName)
		if ok {
			return table, nil
		}
	}
	table, err := sqlAdapter.GetTableSchema(ctx, namespace, tableName)
	if err != nil {
		return nil, err
	}
	if table.Exists() && cacheTable {
		th.updateCached(table.Name, table)
	}
	return table, nil
}

func (th *TableHelper) GetCached(tableName string) (*Table, bool) {
	th.RLock()
	dbSchema, ok := th.tablesCache[tableName]
	th.RUnlock()

	if ok {
		return dbSchema.CleanClone(), true
	}
	return nil, false
}

func (th *TableHelper) updateCached(tableName string, dbSchema *Table) {
	th.Lock()
	cloned := dbSchema.CleanClone()
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
	unquoted, useQuoting := th.adaptSqlIdentifier(tableName, "table", th.tableNameFunc, utils.IsAlphanumeric(tableName))
	if useQuoting {
		return th.identifierQuoteStr + unquoted + th.identifierQuoteStr, unquoted
	} else {
		return unquoted, unquoted
	}
}

func (th *TableHelper) adaptColumnName(columnName string) (quotedIfNeeded string, unquoted string) {
	unquoted, useQuoting := th._adaptColumnName(columnName)
	if useQuoting {
		return th.identifierQuoteStr + unquoted + th.identifierQuoteStr, unquoted
	} else {
		return unquoted, unquoted
	}
}

func (th *TableHelper) _adaptColumnName(columnName string) (unquoted string, useQuoting bool) {
	var alphanumeric int
	useCache := th.columnNameFunc != nil
	if !useCache {
		a := utils.IsAlphanumeric(columnName)
		if a {
			alphanumeric = 1
		} else {
			useCache = true
			alphanumeric = -1
		}
	}
	if useCache {
		colName, ok := th.columnNameCache.Get(columnName)
		if ok {
			return colName.Unquoted, colName.UseQuoting
		}
	}
	if alphanumeric == 0 && utils.IsAlphanumeric(columnName) {
		alphanumeric = 1
	}
	unquoted, useQuoting = th.adaptSqlIdentifier(columnName, "column", th.columnNameFunc, alphanumeric == 1)
	if useCache {
		th.columnNameCache.Set(columnName, &bulkerlib.ColumnName{Unquoted: unquoted, UseQuoting: useQuoting})
	}
	return unquoted, useQuoting

}

// adaptSqlIdentifier adapts the given identifier to basic rules derived from the SQL standard and injection protection:
// - must only contain letters, numbers, underscores, hyphen, and spaces - all other characters are removed
// - identifiers are that use different character cases, space, hyphen or don't begin with letter or underscore get quoted
func (th *TableHelper) adaptSqlIdentifier(identifier string, kind string, idFunc IdentifierFunction, alphanumeric bool) (unquoted string, useQuoting bool) {
	useQuoting = th.useQuoting
	var result string
	if identifier == "" {
		result = "_unnamed"
	} else {
		result = identifier
		if !alphanumeric {
			result = sqlIdentifierUnsupportedCharacters.ReplaceAllString(identifier, "_")
			if result == "" || result == "_" {
				result = fmt.Sprintf("%s_%x", kind, utils.HashString(identifier))
				alphanumeric = true
			}
		}
		if alphanumeric && len(result) > th.maxIdentifierLength {
			result = result[:th.maxIdentifierLength]
		} else if !alphanumeric {
			runes := []rune(result)
			if len(runes) > th.maxIdentifierLength {
				result = string(runes[:th.maxIdentifierLength])
			}
			if idFunc != nil {
				alphanumeric = utils.IsAlphanumeric(result)
			}
		}
	}
	if idFunc != nil {
		result, useQuoting = idFunc(result, alphanumeric)
	}

	return result, useQuoting
}

func (th *TableHelper) TableName(tableName string) string {
	unquoted, _ := th.adaptSqlIdentifier(tableName, "table", th.tableNameFunc, utils.IsAlphanumeric(tableName))
	return unquoted
}

func (th *TableHelper) ColumnName(columnName string) string {
	unquoted, _ := th._adaptColumnName(columnName)
	return unquoted
}
