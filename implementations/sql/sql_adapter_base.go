package sql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jitsucom/bulker/base/errorj"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/objects"
	"github.com/jitsucom/bulker/base/timestamp"
	"github.com/jitsucom/bulker/implementations"
	"github.com/jitsucom/bulker/types"
	"strconv"
	"strings"
	"text/template"
)

const (
	createTableTemplate     = `CREATE %s TABLE %s (%s)`
	addColumnTemplate       = `ALTER TABLE %s ADD COLUMN %s`
	dropPrimaryKeyTemplate  = `ALTER TABLE %s DROP CONSTRAINT %s`
	alterPrimaryKeyTemplate = `ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s)`

	deleteQueryTemplate   = `DELETE FROM %s WHERE %s`
	selectQueryTemplate   = `SELECT %s FROM %s%s%s`
	insertQuery           = `INSERT INTO {{.TableName}}({{.Columns}}) VALUES ({{.Placeholders}})`
	insertFromSelectQuery = `INSERT INTO {{.TableTo}}({{.Columns}}) SELECT {{.Columns}} FROM {{.TableFrom}}`
	renameTableTemplate   = `ALTER TABLE %s%s RENAME TO %s`

	updateStatementTemplate = `UPDATE %s SET %s WHERE %s`
	dropTableTemplate       = `DROP TABLE %s%s`
	truncateTableTemplate   = `TRUNCATE TABLE %s`
)

var (
	insertQueryTemplate, _           = template.New("insertQuery").Parse(insertQuery)
	insertFromSelectQueryTemplate, _ = template.New("insertFromSelectQuery").Parse(insertFromSelectQuery)

	unmappedValue ValueMappingFunction = func(val any, valPresent bool, column SQLColumn) any {
		return val
	}
)

// DbConnectFunction function is used to connect to database
type DbConnectFunction[T any] func(config *T) (*sql.DB, error)

// ColumnDDLFunction generate column DDL for CREATE TABLE statement based on type (SQLColumn) and whether it is used for PK
type ColumnDDLFunction func(quotedName, name string, table *Table) string

// ValueMappingFunction maps object value to database value. For cases such default value substitution for null or missing values
type ValueMappingFunction func(value any, valuePresent bool, column SQLColumn) any

// TypeCastFunction wraps parameter(or placeholder) to a type cast expression if it is necessary (e.g. on types overrides)
type TypeCastFunction func(placeholder string, column SQLColumn) string

// ErrorAdapter is used to extract implementation specific payload and adapt to standard error
type ErrorAdapter func(error) error

type SQLAdapterBase[T any] struct {
	objects.ServiceBase
	typeId          string
	config          *T
	dataSource      *sql.DB
	queryLogger     *logging.QueryLogger
	batchFileFormat implementations.FileFormat
	temporaryTables bool

	typesMapping        map[types.DataType]string
	reverseTypesMapping map[string]types.DataType

	dbConnectFunction    DbConnectFunction[T]
	parameterPlaceholder ParameterPlaceholder
	typecastFunc         TypeCastFunction
	valueMappingFunction ValueMappingFunction
	_columnDDLFunc       ColumnDDLFunction
	tableHelper          TableHelper
	checkErrFunc         ErrorAdapter
}

func newSQLAdapterBase[T any](id string, typeId string, config *T, dbConnectFunction DbConnectFunction[T], dataTypes map[types.DataType][]string, queryLogger *logging.QueryLogger, typecastFunc TypeCastFunction, parameterPlaceholder ParameterPlaceholder, columnDDLFunc ColumnDDLFunction, valueMappingFunction ValueMappingFunction, checkErrFunc ErrorAdapter) (SQLAdapterBase[T], error) {
	s := SQLAdapterBase[T]{
		ServiceBase:          objects.NewServiceBase(id),
		typeId:               typeId,
		config:               config,
		dbConnectFunction:    dbConnectFunction,
		queryLogger:          queryLogger,
		parameterPlaceholder: parameterPlaceholder,
		typecastFunc:         typecastFunc,
		valueMappingFunction: valueMappingFunction,
		_columnDDLFunc:       columnDDLFunc,
		checkErrFunc:         checkErrFunc,
	}
	s.batchFileFormat = implementations.JSON
	var err error
	s.dataSource, err = dbConnectFunction(config)
	s.initTypes(dataTypes)
	return s, err
}

func (b *SQLAdapterBase[T]) initTypes(dataTypes map[types.DataType][]string) {
	typeMapping := make(map[types.DataType]string, len(dataTypes))
	reverseTypeMapping := make(map[string]types.DataType, len(dataTypes)+3)
	for dataType, postgresTypes := range dataTypes {
		for i, postgresType := range postgresTypes {
			if i == 0 {
				typeMapping[dataType] = postgresType
			}
			if dataType != types.UNKNOWN && dataType != types.JSON {
				reverseTypeMapping[postgresType] = dataType
			}
		}
	}
	b.typesMapping = typeMapping
	b.reverseTypesMapping = reverseTypeMapping
}

// Type returns Postgres type
func (b *SQLAdapterBase[T]) Type() string {
	return b.typeId
}

func (b *SQLAdapterBase[T]) GetBatchFileFormat() implementations.FileFormat {
	return b.batchFileFormat
}

func (b *SQLAdapterBase[T]) Ping(ctx context.Context) error {
	if b.dataSource != nil {
		err := b.dataSource.PingContext(ctx)
		if err != nil {
			dataSource, err := b.dbConnectFunction(b.config)
			if err == nil {
				_ = b.dataSource.Close()
				b.dataSource = dataSource
				return nil
			} else {
				return fmt.Errorf("failed to connect to %sю error: %v", b.typeId, err)
			}
		}
	} else {
		var err error
		b.dataSource, err = b.dbConnectFunction(b.config)
		if err != nil {
			return fmt.Errorf("failed to connect to %sю error: %v", b.typeId, err)
		}
	}
	return nil
}

// Close underlying sql.DB
func (b *SQLAdapterBase[T]) Close() error {
	if b.dataSource != nil {
		return b.dataSource.Close()
	}
	return nil
}

// OpenTx opens underline sql transaction and return wrapped instance
func (b *SQLAdapterBase[T]) openTx(ctx context.Context, sqlAdapter SQLAdapter) (*TxSQLAdapter, error) {
	tx, err := b.dataSource.BeginTx(ctx, nil)
	if err != nil {
		return nil, errorj.BeginTransactionError.Wrap(err, "failed to begin transaction")
	}

	return &TxSQLAdapter{sqlAdapter: sqlAdapter, tx: NewTxWrapper(b.Type(), tx, b.queryLogger, b.checkErrFunc)}, nil
}

func (b *SQLAdapterBase[T]) txOrDb(ctx context.Context) TxOrDB {
	txOrDb, ok := ctx.Value(ContextTransactionKey).(TxOrDB)
	if !ok {
		return NewDbWrapper(b.typeId, b.dataSource, b.queryLogger, b.checkErrFunc)
	}
	return txOrDb
}

func (b *SQLAdapterBase[T]) Select(ctx context.Context, tableName string, whenConditions *WhenConditions, orderBy []string) ([]map[string]any, error) {
	return b.selectFrom(ctx, selectQueryTemplate, tableName, "*", whenConditions, orderBy)
}
func (b *SQLAdapterBase[T]) selectFrom(ctx context.Context, statement string, tableName string, selectExpression string, whenConditions *WhenConditions, orderBy []string) ([]map[string]any, error) {
	quotedTableName := b.tableHelper.quotedTableName(tableName)
	whenCondition, values := b.ToWhenConditions(whenConditions, b.parameterPlaceholder, 0)
	if whenCondition != "" {
		whenCondition = " WHERE " + whenCondition
	}
	orderByClause := ""
	if len(orderBy) > 0 {
		quotedOrderByColumns := make([]string, len(orderBy))
		for i, col := range orderBy {
			quotedOrderByColumns[i] = fmt.Sprintf("%s asc", b.quotedColumnName(col))
		}
		orderByClause = " ORDER BY " + strings.Join(quotedOrderByColumns, ", ")
	}
	var rows *sql.Rows
	var err error
	query := fmt.Sprintf(statement, selectExpression, quotedTableName, whenCondition, orderByClause)
	if b.typeId == MySQLBulkerTypeId {
		//For MySQL using Prepared statement switches mySQL to use Binary protocol that preserves types information
		var stmt *sql.Stmt
		stmt, err = b.txOrDb(ctx).PrepareContext(ctx, query)
		if err == nil {
			defer func() {
				_ = stmt.Close()
			}()
			rows, err = stmt.QueryContext(ctx, values...)
		}
	} else {
		rows, err = b.txOrDb(ctx).QueryContext(ctx, query, values...)
	}
	if err != nil {
		return nil, errorj.SelectFromTableError.Wrap(err, "failed execute select").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Table:     quotedTableName,
				Statement: query,
				Values:    values,
			})
	}

	defer rows.Close()
	var result []map[string]any
	for rows.Next() {
		var row map[string]any
		row, err = rowToMap(rows)
		if err != nil {
			break
		}
		result = append(result, row)
	}

	if err == nil {
		err = rows.Err()
	}
	if err != nil {
		return nil, errorj.SelectFromTableError.Wrap(err, "failed read selected rows").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Table:     quotedTableName,
				Statement: query,
				Values:    values,
			})
	}

	return result, nil
}

func (b *SQLAdapterBase[T]) Count(ctx context.Context, tableName string, whenConditions *WhenConditions) (int, error) {
	res, err := b.selectFrom(ctx, selectQueryTemplate, tableName, "count(*) as jitsu_count", whenConditions, nil)
	if err != nil {
		return -1, err
	}
	if len(res) == 0 {
		return -1, fmt.Errorf("select count * gave no result")
	}
	scnt := res[0]["jitsu_count"]
	return strconv.Atoi(fmt.Sprint(scnt))
}

func (b *SQLAdapterBase[T]) Delete(ctx context.Context, tableName string, deleteConditions *WhenConditions) error {
	quotedTableName := b.quotedTableName(tableName)

	deleteCondition, values := b.ToWhenConditions(deleteConditions, b.parameterPlaceholder, 0)
	query := fmt.Sprintf(deleteQueryTemplate, quotedTableName, deleteCondition)

	if _, err := b.txOrDb(ctx).ExecContext(ctx, query, values...); err != nil {

		return errorj.DeleteFromTableError.Wrap(err, "failed to delete data").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Table:     quotedTableName,
				Statement: query,
			})
	}

	return nil
}

func (b *SQLAdapterBase[T]) Update(ctx context.Context, tableName string, object types.Object, whenConditions *WhenConditions) error {
	quotedTableName := b.quotedTableName(tableName)

	updateCondition, updateValues := b.ToWhenConditions(whenConditions, b.parameterPlaceholder, len(object))

	columns := make([]string, len(object), len(object))
	values := make([]any, len(object)+len(updateValues), len(object)+len(updateValues))
	i := 0
	for name, value := range object {
		columns[i] = b.quotedColumnName(name) + "= " + b.parameterPlaceholder(i+1, name) //$0 - wrong
		values[i] = value
		i++
	}
	for a := 0; a < len(updateValues); a++ {
		values[i+a] = updateValues[a]
	}

	statement := fmt.Sprintf(updateStatementTemplate, quotedTableName, strings.Join(columns, ", "), updateCondition)

	if _, err := b.txOrDb(ctx).ExecContext(ctx, statement, values...); err != nil {

		return errorj.UpdateError.Wrap(err, "failed to update").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Table:     quotedTableName,
				Statement: statement,
				Values:    values,
			})
	}

	return nil
}

func (b *SQLAdapterBase[T]) DropTable(ctx context.Context, tableName string, ifExists bool) error {
	quotedTableName := b.quotedTableName(tableName)

	ifExs := ""
	if ifExists {
		ifExs = "IF EXISTS "
	}
	query := fmt.Sprintf(dropTableTemplate, ifExs, quotedTableName)

	if _, err := b.txOrDb(ctx).ExecContext(ctx, query); err != nil {

		return errorj.DropError.Wrap(err, "failed to drop table").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Table:     quotedTableName,
				Statement: query,
			})
	}

	return nil
}

// TruncateTable deletes all records in tableName table
func (b *SQLAdapterBase[T]) TruncateTable(ctx context.Context, tableName string) error {
	quotedTableName := b.quotedTableName(tableName)

	statement := fmt.Sprintf(truncateTableTemplate, quotedTableName)
	if _, err := b.txOrDb(ctx).ExecContext(ctx, statement); err != nil {
		return errorj.TruncateError.Wrap(err, "failed to truncate table").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Table:     quotedTableName,
				Statement: statement,
			})
	}

	return nil
}

type QueryPayload struct {
	TableName      string
	Columns        string
	Placeholders   string
	PrimaryKeyName string
	UpdateSet      string

	TableTo        string
	TableFrom      string
	JoinConditions string
	SourceColumns  string
}

func (b *SQLAdapterBase[T]) insert(ctx context.Context, table *Table, objects []types.Object) error {
	return b.insertOrMerge(ctx, table, objects, nil)
}

// plainInsert inserts provided object into Snowflake
func (b *SQLAdapterBase[T]) insertOrMerge(ctx context.Context, table *Table, objects []types.Object, mergeQuery *template.Template) error {
	quotedTableName := b.quotedTableName(table.Name)

	columns := table.SortedColumnNames()
	columnNames := make([]string, len(columns))
	placeholders := make([]string, len(columns))
	values := make([]any, len(columns))
	updateColumns := make([]string, len(columns))

	for i, name := range columns {
		if mergeQuery != nil {
			updateColumns[i] = fmt.Sprintf(`%s=%s`, b.quotedColumnName(name), b.typecastFunc(b.parameterPlaceholder(i+1, b.quotedColumnName(name)), table.Columns[name]))
		}
		columnNames[i] = b.quotedColumnName(name)
		placeholders[i] = b.typecastFunc(b.parameterPlaceholder(i+1, name), table.Columns[name])
	}

	insertPayload := QueryPayload{
		TableName:      quotedTableName,
		Columns:        strings.Join(columnNames, ", "),
		Placeholders:   strings.Join(placeholders, ", "),
		PrimaryKeyName: table.PrimaryKeyName,
		UpdateSet:      strings.Join(updateColumns, ","),
	}
	buf := strings.Builder{}
	template := insertQueryTemplate
	if mergeQuery != nil {
		template = mergeQuery
	}
	err := template.Execute(&buf, insertPayload)
	if err != nil {
		return errorj.ExecuteInsertError.Wrap(err, "failed to build query from template")
	}
	statement := buf.String()
	for _, object := range objects {
		for i, name := range columns {
			sqlColumn := table.Columns[name]
			value, valuePresent := object[name]
			values[i] = b.valueMappingFunction(value, valuePresent, sqlColumn)
		}
		if mergeQuery != nil && b.parameterPlaceholder(1, "dummy") == "?" {
			// Without positional parameters we need to duplicate values for placeholders in UPDATE part
			values = append(values, values...)
		}
		_, err := b.txOrDb(ctx).ExecContext(ctx, statement, values...)
		if err != nil {
			return errorj.ExecuteInsertError.Wrap(err, "failed to execute single insert").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Table:       quotedTableName,
					PrimaryKeys: table.GetPKFields(),
					Statement:   statement,
					Values:      values,
				})
		}
	}

	return nil
}

func (b *SQLAdapterBase[T]) copy(ctx context.Context, targetTable *Table, sourceTable *Table) error {
	return b.copyOrMerge(ctx, targetTable, sourceTable, nil, "")
}

func (b *SQLAdapterBase[T]) copyOrMerge(ctx context.Context, targetTable *Table, sourceTable *Table, mergeQuery *template.Template, sourceAlias string) error {
	quotedTargetTableName := b.quotedTableName(targetTable.Name)
	quotedSourceTableName := b.quotedTableName(sourceTable.Name)

	//insert from select
	columns := sourceTable.SortedColumnNames()
	columnNames := make([]string, len(columns))
	for i, name := range columns {
		columnNames[i] = b.quotedColumnName(name)
	}
	updateColumns := make([]string, len(columns))
	insertColumns := make([]string, len(columns))
	var joinConditions []string
	if mergeQuery != nil {
		for i, name := range columns {
			updateColumns[i] = fmt.Sprintf(`%s=%s.%s`, b.quotedColumnName(name), sourceAlias, b.quotedColumnName(name))
			insertColumns[i] = b.typecastFunc(fmt.Sprintf(`%s.%s`, sourceAlias, b.quotedColumnName(name)), targetTable.Columns[name])
		}
		for pkField := range targetTable.PKFields {
			joinConditions = append(joinConditions, fmt.Sprintf("T.%s = %s.%s", b.quotedColumnName(pkField), sourceAlias, b.quotedColumnName(pkField)))
		}
	}
	insertPayload := QueryPayload{
		TableTo:        quotedTargetTableName,
		TableFrom:      quotedSourceTableName,
		Columns:        strings.Join(columnNames, ","),
		PrimaryKeyName: targetTable.PrimaryKeyName,
		JoinConditions: strings.Join(joinConditions, " AND "),
		SourceColumns:  strings.Join(insertColumns, ", "),
		UpdateSet:      strings.Join(updateColumns, ","),
	}
	buf := strings.Builder{}
	template := insertFromSelectQueryTemplate
	if mergeQuery != nil {
		template = mergeQuery
	}
	err := template.Execute(&buf, insertPayload)
	if err != nil {
		return errorj.ExecuteInsertError.Wrap(err, "failed to build query from template")
	}
	statement := buf.String()

	if _, err := b.txOrDb(ctx).ExecContext(ctx, statement); err != nil {

		return errorj.BulkMergeError.Wrap(err, "failed to bulk insert").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Table:       quotedTargetTableName,
				PrimaryKeys: targetTable.GetPKFields(),
				Statement:   statement,
			})
	}
	return nil
}

// CreateTable create table columns and pk key
// override input table sql type with configured cast type
// make fields from Table PkFields - 'not null'
func (b *SQLAdapterBase[T]) CreateTable(ctx context.Context, schemaToCreate *Table) error {
	quotedTableName := b.quotedTableName(schemaToCreate.Name)

	columns := schemaToCreate.SortedColumnNames()
	columnsDDL := make([]string, len(columns))
	for i, columnName := range columns {
		columnsDDL[i] = b.columnDDL(columnName, schemaToCreate)
	}
	temporary := ""
	if b.temporaryTables && schemaToCreate.Temporary {
		temporary = "TEMPORARY"
	}

	query := fmt.Sprintf(createTableTemplate, temporary, quotedTableName, strings.Join(columnsDDL, ", "))

	if _, err := b.txOrDb(ctx).ExecContext(ctx, query); err != nil {
		return errorj.CreateTableError.Wrap(err, "failed to create table").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Table:       quotedTableName,
				PrimaryKeys: schemaToCreate.GetPKFields(),
				Statement:   query,
			})
	}

	if err := b.createPrimaryKey(ctx, schemaToCreate); err != nil {
		return err
	}

	return nil
}

// PatchTableSchema alter table with columns (if not empty)
// recreate primary key (if not empty) or delete primary key if Table.DeletePkFields is true
func (b *SQLAdapterBase[T]) PatchTableSchema(ctx context.Context, patchTable *Table) error {
	quotedTableName := b.quotedTableName(patchTable.Name)

	columns := patchTable.SortedColumnNames()

	//patch columns
	for _, columnName := range columns {
		columnDDL := b.columnDDL(columnName, patchTable)
		query := fmt.Sprintf(addColumnTemplate, quotedTableName, columnDDL)

		if _, err := b.txOrDb(ctx).ExecContext(ctx, query); err != nil {
			return errorj.PatchTableError.Wrap(err, "failed to patch table").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Table:       quotedTableName,
					PrimaryKeys: patchTable.GetPKFields(),
					Statement:   query,
				})
		}
	}

	//patch primary keys - delete old
	if patchTable.DeletePkFields {
		err := b.deletePrimaryKey(ctx, patchTable)
		if err != nil {
			return err
		}
	}

	//patch primary keys - create new
	if len(patchTable.PKFields) > 0 {
		err := b.createPrimaryKey(ctx, patchTable)
		if err != nil {
			return err
		}
	}

	return nil
}

// createPrimaryKey create primary key constraint
func (b *SQLAdapterBase[T]) createPrimaryKey(ctx context.Context, table *Table) error {
	if len(table.PKFields) == 0 {
		return nil
	}

	quotedTableName := b.quotedTableName(table.Name)

	columnNames := make([]string, len(table.PKFields))
	for i, column := range table.GetPKFields() {
		columnNames[i] = b.quotedColumnName(column)
	}

	statement := fmt.Sprintf(alterPrimaryKeyTemplate,
		quotedTableName, table.PrimaryKeyName, strings.Join(columnNames, ","))

	if _, err := b.txOrDb(ctx).ExecContext(ctx, statement); err != nil {
		return errorj.CreatePrimaryKeysError.Wrap(err, "failed to set primary key").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Table:       quotedTableName,
				PrimaryKeys: table.GetPKFields(),
				Statement:   statement,
			})
	}

	return nil
}

// delete primary key
func (b *SQLAdapterBase[T]) deletePrimaryKey(ctx context.Context, table *Table) error {
	quotedTableName := b.quotedTableName(table.Name)

	query := fmt.Sprintf(dropPrimaryKeyTemplate, quotedTableName, table.PrimaryKeyName)

	if _, err := b.txOrDb(ctx).ExecContext(ctx, query); err != nil {
		return errorj.DeletePrimaryKeysError.Wrap(err, "failed to delete primary key").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Table:       quotedTableName,
				PrimaryKeys: table.GetPKFields(),
				Statement:   query,
			})
	}

	return nil
}

func (b *SQLAdapterBase[T]) renameTable(ctx context.Context, ifExists bool, tableName, newTableName string) error {
	quotedTableName := b.quotedTableName(tableName)
	quotedNewTableName := b.quotedTableName(newTableName)

	ifExs := ""
	if ifExists {
		ifExs = "IF EXISTS "
	}
	query := fmt.Sprintf(renameTableTemplate, ifExs, quotedTableName, quotedNewTableName)

	if _, err := b.txOrDb(ctx).ExecContext(ctx, query); err != nil {
		return errorj.RenameError.Wrap(err, "failed to rename table").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Table:     quotedTableName,
				Statement: query,
			})
	}

	return nil
}

func (b *SQLAdapterBase[T]) ReplaceTable(ctx context.Context, targetTableName string, replacementTable *Table, dropOldTable bool) (err error) {
	tmpTable := "deprecated_" + targetTableName + timestamp.Now().Format("_20060102_150405")
	err1 := b.renameTable(ctx, true, targetTableName, tmpTable)
	err = b.renameTable(ctx, false, replacementTable.Name, targetTableName)
	if dropOldTable && err1 == nil && err == nil {
		return b.DropTable(ctx, tmpTable, true)
	}
	return
}

func (b *SQLAdapterBase[T]) TableHelper() *TableHelper {
	return &b.tableHelper
}

func (b *SQLAdapterBase[T]) columnDDL(name string, table *Table) string {
	quoted, unquoted := b.tableHelper.adaptColumnName(name)
	return b._columnDDLFunc(quoted, unquoted, table)
}

// quotedColumnName adapts table name to sql identifier rules of database and quotes accordingly (if needed)
func (b *SQLAdapterBase[T]) quotedTableName(tableName string) string {
	return b.tableHelper.quotedTableName(tableName)
}

// quotedColumnName adapts column name to sql identifier rules of database and quotes accordingly (if needed)
func (b *SQLAdapterBase[T]) quotedColumnName(columnName string) string {
	return b.tableHelper.quotedColumnName(columnName)
}

func (b *SQLAdapterBase[T]) ColumnName(identifier string) string {
	return b.tableHelper.ColumnName(identifier)
}

func (b *SQLAdapterBase[T]) TableName(identifier string) string {
	return b.tableHelper.TableName(identifier)
}

// ToWhenConditions generates WHEN clause for SQL query based on provided WhenConditions
//
// paramExpression - SQLParameterExpression function that produce parameter placeholder for parametrized query,
// depending on database can be: IndexParameterPlaceholder, QuestionMarkParameterPlaceholder, NamedParameterPlaceholder
//
// valuesShift - for parametrized query index of first when clause value in all values provided to query
// (for UPDATE queries 'valuesShift' = len(object fields))
func (b *SQLAdapterBase[T]) ToWhenConditions(conditions *WhenConditions, paramExpression ParameterPlaceholder, valuesShift int) (string, []any) {
	if conditions == nil {
		return "", []any{}
	}
	var queryConditions []string
	var values []any

	for i, condition := range conditions.Conditions {
		switch strings.ToLower(condition.Clause) {
		case "is null":
		case "is not null":
			queryConditions = append(queryConditions, b.quotedColumnName(condition.Field)+" "+condition.Clause)
		default:
			queryConditions = append(queryConditions, b.quotedColumnName(condition.Field)+" "+condition.Clause+" "+paramExpression(i+valuesShift+1, condition.Field))
			values = append(values, types.ReformatValue(condition.Value))
		}
	}

	return strings.Join(queryConditions, " "+conditions.JoinCondition+" "), values
}

func (b *SQLAdapterBase[T]) GetSQLType(dataType types.DataType) (string, bool) {
	v, ok := b.typesMapping[dataType]
	return v, ok
}

func (b *SQLAdapterBase[T]) GetDataType(sqlType string) (types.DataType, bool) {
	v, ok := b.reverseTypesMapping[sqlType]
	return v, ok
}

func checkNotExistErr(err error) error {
	if notExistRegexp.MatchString(err.Error()) {
		return ErrTableNotExist
	}
	return err
}
