package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/jitsucom/bulker/base/errorj"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/timestamp"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/types"
	"github.com/lib/pq"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

func init() {
	bulker.RegisterBulker(PostgresBulkerTypeId, NewPostgres)
}

const (
	PostgresBulkerTypeId = "postgres"

	tableSchemaQuery = `SELECT 
 							pg_attribute.attname AS name,
    						pg_catalog.format_type(pg_attribute.atttypid,pg_attribute.atttypmod) AS column_type
						FROM pg_attribute
         					JOIN pg_class ON pg_class.oid = pg_attribute.attrelid
         					LEFT JOIN pg_attrdef pg_attrdef ON pg_attrdef.adrelid = pg_class.oid AND pg_attrdef.adnum = pg_attribute.attnum
         					LEFT JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
         					LEFT JOIN pg_constraint ON pg_constraint.conrelid = pg_class.oid AND pg_attribute.attnum = ANY (pg_constraint.conkey)
						WHERE pg_class.relkind = 'r'::char
  							AND  pg_namespace.nspname = $1
  							AND pg_class.relname = $2
  							AND pg_attribute.attnum > 0`
	primaryKeyFieldsQuery = `SELECT tco.constraint_name as constraint_name,
       kcu.column_name as key_column
FROM information_schema.table_constraints tco
         JOIN information_schema.key_column_usage kcu
              ON kcu.constraint_name = tco.constraint_name
                  AND kcu.constraint_schema = tco.constraint_schema
                  AND kcu.constraint_name = tco.constraint_name
WHERE tco.constraint_type = 'PRIMARY KEY' AND 
      kcu.table_schema = $1 AND
      kcu.table_name = $2`
	createDbSchemaIfNotExistsTemplate = `CREATE SCHEMA IF NOT EXISTS "%s"`
	addColumnTemplate                 = `ALTER TABLE "%s"."%s" ADD COLUMN %s`
	dropPrimaryKeyTemplate            = `ALTER TABLE "%s"."%s" DROP CONSTRAINT %s`
	alterPrimaryKeyTemplate           = `ALTER TABLE "%s"."%s" ADD CONSTRAINT %s PRIMARY KEY (%s)`
	createTableTemplate               = `CREATE TABLE "%s"."%s" (%s)`
	insertTemplate                    = `INSERT INTO "%s"."%s" (%s) VALUES %s`
	mergeTemplate                     = `INSERT INTO "%s"."%s"(%s) VALUES %s ON CONFLICT ON CONSTRAINT %s DO UPDATE set %s;`
	bulkCopyTemplate                  = `INSERT INTO "%s"."%s"(%s) SELECT %s FROM "%s"."%s" `
	bulkMergeTemplate                 = `INSERT INTO "%s"."%s"(%s) SELECT %s FROM "%s"."%s" ON CONFLICT ON CONSTRAINT %s DO UPDATE SET %s`
	bulkMergePrefix                   = `excluded`
	deleteQueryTemplate               = `DELETE FROM "%s"."%s" WHERE %s`
	selectQueryTemplate               = `SELECT %s FROM "%s"."%s"%s`

	updateStatement   = `UPDATE "%s"."%s" SET %s WHERE %s`
	dropTableTemplate = `DROP TABLE %s"%s"."%s"`

	renameTableTemplate           = `ALTER TABLE "%s"."%s" RENAME TO "%s"`
	postgresTruncateTableTemplate = `TRUNCATE "%s"."%s"`

	//TODO: replace values limit
	PostgresValuesLimit = 65535 // this is a limitation of parameters one can pass as query values. If more parameters are passed, error is returned
)

var (
	SchemaToPostgres = map[types.DataType]string{
		types.STRING:    "text",
		types.INT64:     "bigint",
		types.FLOAT64:   "double precision",
		types.TIMESTAMP: "timestamp",
		types.BOOL:      "boolean",
		types.UNKNOWN:   "text",
	}
)

// Postgres is adapter for creating,patching (schema or table), inserting data to postgres
type Postgres struct {
	config      *DataSourceConfig
	dataSource  *sql.DB
	dbWrapper   TxOrDB
	queryLogger *logging.QueryLogger
}

// NewPostgres return configured Postgres bulker.Bulker instance
func NewPostgres(bulkerConfig bulker.Config) (bulker.Bulker, error) {
	config := &DataSourceConfig{}
	if err := utils.ParseObject(bulkerConfig.DestinationConfig, config); err != nil {
		return nil, fmt.Errorf("failed to parse destination config: %w", err)
	}

	connectionString := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s ",
		config.Host, config.Port, config.Db, config.Username, config.Password)
	logging.Info("connecting to postgres: %s", connectionString)
	//concat provided connection parameters
	for k, v := range config.Parameters {
		connectionString += k + "=" + v + " "
	}
	dataSource, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, err
	}

	if err := dataSource.Ping(); err != nil {
		dataSource.Close()
		return nil, err
	}

	//set default value
	dataSource.SetConnMaxLifetime(10 * time.Minute)

	p := &Postgres{config: config, dataSource: dataSource, queryLogger: logging.NewQueryLogger(bulkerConfig.Id, os.Stderr, os.Stderr)}
	p.dbWrapper = NewDbWrapper(PostgresBulkerTypeId, dataSource, p.queryLogger, checkErr)

	return p, nil
}

func (p *Postgres) CreateStream(id, tableName string, mode bulker.BulkMode, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	switch mode {
	case bulker.AutoCommit:
		return newAutoCommitStream(id, p, p.dataSource, tableName, streamOptions...)
	case bulker.Transactional:
		return newTransactionalStream(id, p, p.dataSource, tableName, streamOptions...)
	case bulker.ReplaceTable:
		return newReplaceTableStream(id, p, p.dataSource, tableName, streamOptions...)
	case bulker.ReplacePartition:
		return newReplacePartitionStream(id, p, p.dataSource, tableName, streamOptions...)
	}
	return nil, fmt.Errorf("unsupported bulk mode: %s", mode)
}

// Type returns Postgres type
func (p *Postgres) Type() string {
	return PostgresBulkerTypeId
}

func (p *Postgres) GetConfig() *DataSourceConfig {
	return p.config
}

func (p *Postgres) GetTypesMapping() map[types.DataType]string {
	return SchemaToPostgres
}

// OpenTx opens underline sql transaction and return wrapped instance
func (p *Postgres) OpenTx(ctx context.Context) (*TxSQLAdapter, error) {
	tx, err := p.dataSource.BeginTx(ctx, nil)
	if err != nil {
		return nil, errorj.BeginTransactionError.Wrap(err, "failed to begin transaction").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema: p.config.Schema,
			})
	}

	return &TxSQLAdapter{sqlAdapter: p, tx: NewTxWrapper(p.Type(), tx, p.queryLogger, checkErr)}, nil
}

// InitDatabase creates database schema instance if doesn't exist
func (p *Postgres) InitDatabase(ctx context.Context) error {
	query := fmt.Sprintf(createDbSchemaIfNotExistsTemplate, p.config.Schema)

	if _, err := p.txOrDb(ctx).ExecContext(ctx, query); err != nil {
		return errorj.CreateSchemaError.Wrap(err, "failed to create db schema").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:    p.config.Schema,
				Statement: query,
			})
	}

	return nil
}

// GetTableSchema returns table (name,columns with name and types) representation wrapped in Table struct
func (p *Postgres) GetTableSchema(ctx context.Context, tableName string) (*Table, error) {
	table, err := p.getTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	//don't select primary keys of non-existent table
	if len(table.Columns) == 0 {
		return table, nil
	}

	primaryKeyName, pkFields, err := p.getPrimaryKey(ctx, tableName)
	if err != nil {
		return nil, err
	}

	table.PKFields = pkFields
	table.PrimaryKeyName = primaryKeyName

	jitsuPrimaryKeyName := BuildConstraintName(table.Name)
	if primaryKeyName != "" && primaryKeyName != jitsuPrimaryKeyName {
		logging.Warnf("table: %s has a custom primary key with name: %s that isn't managed by Jitsu. Custom primary key will be used in rows deduplication and updates. primary_key_fields configuration provided in Jitsu config will be ignored.", table.Name, primaryKeyName)
	}
	return table, nil
}

func (p *Postgres) getTable(ctx context.Context, tableName string) (*Table, error) {
	table := &Table{Name: tableName, Columns: map[string]SQLColumn{}, PKFields: utils.Set[string]{}}
	rows, err := p.txOrDb(ctx).QueryContext(ctx, tableSchemaQuery, p.config.Schema, tableName)
	if err != nil {
		return nil, errorj.GetTableError.Wrap(err, "failed to get table columns").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:      p.config.Schema,
				Table:       table.Name,
				PrimaryKeys: table.GetPKFields(),
				Statement:   tableSchemaQuery,
				Values:      []any{p.config.Schema, tableName},
			})
	}

	defer rows.Close()
	for rows.Next() {
		var columnName, columnPostgresType string
		if err := rows.Scan(&columnName, &columnPostgresType); err != nil {
			return nil, errorj.GetTableError.Wrap(err, "failed to scan result").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Schema:      p.config.Schema,
					Table:       table.Name,
					PrimaryKeys: table.GetPKFields(),
					Statement:   tableSchemaQuery,
					Values:      []any{p.config.Schema, tableName},
				})
		}
		if columnPostgresType == "-" {
			//skip dropped postgres field
			continue
		}
		table.Columns[columnName] = SQLColumn{Type: columnPostgresType}
	}

	if err := rows.Err(); err != nil {
		return nil, errorj.GetTableError.Wrap(err, "failed read last row").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:      p.config.Schema,
				Table:       table.Name,
				PrimaryKeys: table.GetPKFields(),
				Statement:   tableSchemaQuery,
				Values:      []any{p.config.Schema, tableName},
			})
	}

	return table, nil
}

// CreateTable create table columns and pk key
// override input table sql type with configured cast type
// make fields from Table PkFields - 'not null'
func (p *Postgres) CreateTable(ctx context.Context, schemaToCreate *Table) error {
	var columnsDDL []string
	pkFields := schemaToCreate.GetPKFieldsSet()
	for _, columnName := range schemaToCreate.SortedColumnNames() {
		column := schemaToCreate.Columns[columnName]
		columnsDDL = append(columnsDDL, p.columnDDL(columnName, column, pkFields))
	}

	//sorting columns asc
	sort.Strings(columnsDDL)
	query := fmt.Sprintf(createTableTemplate, p.config.Schema, schemaToCreate.Name, strings.Join(columnsDDL, ", "))

	if _, err := p.txOrDb(ctx).ExecContext(ctx, query); err != nil {
		return errorj.CreateTableError.Wrap(err, "failed to create table").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:      p.config.Schema,
				Table:       schemaToCreate.Name,
				PrimaryKeys: schemaToCreate.GetPKFields(),
				Statement:   query,
			})
	}

	if err := p.createPrimaryKeyInTransaction(ctx, schemaToCreate); err != nil {
		return err
	}

	return nil
}

// PatchTableSchema alter table with columns (if not empty)
// recreate primary key (if not empty) or delete primary key if Table.DeletePkFields is true
func (p *Postgres) PatchTableSchema(ctx context.Context, patchTable *Table) error {
	pkFields := patchTable.GetPKFieldsSet()
	//patch columns
	for _, columnName := range patchTable.SortedColumnNames() {
		column := patchTable.Columns[columnName]
		columnDDL := p.columnDDL(columnName, column, pkFields)
		query := fmt.Sprintf(addColumnTemplate, p.config.Schema, patchTable.Name, columnDDL)

		if _, err := p.txOrDb(ctx).ExecContext(ctx, query); err != nil {
			return errorj.PatchTableError.Wrap(err, "failed to patch table").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Schema:      p.config.Schema,
					Table:       patchTable.Name,
					PrimaryKeys: patchTable.GetPKFields(),
					Statement:   query,
				})
		}
	}

	//patch primary keys - delete old
	if patchTable.DeletePkFields {
		err := p.deletePrimaryKeyInTransaction(ctx, patchTable)
		if err != nil {
			return err
		}
	}

	//patch primary keys - create new
	if len(patchTable.PKFields) > 0 {
		err := p.createPrimaryKeyInTransaction(ctx, patchTable)
		if err != nil {
			return err
		}
	}

	return nil
}

// createPrimaryKeyInTransaction create primary key constraint
func (p *Postgres) createPrimaryKeyInTransaction(ctx context.Context, table *Table) error {
	if len(table.PKFields) == 0 {
		return nil
	}

	var quotedColumnNames []string
	for _, column := range table.GetPKFields() {
		quotedColumnNames = append(quotedColumnNames, fmt.Sprintf(`"%s"`, column))
	}

	statement := fmt.Sprintf(alterPrimaryKeyTemplate,
		p.config.Schema, table.Name, table.PrimaryKeyName, strings.Join(quotedColumnNames, ","))

	if _, err := p.txOrDb(ctx).ExecContext(ctx, statement); err != nil {
		return errorj.CreatePrimaryKeysError.Wrap(err, "failed to set primary key").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:      p.config.Schema,
				Table:       table.Name,
				PrimaryKeys: table.GetPKFields(),
				Statement:   statement,
			})
	}

	return nil
}

// delete primary key
func (p *Postgres) deletePrimaryKeyInTransaction(ctx context.Context, table *Table) error {
	query := fmt.Sprintf(dropPrimaryKeyTemplate, p.config.Schema, table.Name, table.PrimaryKeyName)

	if _, err := p.txOrDb(ctx).ExecContext(ctx, query); err != nil {
		return errorj.DeletePrimaryKeysError.Wrap(err, "failed to delete primary key").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:      p.config.Schema,
				Table:       table.Name,
				PrimaryKeys: table.GetPKFields(),
				Statement:   query,
			})
	}

	return nil
}

func (p *Postgres) ReplaceTable(ctx context.Context, originalTable, replacementTable string, dropOldTable bool) (err error) {
	tmpTable := "deprecated_" + originalTable + timestamp.Now().Format("_20060102_150405")
	err1 := p.renameTableInTransaction(ctx, true, originalTable, tmpTable)
	err = p.renameTableInTransaction(ctx, false, replacementTable, originalTable)
	if dropOldTable && err1 == nil && err == nil {
		return p.DropTable(ctx, tmpTable, true)
	}
	return
}

// Update one record in Postgres
func (p *Postgres) Update(ctx context.Context, tableName string, object types.Object, whenConditions *WhenConditions) error {
	updateCondition, updateValues := p.toWhenConditions(whenConditions)

	columns := make([]string, len(object), len(object))
	values := make([]any, len(object)+len(updateValues), len(object)+len(updateValues))
	i := 0
	for name, value := range object {
		columns[i] = name + "= $" + strconv.Itoa(i+1) //$0 - wrong
		values[i] = value
		i++
	}
	for a := 0; a < len(updateValues); a++ {
		values[i+a] = updateValues[a]
	}

	statement := fmt.Sprintf(updateStatement, p.config.Schema, tableName, strings.Join(columns, ", "), updateCondition)

	if _, err := p.txOrDb(ctx).ExecContext(ctx, statement, values...); err != nil {

		return errorj.UpdateError.Wrap(err, "failed to update").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:    p.config.Schema,
				Table:     tableName,
				Statement: statement,
				Values:    values,
			})
	}

	return nil
}

func (p *Postgres) Insert(ctx context.Context, table *Table, merge bool, objects []types.Object) error {
	var placeholdersBuilder strings.Builder
	var headerWithoutQuotes []string
	for _, name := range table.SortedColumnNames() {
		headerWithoutQuotes = append(headerWithoutQuotes, name)
	}
	valuesAmount := len(objects) * len(table.Columns)
	maxValues := valuesAmount
	if maxValues > PostgresValuesLimit {
		maxValues = PostgresValuesLimit
	}
	valueArgs := make([]any, 0, maxValues)
	placeholdersCounter := 1
	operation := 0
	operations := int(math.Max(1, float64(valuesAmount)/float64(PostgresValuesLimit)))
	for _, row := range objects {
		// if number of values exceeds limit, we have to execute insert query on processed rows
		if len(valueArgs)+len(headerWithoutQuotes) > PostgresValuesLimit {
			operation++
			if err := p.executeInsertInTransaction(ctx, table, merge, headerWithoutQuotes, removeLastComma(placeholdersBuilder.String()), valueArgs); err != nil {
				return errorj.Decorate(err, "middle insert %d of %d in batch", operation, operations)
			}

			placeholdersBuilder.Reset()
			placeholdersCounter = 1
			valueArgs = make([]any, 0, maxValues)
		}

		_, _ = placeholdersBuilder.WriteString("(")

		for i, column := range headerWithoutQuotes {
			value, _ := row[column]
			//replace zero byte character for text fields
			if table.Columns[column].Type == "text" {
				if v, ok := value.(string); ok {
					if strings.ContainsRune(v, '\u0000') {
						value = strings.ReplaceAll(v, "\u0000", "")
					}
				}
			}
			valueArgs = append(valueArgs, value)
			castClause := p.getCastClause(column, table.Columns[column])

			_, _ = placeholdersBuilder.WriteString("$" + strconv.Itoa(placeholdersCounter) + castClause)

			if i < len(headerWithoutQuotes)-1 {
				_, _ = placeholdersBuilder.WriteString(",")
			}
			placeholdersCounter++
		}
		_, _ = placeholdersBuilder.WriteString("),")
	}

	if len(valueArgs) > 0 {
		operation++
		if err := p.executeInsertInTransaction(ctx, table, merge, headerWithoutQuotes, removeLastComma(placeholdersBuilder.String()), valueArgs); err != nil {
			return errorj.Decorate(err, "last insert %d of %d in batch", operation, operations)
		}
	}

	return nil
}

func (p *Postgres) CopyTables(ctx context.Context, targetTable *Table, sourceTable *Table, merge bool) error {
	//insert from select
	var setValues []string
	var headerWithQuotes []string
	for _, name := range targetTable.SortedColumnNames() {
		if merge {
			setValues = append(setValues, fmt.Sprintf(`"%s"=%s."%s"`, name, bulkMergePrefix, name))
		}
		headerWithQuotes = append(headerWithQuotes, fmt.Sprintf(`"%s"`, name))
	}
	insertFromSelectStatement := ""
	if merge && len(targetTable.PKFields) > 0 {
		insertFromSelectStatement = fmt.Sprintf(bulkMergeTemplate, p.config.Schema, targetTable.Name, strings.Join(headerWithQuotes, ", "), strings.Join(headerWithQuotes, ", "), p.config.Schema, sourceTable.Name, targetTable.PrimaryKeyName, strings.Join(setValues, ", "))
	} else {
		insertFromSelectStatement = fmt.Sprintf(bulkCopyTemplate, p.config.Schema, targetTable.Name, strings.Join(headerWithQuotes, ", "), strings.Join(headerWithQuotes, ", "), p.config.Schema, sourceTable.Name)
	}

	if _, err := p.txOrDb(ctx).ExecContext(ctx, insertFromSelectStatement); err != nil {

		return errorj.BulkMergeError.Wrap(err, "failed to bulk merge").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:      p.config.Schema,
				Table:       targetTable.Name,
				PrimaryKeys: targetTable.GetPKFields(),
				Statement:   insertFromSelectStatement,
			})
	}
	return nil
}

func (p *Postgres) DropTable(ctx context.Context, tableName string, ifExists bool) error {
	ifExs := ""
	if ifExists {
		ifExs = "IF EXISTS "
	}
	query := fmt.Sprintf(dropTableTemplate, ifExs, p.config.Schema, tableName)

	if _, err := p.txOrDb(ctx).ExecContext(ctx, query); err != nil {

		return errorj.DropError.Wrap(err, "failed to drop table").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:    p.config.Schema,
				Table:     tableName,
				Statement: query,
			})
	}

	return nil
}

func (p *Postgres) renameTableInTransaction(ctx context.Context, ifExists bool, tableName, newTableName string) error {
	if ifExists {
		row := p.txOrDb(ctx).QueryRowContext(ctx, fmt.Sprintf(`SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s')`, p.config.Schema, tableName))
		exists := false
		err := row.Scan(&exists)
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}
	}
	query := fmt.Sprintf(renameTableTemplate, p.config.Schema, tableName, newTableName)

	if _, err := p.txOrDb(ctx).ExecContext(ctx, query); err != nil {

		return errorj.RenameError.Wrap(err, "failed to rename table").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:    p.config.Schema,
				Table:     tableName,
				Statement: query,
			})
	}

	return nil
}

func (p *Postgres) Select(ctx context.Context, tableName string, whenConditions *WhenConditions) ([]map[string]any, error) {
	return p.selectFrom(ctx, tableName, "*", whenConditions)
}
func (p *Postgres) selectFrom(ctx context.Context, tableName string, selectExpression string, deleteConditions *WhenConditions) ([]map[string]any, error) {
	whenCondition, values := p.toWhenConditions(deleteConditions)
	if whenCondition != "" {
		whenCondition = " WHERE " + whenCondition
	}
	query := fmt.Sprintf(selectQueryTemplate, selectExpression, p.config.Schema, tableName, whenCondition)

	rows, err := p.txOrDb(ctx).QueryContext(ctx, query, values...)
	if err != nil {
		return nil, errorj.SelectFromTableError.Wrap(err, "failed execute select").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:    p.config.Schema,
				Table:     tableName,
				Statement: query,
				Values:    values,
			})
	}

	defer rows.Close()
	var columns []string
	var result []map[string]any
	for rows.Next() {
		if columns == nil {
			columns, err = rows.Columns()
			if err != nil {
				break
			}
		}
		data := make([]any, len(columns))
		for i := range columns {
			data[i] = &ColumnScanner{}
		}
		if err = rows.Scan(data...); err != nil {
			break
		}
		row := make(map[string]any, len(columns))
		for i, v := range data {
			row[columns[i]] = v.(*ColumnScanner).Get()
		}
		result = append(result, row)
	}

	if err == nil {
		err = rows.Err()
	}
	if err != nil {
		return nil, errorj.SelectFromTableError.Wrap(err, "failed read selected rows").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:    p.config.Schema,
				Table:     tableName,
				Statement: query,
				Values:    values,
			})
	}

	return result, nil
}

func (p *Postgres) Count(ctx context.Context, tableName string, whenConditions *WhenConditions) (int, error) {
	res, err := p.selectFrom(ctx, tableName, "count(*) as jitsu_count", whenConditions)
	if err != nil {
		return -1, err
	}
	if len(res) == 0 {
		return -1, fmt.Errorf("select count * gave no result")
	}
	return res[0]["jitsu_count"].(int), nil
}

func (p *Postgres) Delete(ctx context.Context, tableName string, deleteConditions *WhenConditions) error {
	deleteCondition, values := p.toWhenConditions(deleteConditions)
	query := fmt.Sprintf(deleteQueryTemplate, p.config.Schema, tableName, deleteCondition)

	if _, err := p.txOrDb(ctx).ExecContext(ctx, query, values...); err != nil {
		return errorj.DeleteFromTableError.Wrap(err, "failed to delete data").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:    p.config.Schema,
				Table:     tableName,
				Statement: query,
			})
	}

	return nil
}

// TruncateTable deletes all records in tableName table
func (p *Postgres) TruncateTable(ctx context.Context, tableName string) error {
	statement := fmt.Sprintf(postgresTruncateTableTemplate, p.config.Schema, tableName)
	if _, err := p.txOrDb(ctx).ExecContext(ctx, statement); err != nil {
		return errorj.TruncateError.Wrap(err, "failed to truncate table").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:    p.config.Schema,
				Table:     tableName,
				Statement: statement,
			})
	}

	return nil
}

func (p *Postgres) toWhenConditions(conditions *WhenConditions) (string, []any) {
	if conditions == nil {
		return "", []any{}
	}
	var queryConditions []string
	var values []any

	for i, condition := range conditions.Conditions {
		conditionString := condition.Field + " " + condition.Clause + " $" + strconv.Itoa(i+1)
		queryConditions = append(queryConditions, conditionString)
		values = append(values, types.ReformatValue(condition.Value))
	}

	return strings.Join(queryConditions, " "+conditions.JoinCondition+" "), values
}

// executeInsert execute insert with insertTemplate
func (p *Postgres) executeInsertInTransaction(ctx context.Context, table *Table, merge bool, headerWithoutQuotes []string,
	placeholders string, valueArgs []any) error {
	var quotedHeader []string
	for _, columnName := range headerWithoutQuotes {
		quotedHeader = append(quotedHeader, fmt.Sprintf(`"%s"`, columnName))
	}
	var statement string
	if merge && len(table.PKFields) > 0 {
		statement = fmt.Sprintf(mergeTemplate, p.config.Schema, table.Name, strings.Join(quotedHeader, ","), placeholders, table.PrimaryKeyName, p.buildUpdateSection(headerWithoutQuotes))
	} else {
		statement = fmt.Sprintf(insertTemplate, p.config.Schema, table.Name, strings.Join(quotedHeader, ","), placeholders)
	}

	if _, err := p.txOrDb(ctx).ExecContext(ctx, statement, valueArgs...); err != nil {
		return errorj.ExecuteInsertInBatchError.Wrap(err, "failed to execute insert").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:          p.config.Schema,
				Table:           table.Name,
				PrimaryKeys:     table.GetPKFields(),
				Statement:       statement,
				ValuesMapString: types.ObjectValuesToString(headerWithoutQuotes, valueArgs),
			})
	}

	return nil
}

// columnDDL returns column DDL (quoted column name, mapped sql type and 'not null' if pk field)
func (p *Postgres) columnDDL(name string, column SQLColumn, pkFields utils.Set[string]) string {
	var notNullClause string
	sqlType := column.GetDDLType()

	//not null
	if _, ok := pkFields[name]; ok {
		notNullClause = " not null " + p.getDefaultValueStatement(sqlType)
	}

	return fmt.Sprintf(`"%s" %s%s`, name, sqlType, notNullClause)
}

// getCastClause returns ::SQL_TYPE clause or empty string
// $1::type, $2::type, $3, etc
func (p *Postgres) getCastClause(name string, column SQLColumn) string {
	if column.Override {
		return "::" + column.Type
	}
	return ""
}

// return default value statement for creating column
func (p *Postgres) getDefaultValueStatement(sqlType string) string {
	//get default value based on type
	if strings.Contains(sqlType, "var") || strings.Contains(sqlType, "text") {
		return "default ''"
	}

	return "default 0"
}

// Close underlying sql.DB
func (p *Postgres) Close() error {
	return p.dataSource.Close()
}

// getPrimaryKey returns primary key name and fields
func (p *Postgres) getPrimaryKey(ctx context.Context, tableName string) (string, utils.Set[string], error) {
	primaryKeys := utils.Set[string]{}
	pkFieldsRows, err := p.txOrDb(ctx).QueryContext(ctx, primaryKeyFieldsQuery, p.config.Schema, tableName)
	if err != nil {
		return "", nil, errorj.GetPrimaryKeysError.Wrap(err, "failed to get primary key").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:    p.config.Schema,
				Table:     tableName,
				Statement: primaryKeyFieldsQuery,
				Values:    []any{p.config.Schema, tableName},
			})
	}

	defer pkFieldsRows.Close()
	var pkFields []string
	var primaryKeyName string
	for pkFieldsRows.Next() {
		var constraintName, keyColumn string
		if err := pkFieldsRows.Scan(&constraintName, &keyColumn); err != nil {
			return "", nil, errorj.GetPrimaryKeysError.Wrap(err, "failed to scan result").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Schema:    p.config.Schema,
					Table:     tableName,
					Statement: primaryKeyFieldsQuery,
					Values:    []any{p.config.Schema, tableName},
				})
		}
		if primaryKeyName == "" && constraintName != "" {
			primaryKeyName = constraintName
		}

		pkFields = append(pkFields, keyColumn)
	}

	if err := pkFieldsRows.Err(); err != nil {
		return "", nil, errorj.GetPrimaryKeysError.Wrap(err, "failed read last row").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:    p.config.Schema,
				Table:     tableName,
				Statement: primaryKeyFieldsQuery,
				Values:    []any{p.config.Schema, tableName},
			})
	}

	for _, field := range pkFields {
		primaryKeys[field] = struct{}{}
	}

	return primaryKeyName, primaryKeys, nil
}

// buildUpdateSection returns value for merge update statement ("col1"=$1, "col2"=$2)
func (p *Postgres) buildUpdateSection(header []string) string {
	var updateColumns []string
	for i, columnName := range header {
		updateColumns = append(updateColumns, fmt.Sprintf(`"%s"=$%d`, columnName, i+1))
	}
	return strings.Join(updateColumns, ",")
}

func (p *Postgres) txOrDb(ctx context.Context) TxOrDB {
	txOrDb, ok := ctx.Value(ContextTransactionKey).(TxOrDB)
	if !ok {
		return p.dbWrapper
	}
	return txOrDb
}

func removeLastComma(str string) string {
	if last := len(str) - 1; last >= 0 && str[last] == ',' {
		str = str[:last]
	}

	return str
}

// checkErr checks and extracts parsed pq.Error and extract code,message,details
func checkErr(err error) error {
	if err == nil {
		return nil
	}

	if pgErr, ok := err.(*pq.Error); ok {
		msgParts := []string{"pq:"}
		if pgErr.Code != "" {
			msgParts = append(msgParts, string(pgErr.Code))
		}
		if pgErr.Message != "" {
			msgParts = append(msgParts, pgErr.Message)
		}
		if pgErr.Detail != "" {
			msgParts = append(msgParts, pgErr.Detail)
		}
		if pgErr.Schema != "" {
			msgParts = append(msgParts, "schema:"+pgErr.Schema)
		}
		if pgErr.Table != "" {
			msgParts = append(msgParts, "table:"+pgErr.Table)
		}
		if pgErr.Column != "" {
			msgParts = append(msgParts, "column:"+pgErr.Column)
		}
		if pgErr.DataTypeName != "" {
			msgParts = append(msgParts, "data_type:"+pgErr.DataTypeName)
		}
		if pgErr.Constraint != "" {
			msgParts = append(msgParts, "constraint:"+pgErr.Constraint)
		}
		return errors.New(strings.Join(msgParts, " "))
	}

	return err
}
