package implementations

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jitsucom/bulker/base/errorj"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/timestamp"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/base/uuid"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/types"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

const (
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
	bulkMergeTemplate                 = `INSERT INTO "%s"."%s"(%s) SELECT %s FROM "%s"."%s" ON CONFLICT ON CONSTRAINT %s DO UPDATE SET %s`
	bulkMergePrefix                   = `excluded`
	deleteQueryTemplate               = `DELETE FROM "%s"."%s" WHERE %s`

	updateStatement   = `UPDATE "%s"."%s" SET %s WHERE %s=$%d`
	dropTableTemplate = `DROP TABLE %s"%s"."%s"`

	renameTableTemplate = `ALTER TABLE "%s"."%s" RENAME TO "%s"`
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
	ctx         context.Context
	config      *types.DataSourceConfig
	dataSource  *sql.DB
	queryLogger *logging.QueryLogger

	sqlTypes types.SQLTypes
}

// NewPostgres return configured Postgres adapter instance
func NewPostgres(ctx context.Context, config *types.DataSourceConfig, queryLogger *logging.QueryLogger, sqlTypes types.SQLTypes) (*Postgres, error) {
	connectionString := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s ",
		config.Host, config.Port, config.Db, config.Username, config.Password)
	logging.Info(ctx, "connecting to postgres: %s", connectionString)
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

	p := &Postgres{ctx: ctx, config: config, dataSource: dataSource, queryLogger: queryLogger, sqlTypes: reformatMappings(sqlTypes, SchemaToPostgres)}
	//create db schema if doesn't exist
	err = p.CreateDbSchema(config.Schema)
	if err != nil {
		_ = p.Close()
		return nil, err
	}
	return p, nil
}

func (p *Postgres) CreateStream(id, tableName string, mode bulker.BulkMode, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	switch mode {
	case bulker.AutoCommit:
		return NewAutoCommitStream(id, p, tableName, streamOptions...)
	case bulker.Transactional:
		return NewTransactionalStream(id, p, tableName, streamOptions...)
	}

	return nil, fmt.Errorf("unsupported bulk mode: %d", mode)
}

// Type returns Postgres type
func (p *Postgres) Type() string {
	return "Postgres"
}

func (p *Postgres) GetConfig() *types.DataSourceConfig {
	return p.config
}

func (p *Postgres) GetTypesMapping() map[types.DataType]string {
	return SchemaToPostgres
}

// OpenTx opens underline sql transaction and return wrapped instance
func (p *Postgres) OpenTx() (*types.Transaction, error) {
	tx, err := p.dataSource.BeginTx(p.ctx, nil)
	if err != nil {
		err = types.CheckErr(err)
		return nil, errorj.BeginTransactionError.Wrap(err, "failed to begin transaction").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema: p.config.Schema,
			})
	}

	return types.NewTransaction(p.Type(), tx), nil
}

func (p *Postgres) GetAutoCommitTx() *types.Transaction {
	return types.NewAutoCommitTx(p.dataSource)
}

// CreateDbSchema creates database schema instance if doesn't exist
func (p *Postgres) CreateDbSchema(dbSchemaName string) error {
	query := fmt.Sprintf(createDbSchemaIfNotExistsTemplate, dbSchemaName)
	p.queryLogger.LogDDL(query)

	if _, err := p.dataSource.ExecContext(p.ctx, query); err != nil {
		err = types.CheckErr(err)

		return errorj.CreateSchemaError.Wrap(err, "failed to create db schema").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:    dbSchemaName,
				Statement: query,
			})
	}

	return nil
}

// GetTableSchema returns table (name,columns with name and types) representation wrapped in Table struct
func (p *Postgres) GetTableSchema(wrappedTx types.TxOrDatasource, tableName string) (*types.Table, error) {
	table, err := p.getTable(wrappedTx, tableName)
	if err != nil {
		return nil, err
	}

	//don't select primary keys of non-existent table
	if len(table.Columns) == 0 {
		return table, nil
	}

	primaryKeyName, pkFields, err := p.getPrimaryKey(wrappedTx, tableName)
	if err != nil {
		return nil, err
	}

	table.PKFields = pkFields
	table.PrimaryKeyName = primaryKeyName

	jitsuPrimaryKeyName := types.BuildConstraintName(table.Schema, table.Name)
	if primaryKeyName != "" && primaryKeyName != jitsuPrimaryKeyName {
		logging.Warnf("table: %s.%s has a custom primary key with name: %s that isn't managed by Jitsu. Custom primary key will be used in rows deduplication and updates. primary_key_fields configuration provided in Jitsu config will be ignored.", table.Schema, table.Name, primaryKeyName)
	}
	return table, nil
}

func (p *Postgres) getTable(wrappedTx types.TxOrDatasource, tableName string) (*types.Table, error) {
	table := &types.Table{Schema: p.config.Schema, Name: tableName, Columns: map[string]types.SQLColumn{}, PKFields: utils.Set{}}
	rows, err := wrappedTx.QueryContext(p.ctx, tableSchemaQuery, p.config.Schema, tableName)
	if err != nil {
		err = types.CheckErr(err)
		return nil, errorj.GetTableError.Wrap(err, "failed to get table columns").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:      p.config.Schema,
				Table:       table.Name,
				PrimaryKeys: table.GetPKFields(),
				Statement:   tableSchemaQuery,
				Values:      []interface{}{p.config.Schema, tableName},
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
					Values:      []interface{}{p.config.Schema, tableName},
				})
		}
		if columnPostgresType == "-" {
			//skip dropped postgres field
			continue
		}
		table.Columns[columnName] = types.SQLColumn{Type: columnPostgresType}
	}

	if err := rows.Err(); err != nil {
		return nil, errorj.GetTableError.Wrap(err, "failed read last row").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:      p.config.Schema,
				Table:       table.Name,
				PrimaryKeys: table.GetPKFields(),
				Statement:   tableSchemaQuery,
				Values:      []interface{}{p.config.Schema, tableName},
			})
	}

	return table, nil
}

// CreateTable create table columns and pk key
// override input table sql type with configured cast type
// make fields from Table PkFields - 'not null'
func (p *Postgres) CreateTable(wrappedTx types.TxOrDatasource, table *types.Table) error {
	var columnsDDL []string
	pkFields := table.GetPKFieldsSet()
	for _, columnName := range table.SortedColumnNames() {
		column := table.Columns[columnName]
		columnsDDL = append(columnsDDL, p.columnDDL(columnName, column, pkFields))
	}

	//sorting columns asc
	sort.Strings(columnsDDL)
	query := fmt.Sprintf(createTableTemplate, p.config.Schema, table.Name, strings.Join(columnsDDL, ", "))
	p.queryLogger.LogDDL(query)

	if _, err := wrappedTx.ExecContext(p.ctx, query); err != nil {
		err = types.CheckErr(err)

		return errorj.CreateTableError.Wrap(err, "failed to create table").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:      p.config.Schema,
				Table:       table.Name,
				PrimaryKeys: table.GetPKFields(),
				Statement:   query,
			})
	}

	if err := p.createPrimaryKeyInTransaction(wrappedTx, table); err != nil {
		return err
	}

	return nil
}

// PatchTableSchema alter table with columns (if not empty)
// recreate primary key (if not empty) or delete primary key if Table.DeletePkFields is true
func (p *Postgres) PatchTableSchema(wrappedTx types.TxOrDatasource, patchTable *types.Table) error {
	pkFields := patchTable.GetPKFieldsSet()
	//patch columns
	for _, columnName := range patchTable.SortedColumnNames() {
		column := patchTable.Columns[columnName]
		columnDDL := p.columnDDL(columnName, column, pkFields)
		query := fmt.Sprintf(addColumnTemplate, p.config.Schema, patchTable.Name, columnDDL)
		p.queryLogger.LogDDL(query)

		if _, err := wrappedTx.ExecContext(p.ctx, query); err != nil {
			err = types.CheckErr(err)
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
		err := p.deletePrimaryKeyInTransaction(wrappedTx, patchTable)
		if err != nil {
			return err
		}
	}

	//patch primary keys - create new
	if len(patchTable.PKFields) > 0 {
		err := p.createPrimaryKeyInTransaction(wrappedTx, patchTable)
		if err != nil {
			return err
		}
	}

	return nil
}

// createPrimaryKeyInTransaction create primary key constraint
func (p *Postgres) createPrimaryKeyInTransaction(wrappedTx types.TxOrDatasource, table *types.Table) error {
	if len(table.PKFields) == 0 {
		return nil
	}

	var quotedColumnNames []string
	for _, column := range table.GetPKFields() {
		quotedColumnNames = append(quotedColumnNames, fmt.Sprintf(`"%s"`, column))
	}

	statement := fmt.Sprintf(alterPrimaryKeyTemplate,
		p.config.Schema, table.Name, table.PrimaryKeyName, strings.Join(quotedColumnNames, ","))
	p.queryLogger.LogDDL(statement)

	if _, err := wrappedTx.ExecContext(p.ctx, statement); err != nil {
		err = types.CheckErr(err)
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
func (p *Postgres) deletePrimaryKeyInTransaction(wrappedTx types.TxOrDatasource, table *types.Table) error {
	query := fmt.Sprintf(dropPrimaryKeyTemplate, p.config.Schema, table.Name, table.PrimaryKeyName)
	p.queryLogger.LogDDL(query)

	if _, err := wrappedTx.ExecContext(p.ctx, query); err != nil {
		err = types.CheckErr(err)
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

func (p *Postgres) ReplaceTable(wrappedTx types.TxOrDatasource, originalTable, replacementTable string, dropOldTable bool) (err error) {
	tmpTable := "deprecated_" + originalTable + timestamp.Now().Format("_20060102_150405")
	err1 := p.renameTableInTransaction(wrappedTx, true, originalTable, tmpTable)
	err = p.renameTableInTransaction(wrappedTx, false, replacementTable, originalTable)
	if dropOldTable && err1 == nil && err == nil {
		return p.DropTable(wrappedTx, &types.Table{Name: tmpTable}, true)
	}
	return
}

// Update one record in Postgres
func (p *Postgres) Update(wrappedTx types.TxOrDatasource, table *types.Table, object map[string]interface{}, whereKey string, whereValue interface{}) error {
	columns := make([]string, len(object), len(object))
	values := make([]interface{}, len(object)+1, len(object)+1)
	i := 0
	for name, value := range object {
		columns[i] = name + "= $" + strconv.Itoa(i+1) //$0 - wrong
		values[i] = value
		i++
	}
	values[i] = whereValue

	statement := fmt.Sprintf(updateStatement, p.config.Schema, table.Name, strings.Join(columns, ", "), whereKey, i+1)
	p.queryLogger.LogQueryWithValues(statement, values)

	if _, err := wrappedTx.ExecContext(p.ctx, statement, values...); err != nil {
		err = types.CheckErr(err)

		return errorj.UpdateError.Wrap(err, "failed to update").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:      p.config.Schema,
				Table:       table.Name,
				PrimaryKeys: table.GetPKFields(),
				Statement:   statement,
				Values:      values,
			})
	}

	return nil
}

// Insert should be used when table has no primary keys. Inserts data in batches to improve performance.
func (p *Postgres) Insert(wrappedTx types.TxOrDatasource, table *types.Table, merge bool, objects []types.Object) error {
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
	valueArgs := make([]interface{}, 0, maxValues)
	placeholdersCounter := 1
	operation := 0
	operations := int(math.Max(1, float64(valuesAmount)/float64(PostgresValuesLimit)))
	for _, row := range objects {
		// if number of values exceeds limit, we have to execute insert query on processed rows
		if len(valueArgs)+len(headerWithoutQuotes) > PostgresValuesLimit {
			operation++
			if err := p.executeInsertInTransaction(wrappedTx, table, merge, headerWithoutQuotes, removeLastComma(placeholdersBuilder.String()), valueArgs); err != nil {
				return errorj.Decorate(err, "middle insert %d of %d in batch", operation, operations)
			}

			placeholdersBuilder.Reset()
			placeholdersCounter = 1
			valueArgs = make([]interface{}, 0, maxValues)
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
		if err := p.executeInsertInTransaction(wrappedTx, table, merge, headerWithoutQuotes, removeLastComma(placeholdersBuilder.String()), valueArgs); err != nil {
			return errorj.Decorate(err, "last insert %d of %d in batch", operation, operations)
		}
	}

	return nil
}

// bulkMergeInTransaction creates tmp table without duplicates
// inserts all data into tmp table and using bulkMergeTemplate merges all data to main table
func (p *Postgres) bulkMergeInTransaction(wrappedTx *types.Transaction, table *types.Table, objects []types.Object) error {
	tmpTable := &types.Table{
		Name:           fmt.Sprintf("jitsu_tmp_%s", uuid.NewLettersNumbers()[:5]),
		Columns:        table.Columns,
		PKFields:       utils.Set{},
		DeletePkFields: false,
	}

	if err := p.CreateTable(wrappedTx, tmpTable); err != nil {
		return errorj.Decorate(err, "failed to create temporary table")
	}

	if err := p.Insert(wrappedTx, tmpTable, false, objects); err != nil {
		return errorj.Decorate(err, "failed to insert into temporary table")
	}

	if err := p.MergeTables(wrappedTx, table, tmpTable); err != nil {
		return err
	}

	//delete tmp table
	if err := p.DropTable(wrappedTx, tmpTable, false); err != nil {
		return errorj.Decorate(err, "failed to drop temporary table")
	}

	return nil
}

func (p *Postgres) MergeTables(wrappedTx types.TxOrDatasource, targetTable *types.Table, sourceTable *types.Table) error {
	//insert from select
	var setValues []string
	var headerWithQuotes []string
	for _, name := range targetTable.SortedColumnNames() {
		setValues = append(setValues, fmt.Sprintf(`"%s"=%s."%s"`, name, bulkMergePrefix, name))
		headerWithQuotes = append(headerWithQuotes, fmt.Sprintf(`"%s"`, name))
	}

	insertFromSelectStatement := fmt.Sprintf(bulkMergeTemplate, p.config.Schema, targetTable.Name, strings.Join(headerWithQuotes, ", "), strings.Join(headerWithQuotes, ", "), p.config.Schema, sourceTable.Name, targetTable.PrimaryKeyName, strings.Join(setValues, ", "))
	p.queryLogger.LogQuery(insertFromSelectStatement)

	if _, err := wrappedTx.ExecContext(p.ctx, insertFromSelectStatement); err != nil {
		err = types.CheckErr(err)

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

func (p *Postgres) DropTable(wrappedTx types.TxOrDatasource, table *types.Table, ifExists bool) error {
	ifExs := ""
	if ifExists {
		ifExs = "IF EXISTS "
	}
	query := fmt.Sprintf(dropTableTemplate, ifExs, p.config.Schema, table.Name)
	p.queryLogger.LogDDL(query)

	if _, err := wrappedTx.ExecContext(p.ctx, query); err != nil {
		err = types.CheckErr(err)

		return errorj.DropError.Wrap(err, "failed to drop table").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:      p.config.Schema,
				Table:       table.Name,
				PrimaryKeys: table.GetPKFields(),
				Statement:   query,
			})
	}

	return nil
}

func (p *Postgres) renameTableInTransaction(wrappedTx types.TxOrDatasource, ifExists bool, tableName, newTableName string) error {
	if ifExists {
		row := wrappedTx.QueryRowContext(p.ctx, fmt.Sprintf(`SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s')`, p.config.Schema, tableName))
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
	p.queryLogger.LogDDL(query)

	if _, err := wrappedTx.ExecContext(p.ctx, query); err != nil {
		err = types.CheckErr(err)

		return errorj.RenameError.Wrap(err, "failed to rename table").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:    p.config.Schema,
				Table:     tableName,
				Statement: query,
			})
	}

	return nil
}

func (p *Postgres) Delete(wrappedTx types.TxOrDatasource, tableName string, deleteConditions *types.DeleteConditions) error {
	deleteCondition, values := p.toDeleteQuery(deleteConditions)
	query := fmt.Sprintf(deleteQueryTemplate, p.config.Schema, tableName, deleteCondition)
	p.queryLogger.LogQueryWithValues(query, values)

	if _, err := wrappedTx.ExecContext(p.ctx, query, values...); err != nil {
		err = types.CheckErr(err)
		return errorj.DeleteFromTableError.Wrap(err, "failed to delete data").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:    p.config.Schema,
				Table:     tableName,
				Statement: query,
			})
	}

	return nil
}

func (p *Postgres) toDeleteQuery(conditions *types.DeleteConditions) (string, []interface{}) {
	var queryConditions []string
	var values []interface{}

	for i, condition := range conditions.Conditions {
		conditionString := condition.Field + " " + condition.Clause + " $" + strconv.Itoa(i+1)
		queryConditions = append(queryConditions, conditionString)
		values = append(values, types.ReformatValue(condition.Value))
	}

	return strings.Join(queryConditions, " "+conditions.JoinCondition+" "), values
}

// executeInsert execute insert with insertTemplate
func (p *Postgres) executeInsertInTransaction(wrappedTx types.TxOrDatasource, table *types.Table, merge bool, headerWithoutQuotes []string,
	placeholders string, valueArgs []interface{}) error {
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
	p.queryLogger.LogQueryWithValues(statement, valueArgs)

	if _, err := wrappedTx.Exec(statement, valueArgs...); err != nil {
		err = types.CheckErr(err)
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
func (p *Postgres) columnDDL(name string, column types.SQLColumn, pkFields utils.Set) string {
	var notNullClause string
	sqlType := column.DDLType()

	if overriddenSQLType, ok := p.sqlTypes[name]; ok {
		sqlType = overriddenSQLType.ColumnType
	}

	//not null
	if _, ok := pkFields[name]; ok {
		notNullClause = " not null " + p.getDefaultValueStatement(sqlType)
	}

	return fmt.Sprintf(`"%s" %s%s`, name, sqlType, notNullClause)
}

// getCastClause returns ::SQL_TYPE clause or empty string
// $1::type, $2::type, $3, etc
func (p *Postgres) getCastClause(name string, column types.SQLColumn) string {
	castType, ok := p.sqlTypes[name]
	if !ok && column.Override {
		castType = column
		ok = true
	}
	if ok {
		return "::" + castType.Type
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
func (p *Postgres) getPrimaryKey(wrappedTx types.TxOrDatasource, tableName string) (string, utils.Set, error) {
	primaryKeys := utils.Set{}
	pkFieldsRows, err := wrappedTx.QueryContext(p.ctx, primaryKeyFieldsQuery, p.config.Schema, tableName)
	if err != nil {
		err = types.CheckErr(err)
		return "", nil, errorj.GetPrimaryKeysError.Wrap(err, "failed to get primary key").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:    p.config.Schema,
				Table:     tableName,
				Statement: primaryKeyFieldsQuery,
				Values:    []interface{}{p.config.Schema, tableName},
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
					Values:    []interface{}{p.config.Schema, tableName},
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
				Values:    []interface{}{p.config.Schema, tableName},
			})
	}

	for _, field := range pkFields {
		primaryKeys[field] = struct{}{}
	}

	return primaryKeyName, primaryKeys, nil
}

// buildInsertPayload returns
// 1. column names slice
// 2. quoted column names slice
// 2. placeholders slice
// 3. values slice
func (p *Postgres) buildInsertPayload(table *types.Table, valuesMap map[string]interface{}) ([]string, []string, []string, []interface{}) {
	header := make([]string, len(valuesMap), len(valuesMap))
	quotedHeader := make([]string, len(valuesMap), len(valuesMap))
	placeholders := make([]string, len(valuesMap), len(valuesMap))
	values := make([]interface{}, len(valuesMap), len(valuesMap))

	columns := make([]string, 0, len(valuesMap))
	for name, _ := range valuesMap {
		columns = append(columns, name)
	}
	sort.Strings(columns)
	for i, name := range columns {
		value := valuesMap[name]
		quotedHeader[i] = fmt.Sprintf(`"%s"`, name)
		header[i] = name

		//$1::type, $2::type, $3, etc ($0 - wrong)
		placeholders[i] = fmt.Sprintf("$%d%s", i+1, p.getCastClause(name, table.Columns[name]))
		values[i] = value
	}

	return header, quotedHeader, placeholders, values
}

// buildUpdateSection returns value for merge update statement ("col1"=$1, "col2"=$2)
func (p *Postgres) buildUpdateSection(header []string) string {
	var updateColumns []string
	for i, columnName := range header {
		updateColumns = append(updateColumns, fmt.Sprintf(`"%s"=$%d`, columnName, i+1))
	}
	return strings.Join(updateColumns, ",")
}

// reformatMappings handles old (deprecated) mapping types //TODO remove someday
// put sql types as is
// if mapping type is inner => map with sql type
func reformatMappings(mappingTypeCasts types.SQLTypes, dbTypes map[types.DataType]string) types.SQLTypes {
	formattedSqlTypes := types.SQLTypes{}
	for column, sqlType := range mappingTypeCasts {
		var columnType, columnStatement types.DataType
		var err error

		columnType, err = types.TypeFromString(sqlType.Type)
		if err != nil {
			formattedSqlTypes[column] = sqlType
			continue
		}

		columnStatement, err = types.TypeFromString(sqlType.ColumnType)
		if err != nil {
			formattedSqlTypes[column] = sqlType
			continue
		}

		dbSQLType, _ := dbTypes[columnType]
		dbColumnType, _ := dbTypes[columnStatement]
		formattedSqlTypes[column] = types.SQLColumn{
			Type:       dbSQLType,
			ColumnType: dbColumnType,
		}
	}

	return formattedSqlTypes
}

func removeLastComma(str string) string {
	if last := len(str) - 1; last >= 0 && str[last] == ',' {
		str = str[:last]
	}

	return str
}

// deduplicateObjects returns slices with deduplicated objects
// (two objects with the same pkFields values can't be in one slice)
func deduplicateObjects(table *types.Table, objects []types.Object) [][]types.Object {
	var pkFields []string
	for pkField := range table.PKFields {
		pkFields = append(pkFields, pkField)
	}

	var result [][]types.Object
	duplicatedInput := objects
	for {
		deduplicated, duplicated := getDeduplicatedAndOthers(pkFields, duplicatedInput)
		result = append(result, deduplicated)

		if len(duplicated) == 0 {
			break
		}

		duplicatedInput = duplicated
	}

	return result
}

// getDeduplicatedAndOthers returns slices with deduplicated objects and others objects
// (two objects with the same pkFields values can't be in deduplicated objects slice)
func getDeduplicatedAndOthers(pkFields []string, objects []types.Object) ([]types.Object, []types.Object) {
	var deduplicatedObjects, duplicatedObjects []types.Object
	deduplicatedIDs := utils.Set{}

	//find duplicates
	for _, object := range objects {
		var key string
		for _, pkField := range pkFields {
			value, _ := object[pkField]
			key += fmt.Sprint(value)
		}
		if _, ok := deduplicatedIDs[key]; ok {
			duplicatedObjects = append(duplicatedObjects, object)
		} else {
			deduplicatedIDs.Put(key)
			deduplicatedObjects = append(deduplicatedObjects, object)
		}
	}

	return deduplicatedObjects, duplicatedObjects
}
