package sql

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/jitsucom/bulker/base/errorj"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/types"
	jsoniter "github.com/json-iterator/go"
	"os"
	"strings"
	"text/template"
	"time"

	_ "github.com/lib/pq"
)

func init() {
	bulker.RegisterBulker(PostgresBulkerTypeId, NewPostgres)
}

const (
	PostgresBulkerTypeId = "postgres"

	pgTableSchemaQuery = `SELECT 
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
	pgPrimaryKeyFieldsQuery = `SELECT tco.constraint_name as constraint_name,
       kcu.column_name as key_column
FROM information_schema.table_constraints tco
         JOIN information_schema.key_column_usage kcu
              ON kcu.constraint_name = tco.constraint_name
                  AND kcu.constraint_schema = tco.constraint_schema
                  AND kcu.constraint_name = tco.constraint_name
WHERE tco.constraint_type = 'PRIMARY KEY' AND 
      kcu.table_schema = $1 AND
      kcu.table_name = $2`
	pgCreateDbSchemaIfNotExistsTemplate = `CREATE SCHEMA IF NOT EXISTS "%s"`

	pgMergeQuery = `INSERT INTO {{.TableName}}({{.Columns}}) VALUES ({{.Placeholders}}) ON CONFLICT ON CONSTRAINT {{.PrimaryKeyName}} DO UPDATE set {{.UpdateSet}}`

	pgCopyTemplate = `COPY %s(%s) FROM STDIN`

	pgBulkMergeQuery       = `INSERT INTO {{.TableTo}}({{.Columns}}) SELECT {{.Columns}} FROM {{.TableFrom}} ON CONFLICT ON CONSTRAINT {{.PrimaryKeyName}} DO UPDATE SET {{.UpdateSet}}`
	pgBulkMergeSourceAlias = `excluded`
)

var (
	pgMergeQueryTemplate, _     = template.New("postgresMergeQuery").Parse(pgMergeQuery)
	pgBulkMergeQueryTemplate, _ = template.New("postgresBulkMergeQuery").Parse(pgBulkMergeQuery)

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
	SQLAdapterBase[DataSourceConfig]
}

// NewPostgres return configured Postgres bulker.Bulker instance
func NewPostgres(bulkerConfig bulker.Config) (bulker.Bulker, error) {
	config := &DataSourceConfig{}
	if err := utils.ParseObject(bulkerConfig.DestinationConfig, config); err != nil {
		return nil, fmt.Errorf("failed to parse destination config: %w", err)
	}
	_, config.Schema = adaptSqlIdentifier(config.Schema, 63, 0, nil, false)
	connectionString := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s search_path=%s",
		config.Host, config.Port, config.Db, config.Username, config.Password, config.Schema)
	//concat provided connection parameters
	for k, v := range config.Parameters {
		connectionString += " " + k + "=" + v + " "
	}
	logging.Infof("connecting: %s", connectionString)

	dataSource, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, err
	}

	if err := dataSource.Ping(); err != nil {
		_ = dataSource.Close()
		return nil, err
	}

	//set default value
	dataSource.SetConnMaxLifetime(10 * time.Minute)

	typecastFunc := func(placeholder string, column SQLColumn) string {
		if column.Override {
			return placeholder + "::" + column.Type
		}
		return placeholder
	}
	valueMappingFunc := func(value any, valuePresent bool, sqlColumn SQLColumn) any {
		//replace zero byte character for text fields
		if sqlColumn.Type == "text" {
			if v, ok := value.(string); ok {
				if strings.ContainsRune(v, '\u0000') {
					value = strings.ReplaceAll(v, "\u0000", "")
				}
			}
		}
		return value
	}
	var queryLogger *logging.QueryLogger
	if bulkerConfig.LogLevel == bulker.Verbose {
		queryLogger = logging.NewQueryLogger(bulkerConfig.Id, os.Stderr, os.Stderr)
	}
	p := &Postgres{newSQLAdapterBase(PostgresBulkerTypeId, config, dataSource, queryLogger, typecastFunc, IndexParameterPlaceholder, pgColumnDDL, valueMappingFunc, checkErr)}

	return p, nil
}

func (p *Postgres) CreateStream(id, tableName string, mode bulker.BulkMode, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	streamOptions = append(streamOptions, withLocalBatchFile(fmt.Sprintf("bulker_%s", utils.SanitizeString(id))))

	if err := p.validateOptions(streamOptions); err != nil {
		return nil, err
	}
	switch mode {
	case bulker.Stream:
		return newAutoCommitStream(id, p, tableName, streamOptions...)
	case bulker.Batch:
		return newTransactionalStream(id, p, tableName, streamOptions...)
	case bulker.ReplaceTable:
		return newReplaceTableStream(id, p, tableName, streamOptions...)
	case bulker.ReplacePartition:
		return newReplacePartitionStream(id, p, tableName, streamOptions...)
	}
	return nil, fmt.Errorf("unsupported bulk mode: %s", mode)
}

func (p *Postgres) validateOptions(streamOptions []bulker.StreamOption) error {
	options := &bulker.StreamOptions{}
	for _, option := range streamOptions {
		option(options)
	}
	return nil
}

func (p *Postgres) GetTypesMapping() map[types.DataType]string {
	return SchemaToPostgres
}

// OpenTx opens underline sql transaction and return wrapped instance
func (p *Postgres) OpenTx(ctx context.Context) (*TxSQLAdapter, error) {
	return p.openTx(ctx, p)
}

// InitDatabase creates database schema instance if doesn't exist
func (p *Postgres) InitDatabase(ctx context.Context) error {
	query := fmt.Sprintf(pgCreateDbSchemaIfNotExistsTemplate, p.config.Schema)

	if _, err := p.txOrDb(ctx).ExecContext(ctx, query); err != nil {
		//return errorj.CreateSchemaError.Wrap(err, "failed to create db schema").
		//	WithProperty(errorj.DBInfo, &types.ErrorPayload{
		//		Schema:    p.config.Schema,
		//		Statement: query,
		//	})
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
	tableName = p.TableName(tableName)
	table := &Table{Name: tableName, Columns: map[string]SQLColumn{}, PKFields: utils.Set[string]{}}
	rows, err := p.txOrDb(ctx).QueryContext(ctx, pgTableSchemaQuery, p.config.Schema, tableName)
	if err != nil {
		return nil, errorj.GetTableError.Wrap(err, "failed to get table columns").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:      p.config.Schema,
				Table:       tableName,
				PrimaryKeys: table.GetPKFields(),
				Statement:   pgTableSchemaQuery,
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
					Table:       tableName,
					PrimaryKeys: table.GetPKFields(),
					Statement:   pgTableSchemaQuery,
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
				Table:       tableName,
				PrimaryKeys: table.GetPKFields(),
				Statement:   pgTableSchemaQuery,
				Values:      []any{p.config.Schema, tableName},
			})
	}

	return table, nil
}

func (p *Postgres) Insert(ctx context.Context, table *Table, merge bool, objects []types.Object) error {
	if !merge {
		return p.insert(ctx, table, objects)
	} else {
		return p.insertOrMerge(ctx, table, objects, pgMergeQueryTemplate)
	}
}

func (p *Postgres) CopyTables(ctx context.Context, targetTable *Table, sourceTable *Table, merge bool) error {
	if !merge {
		return p.copy(ctx, targetTable, sourceTable)
	} else {
		return p.copyOrMerge(ctx, targetTable, sourceTable, pgBulkMergeQueryTemplate, pgBulkMergeSourceAlias)
	}
}

func (p *Postgres) LoadTable(ctx context.Context, targetTable *Table, loadSource *LoadSource) (err error) {
	quotedTableName := p.quotedTableName(targetTable.Name)
	if loadSource.Type != LocalFile {
		return fmt.Errorf("LoadTable: only local file is supported")
	}
	if loadSource.Format != p.batchFileFormat {
		return fmt.Errorf("LoadTable: only %s format is supported", p.batchFileFormat)
	}
	columns := targetTable.SortedColumnNames()
	columnNames := make([]string, len(columns))
	for i, name := range columns {
		columnNames[i] = p.quotedColumnName(name)
	}
	copyStatement := fmt.Sprintf(pgCopyTemplate, quotedTableName, strings.Join(columnNames, ", "))
	defer func() {
		if err != nil {
			err = errorj.LoadError.Wrap(err, "failed to load table").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Schema:      p.config.Schema,
					Table:       quotedTableName,
					PrimaryKeys: targetTable.GetPKFields(),
					Statement:   copyStatement,
				})
		}
	}()

	stmt, err := p.txOrDb(ctx).PrepareContext(ctx, copyStatement)
	if err != nil {
		return err
	}
	defer func() {
		_ = stmt.Close()
	}()
	//f, err := os.ReadFile(loadSource.Path)
	//logging.Infof("FILE: %s", f)

	file, err := os.Open(loadSource.Path)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		object := map[string]any{}
		decoder := jsoniter.NewDecoder(bytes.NewReader(scanner.Bytes()))
		decoder.UseNumber()
		err = decoder.Decode(&object)
		if err != nil {
			return err
		}
		args := make([]any, len(columns))
		for i, v := range columns {
			l := types.ReformatValue(object[v])
			args[i] = l
		}
		if _, err := stmt.ExecContext(ctx, args...); err != nil {
			return checkErr(err)
		}
	}
	_, err = stmt.ExecContext(ctx)
	if err != nil {
		return checkErr(err)
	}

	return nil
}

// pgColumnDDL returns column DDL (quoted column name, mapped sql type and 'not null' if pk field)
func pgColumnDDL(name, quotedName string, column SQLColumn, pkFields utils.Set[string]) string {
	var notNullClause string
	sqlType := column.GetDDLType()

	//not null
	if _, ok := pkFields[name]; ok {
		notNullClause = " not null " + getDefaultValueStatement(sqlType)
	}

	return fmt.Sprintf(`%s %s%s`, quotedName, sqlType, notNullClause)
}

// return default value statement for creating column
func getDefaultValueStatement(sqlType string) string {
	//get default value based on type
	if strings.Contains(sqlType, "var") || strings.Contains(sqlType, "text") {
		return "default ''"
	}

	return "default 0"
}

// getPrimaryKey returns primary key name and fields
func (p *Postgres) getPrimaryKey(ctx context.Context, tableName string) (string, utils.Set[string], error) {
	tableName = p.TableName(tableName)
	primaryKeys := utils.Set[string]{}
	pkFieldsRows, err := p.txOrDb(ctx).QueryContext(ctx, pgPrimaryKeyFieldsQuery, p.config.Schema, tableName)
	if err != nil {
		return "", nil, errorj.GetPrimaryKeysError.Wrap(err, "failed to get primary key").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:    p.config.Schema,
				Table:     tableName,
				Statement: pgPrimaryKeyFieldsQuery,
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
					Statement: pgPrimaryKeyFieldsQuery,
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
				Statement: pgPrimaryKeyFieldsQuery,
				Values:    []any{p.config.Schema, tableName},
			})
	}

	for _, field := range pkFields {
		primaryKeys[field] = struct{}{}
	}

	return primaryKeyName, primaryKeys, nil
}
