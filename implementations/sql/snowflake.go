package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/hashicorp/go-multierror"
	"github.com/jitsucom/bulker/base/errorj"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/timestamp"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/implementations"
	"github.com/jitsucom/bulker/types"
	"os"
	"path"
	"strings"
	"text/template"

	sf "github.com/snowflakedb/gosnowflake"
)

func init() {
	bulker.RegisterBulker(SnowflakeBulkerTypeId, NewSnowflake)
}

const (
	SnowflakeBulkerTypeId = "snowflake"

	sfTableExistenceQuery = `SELECT count(*) from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA = ? and TABLE_NAME = ?`
	sfDescTableQuery      = `desc table %s`
	sfCopyStatement       = `COPY INTO %s (%s) from @~/%s FILE_FORMAT=(TYPE= 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE SKIP_HEADER = 1) `

	sfMergeStatement = `MERGE INTO {{.TableTo}} T USING (SELECT {{.Columns}} FROM {{.TableFrom}} ) S ON {{.JoinConditions}} WHEN MATCHED THEN UPDATE SET {{.UpdateSet}} WHEN NOT MATCHED THEN INSERT ({{.Columns}}) VALUES ({{.SourceColumns}})`

	sfCreateSchemaIfNotExistsTemplate = `CREATE SCHEMA IF NOT EXISTS %s`

	sfPrimaryKeyFieldsQuery = `show primary keys in %s`
)

var (
	sfMergeQueryTemplate, _ = template.New("snowflakeMergeQuery").Parse(sfMergeStatement)

	SchemaToSnowflake = map[types.DataType]string{
		types.STRING:    "text",
		types.INT64:     "bigint",
		types.FLOAT64:   "double precision",
		types.TIMESTAMP: "timestamp(6)",
		types.BOOL:      "boolean",
		types.UNKNOWN:   "text",
	}
)

// SnowflakeConfig dto for deserialized datasource config for Snowflake
type SnowflakeConfig struct {
	Account    string             `mapstructure:"account,omitempty" json:"account,omitempty" yaml:"account,omitempty"`
	Port       int                `mapstructure:"port,omitempty" json:"port,omitempty" yaml:"port,omitempty"`
	Db         string             `mapstructure:"db,omitempty" json:"db,omitempty" yaml:"db,omitempty"`
	Schema     string             `mapstructure:"schema,omitempty" json:"schema,omitempty" yaml:"schema,omitempty"`
	Username   string             `mapstructure:"username,omitempty" json:"username,omitempty" yaml:"username,omitempty"`
	Password   string             `mapstructure:"password,omitempty" json:"password,omitempty" yaml:"password,omitempty"`
	Warehouse  string             `mapstructure:"warehouse,omitempty" json:"warehouse,omitempty" yaml:"warehouse,omitempty"`
	Stage      string             `mapstructure:"stage,omitempty" json:"stage,omitempty" yaml:"stage,omitempty"`
	Parameters map[string]*string `mapstructure:"parameters,omitempty" json:"parameters,omitempty" yaml:"parameters,omitempty"`
}

// Validate required fields in SnowflakeConfig
func (sc *SnowflakeConfig) Validate() error {
	if sc == nil {
		return errors.New("Snowflake config is required")
	}
	if sc.Account == "" {
		return errors.New("Snowflake account is required parameter")
	}
	if sc.Db == "" {
		return errors.New("Snowflake db is required parameter")
	}
	if sc.Username == "" {
		return errors.New("Snowflake username is required parameter")
	}
	if sc.Warehouse == "" {
		return errors.New("Snowflake warehouse is required parameter")
	}

	if sc.Parameters == nil {
		sc.Parameters = map[string]*string{}
	}

	sc.Schema = reformatIdentifier(sc.Schema)
	return nil
}

// Snowflake is adapter for creating,patching (schema or table), inserting data to snowflake
type Snowflake struct {
	SQLAdapterBase[SnowflakeConfig]
}

// NewSnowflake returns configured Snowflake adapter instance
func NewSnowflake(bulkerConfig bulker.Config) (bulker.Bulker, error) {
	config := &SnowflakeConfig{}
	if err := utils.ParseObject(bulkerConfig.DestinationConfig, config); err != nil {
		return nil, fmt.Errorf("failed to parse destination config: %w", err)
	}
	cfg := &sf.Config{
		Account:   config.Account,
		User:      config.Username,
		Password:  config.Password,
		Port:      config.Port,
		Schema:    config.Schema,
		Database:  config.Db,
		Warehouse: config.Warehouse,
		Params:    config.Parameters,
	}
	connectionString, err := sf.DSN(cfg)
	if err != nil {
		return nil, err
	}

	dataSource, err := sql.Open("snowflake", connectionString)
	if err != nil {
		return nil, err
	}

	if err := dataSource.Ping(); err != nil {
		dataSource.Close()
		return nil, err
	}
	tableNameFunc := func(config *SnowflakeConfig, tableName string) string {
		return fmt.Sprintf(`%s.%s`, config.Schema, reformatIdentifier(tableName))
	}
	typecastFunc := func(placeholder string, column SQLColumn) string {
		if column.Override {
			return placeholder + "::" + column.Type
		}
		return placeholder
	}
	queryLogger := logging.NewQueryLogger(bulkerConfig.Id, os.Stderr, os.Stderr)
	s := &Snowflake{newSQLAdapterBase(SnowflakeBulkerTypeId, config, dataSource,
		queryLogger, typecastFunc, QuestionMarkParameterPlaceholder, tableNameFunc, reformatIdentifier, sfColumnDDL, unmappedValue, checkErr)}
	s.batchFileFormat = implementations.CSV
	return s, nil
}
func (s *Snowflake) CreateStream(id, tableName string, mode bulker.BulkMode, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	streamOptions = append(streamOptions, withLocalBatchFile(fmt.Sprintf("bulker_%s", utils.SanitizeString(id))))

	if err := s.validateOptions(streamOptions); err != nil {
		return nil, err
	}
	switch mode {
	case bulker.AutoCommit:
		return newAutoCommitStream(id, s, tableName, streamOptions...)
	case bulker.Transactional:
		return newTransactionalStream(id, s, tableName, streamOptions...)
	case bulker.ReplaceTable:
		return newReplaceTableStream(id, s, tableName, streamOptions...)
	case bulker.ReplacePartition:
		return newReplacePartitionStream(id, s, tableName, streamOptions...)
	}
	return nil, fmt.Errorf("unsupported bulk mode: %s", mode)
}

func (s *Snowflake) validateOptions(streamOptions []bulker.StreamOption) error {
	options := &bulker.StreamOptions{}
	for _, option := range streamOptions {
		option(options)
	}
	return nil
}

func (s *Snowflake) GetTypesMapping() map[types.DataType]string {
	return SchemaToSnowflake
}

// OpenTx opens underline sql transaction and return wrapped instance
func (s *Snowflake) OpenTx(ctx context.Context) (*TxSQLAdapter, error) {
	return s.openTx(ctx, s)
}

// InitDatabase create database schema instance if doesn't exist
func (s *Snowflake) InitDatabase(ctx context.Context) error {
	query := fmt.Sprintf(sfCreateSchemaIfNotExistsTemplate, s.config.Schema)

	if _, err := s.txOrDb(ctx).ExecContext(ctx, query); err != nil {
		err = checkErr(err)

		return errorj.CreateSchemaError.Wrap(err, "failed to create db schema").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:    s.config.Schema,
				Statement: query,
			})
	}

	return nil
}

// GetTableSchema returns table (name,columns with name and types) representation wrapped in Table struct
func (s *Snowflake) GetTableSchema(ctx context.Context, tableName string) (*Table, error) {
	table := &Table{Name: tableName, Columns: Columns{}, PKFields: utils.NewSet[string]()}

	countReqRows, err := s.txOrDb(ctx).QueryContext(ctx, sfTableExistenceQuery, reformatToParam(s.config.Schema), reformatToParam(reformatIdentifier(tableName)))
	if err != nil {
		return nil, errorj.GetTableError.Wrap(err, "failed to get table existence").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:    s.config.Schema,
				Table:     table.Name,
				Statement: sfTableExistenceQuery,
				Values:    []any{reformatToParam(s.config.Schema), reformatToParam(reformatIdentifier(tableName))},
			})
	}
	defer countReqRows.Close()
	countReqRows.Next()
	var count int
	if err = countReqRows.Scan(&count); err != nil {
		return nil, errorj.GetTableError.Wrap(err, "failed to scan existence result").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:    s.config.Schema,
				Table:     table.Name,
				Statement: sfTableExistenceQuery,
				Values:    []any{reformatToParam(s.config.Schema), reformatToParam(reformatIdentifier(tableName))},
			})
	}

	//table doesn't exist
	if count == 0 {
		return table, nil
	}

	query := fmt.Sprintf(sfDescTableQuery, s.fullTableName(tableName))
	rows, err := s.txOrDb(ctx).QueryContext(ctx, query)
	if err != nil {
		return nil, errorj.GetTableError.Wrap(err, "failed to get table columns").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:    s.config.Schema,
				Table:     table.Name,
				Statement: query,
			})
	}
	defer rows.Close()

	for rows.Next() {
		var row map[string]any
		row, err = rowToMap(rows)
		if err != nil {
			return nil, errorj.GetTableError.Wrap(err, "failed to scan result").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Schema:    s.config.Schema,
					Table:     table.Name,
					Statement: query,
				})
		}

		columnName := fmt.Sprint(row["name"])
		columnSnowflakeType := fmt.Sprint(row["type"])

		table.Columns[strings.ToLower(columnName)] = SQLColumn{Type: columnSnowflakeType}
	}
	if err := rows.Err(); err != nil {
		return nil, errorj.GetTableError.Wrap(err, "failed read last row").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:    s.config.Schema,
				Table:     table.Name,
				Statement: query,
			})
	}

	primaryKeyName, pkFields, err := s.getPrimaryKey(ctx, tableName)
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

// getPrimaryKey returns primary key name and fields
func (s *Snowflake) getPrimaryKey(ctx context.Context, tableName string) (string, utils.Set[string], error) {
	primaryKeys := utils.Set[string]{}
	statement := fmt.Sprintf(sfPrimaryKeyFieldsQuery, s.fullTableName(tableName))
	pkFieldsRows, err := s.txOrDb(ctx).QueryContext(ctx, statement)
	if err != nil {
		return "", nil, errorj.GetPrimaryKeysError.Wrap(err, "failed to get primary key").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:    s.config.Schema,
				Table:     tableName,
				Statement: statement,
			})
	}

	defer pkFieldsRows.Close()
	var pkFields []string
	var primaryKeyName string
	for pkFieldsRows.Next() {
		var row map[string]any
		row, err = rowToMap(pkFieldsRows)
		if err != nil {
			return "", nil, errorj.GetPrimaryKeysError.Wrap(err, "failed to get primary key").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Schema:    s.config.Schema,
					Table:     tableName,
					Statement: statement,
				})
		}
		constraintName, ok := row["constraint_name"].(string)
		if primaryKeyName == "" && ok {
			primaryKeyName = strings.ToLower(constraintName)
		}
		keyColumn, ok := row["column_name"].(string)
		if ok && keyColumn != "" {
			pkFields = append(pkFields, strings.ToLower(keyColumn))
		}
	}

	if err := pkFieldsRows.Err(); err != nil {
		return "", nil, errorj.GetPrimaryKeysError.Wrap(err, "failed read last row").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:    s.config.Schema,
				Table:     tableName,
				Statement: statement,
			})
	}

	for _, field := range pkFields {
		primaryKeys.Put(field)
	}

	return primaryKeyName, primaryKeys, nil
}

// LoadTable transfer data from local file to Snowflake by passing COPY request to Snowflake
func (s *Snowflake) LoadTable(ctx context.Context, targetTable *Table, loadSource *LoadSource) (err error) {
	if loadSource.Type != LocalFile {
		return fmt.Errorf("LoadTable: only local file is supported")
	}
	if loadSource.Format != s.batchFileFormat {
		return fmt.Errorf("LoadTable: only %s format is supported", s.batchFileFormat)
	}
	tableName := targetTable.Name
	putStatement := fmt.Sprintf("PUT file://%s @~", loadSource.Path)
	if _, err = s.txOrDb(ctx).ExecContext(ctx, putStatement); err != nil {
		return errorj.LoadError.Wrap(err, "failed to put file to stage").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:    s.config.Schema,
				Table:     tableName,
				Statement: putStatement,
			})
	}
	defer func() {
		removeStatement := fmt.Sprintf("REMOVE @~/%s", path.Base(loadSource.Path))
		if _, err2 := s.txOrDb(ctx).ExecContext(ctx, removeStatement); err2 != nil {
			err2 = errorj.LoadError.Wrap(err, "failed to remove file from stage").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Schema:    s.config.Schema,
					Table:     tableName,
					Statement: putStatement,
				})
			err = multierror.Append(err, err2)
		}
	}()
	columns := targetTable.SortedColumnNames()
	columnNames := make([]string, len(columns))
	for i, name := range columns {
		columnNames[i] = s.columnName(name)
	}

	statement := fmt.Sprintf(sfCopyStatement, s.fullTableName(tableName), strings.Join(columnNames, ","), path.Base(loadSource.Path))

	if _, err := s.txOrDb(ctx).ExecContext(ctx, statement); err != nil {
		return errorj.CopyError.Wrap(err, "failed to copy data from stage").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:    s.config.Schema,
				Table:     tableName,
				Statement: statement,
			})
	}

	return nil
}

// Insert inserts data with InsertContext as a single object or a batch into Snowflake
func (s *Snowflake) Insert(ctx context.Context, table *Table, merge bool, objects []types.Object) error {
	if !merge || len(table.GetPKFields()) == 0 {
		return s.insert(ctx, table, objects)
	}
	for _, object := range objects {
		pkMatchConditions := &WhenConditions{}
		for _, pkColumn := range table.GetPKFields() {
			value := object[pkColumn]
			if value == nil {
				pkMatchConditions = pkMatchConditions.Add(pkColumn, "IS NULL", nil)
			} else {
				pkMatchConditions = pkMatchConditions.Add(pkColumn, "=", value)
			}
		}
		res, err := s.SQLAdapterBase.Select(ctx, table.Name, pkMatchConditions, "")
		if err != nil {
			return errorj.ExecuteInsertError.Wrap(err, "failed check primary key collision").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Schema:      s.config.Schema,
					Table:       table.Name,
					PrimaryKeys: table.GetPKFields(),
				})
		}
		if len(res) > 0 {
			return s.Update(ctx, table.Name, object, pkMatchConditions)
		} else {
			return s.insert(ctx, table, []types.Object{object})
		}
	}
	return nil
}

func (s *Snowflake) CopyTables(ctx context.Context, targetTable *Table, sourceTable *Table, merge bool) error {
	if !merge {
		return s.copy(ctx, targetTable, sourceTable)
	} else {
		return s.copyOrMerge(ctx, targetTable, sourceTable, sfMergeQueryTemplate, "S")
	}
}

func (s *Snowflake) ReplaceTable(ctx context.Context, targetTableName string, replacementTable *Table, dropOldTable bool) error {
	tmpTable := "deprecated_" + targetTableName + timestamp.Now().Format("_20060102_150405")
	err1 := s.renameTable(ctx, true, targetTableName, tmpTable)
	err := s.renameTable(ctx, false, replacementTable.Name, targetTableName)
	if dropOldTable && err1 == nil && err == nil {
		return s.DropTable(ctx, tmpTable, true)
	}
	return nil
}

// columnDDLsfColumnDDL returns column DDL (column name, mapped sql type)
func sfColumnDDL(name string, column SQLColumn, pkFields utils.Set[string]) string {
	return fmt.Sprintf(`%s %s`, reformatIdentifier(name), column.GetDDLType())
}

func (s *Snowflake) Select(ctx context.Context, tableName string, whenConditions *WhenConditions, orderBy string) ([]map[string]any, error) {
	ctx = sf.WithHigherPrecision(ctx)
	return s.SQLAdapterBase.Select(ctx, tableName, whenConditions, orderBy)
}

// Snowflake has table with schema, table names and there
// quoted identifiers = without quotes
// unquoted identifiers = uppercased
func reformatToParam(value string) string {
	if strings.Contains(value, `"`) {
		return strings.ReplaceAll(value, `"`, ``)
	} else {
		return strings.ToUpper(value)
	}
}

// Snowflake accepts names (identifiers) started with '_' or letter
// also names can contain only '_', letters, numbers, '$'
// otherwise double quote them
// https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html#unquoted-identifiers
func reformatIdentifier(value string) string {
	if len(value) > 0 {
		if value == "default" {
			return `"DEFAULT"`
		}

		//must begin with a letter or underscore, or enclose in double quotes
		firstSymbol := value[0]

		if isNotLetterOrUnderscore(int32(firstSymbol)) {
			return `"` + value + `"`
		}

		for _, symbol := range value {
			if isNotLetterOrUnderscore(symbol) && isNotNumberOrDollar(symbol) {
				return `"` + value + `"`
			}
		}

	}

	return value
}

// _: 95
// A - Z: 65-90
// a - z: 97-122
func isNotLetterOrUnderscore(symbol int32) bool {
	return symbol < 65 || (symbol != 95 && symbol > 90 && symbol < 97) || symbol > 122
}

// $: 36
// 0 - 9: 48-57
func isNotNumberOrDollar(symbol int32) bool {
	return symbol != 36 && (symbol < 48 || symbol > 57)
}
