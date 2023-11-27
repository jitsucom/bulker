package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/hashicorp/go-multierror"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	types2 "github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/errorj"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/jitsucom/bulker/jitsubase/timestamp"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"os"
	"path"
	"regexp"
	"strings"
	"text/template"
	"time"

	sf "github.com/snowflakedb/gosnowflake"
)

func init() {
	bulker.RegisterBulker(SnowflakeBulkerTypeId, NewSnowflake)
}

const (
	SnowflakeBulkerTypeId = "snowflake"

	sfTableExistenceQuery        = `SELECT count(*) from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA = ? and TABLE_NAME = ?`
	sfDescTableQuery             = `desc table %s`
	sfAlterClusteringKeyTemplate = `ALTER TABLE %s CLUSTER BY (DATE_TRUNC('MONTH', %s))`

	sfCopyStatement = `COPY INTO %s (%s) from @~/%s FILE_FORMAT=(TYPE= 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE SKIP_HEADER = 1) `

	sfMergeStatement = `MERGE INTO {{.TableTo}} T USING (SELECT {{.Columns}} FROM {{.TableFrom}} ) S ON {{.JoinConditions}} WHEN MATCHED THEN UPDATE SET {{.UpdateSet}} WHEN NOT MATCHED THEN INSERT ({{.Columns}}) VALUES ({{.SourceColumns}})`

	sfCreateSchemaIfNotExistsTemplate = `CREATE SCHEMA IF NOT EXISTS %s`

	sfPrimaryKeyFieldsQuery = `show primary keys in %s`
)

var (
	sfReservedWords             = []string{"all", "alter", "and", "any", "as", "between", "by", "case", "cast", "check", "column", "connect", "constraint", "create", "cross", "current", "current_date", "current_time", "current_timestamp", "current_user", "delete", "distinct", "drop", "else", "exists", "false", "following", "for", "from", "full", "grant", "group", "having", "ilike", "in", "increment", "inner", "insert", "intersect", "into", "is", "join", "lateral", "left", "like", "localtime", "localtimestamp", "minus", "natural", "not", "null", "of", "on", "or", "order", "qualify", "regexp", "revoke", "right", "rlike", "row", "rows", "sample", "select", "set", "some", "start", "table", "tablesample", "then", "to", "trigger", "true", "try_cast", "union", "unique", "update", "using", "values", "when", "whenever", "where", "with"}
	sfReservedWordsSet          = utils.NewSet(sfReservedWords...)
	sfUnquotedIdentifierPattern = regexp.MustCompile(`^[a-z_][0-9a-z_]*$|^[A-Z_][0-9A-Z_]*$`)

	sfMergeQueryTemplate, _ = template.New("snowflakeMergeQuery").Parse(sfMergeStatement)

	snowflakeTypes = map[types2.DataType][]string{
		types2.STRING:    {"text", "VARCHAR(16777216)", "VARCHAR"},
		types2.INT64:     {"bigint", "NUMBER(38,0)", "NUMBER"},
		types2.FLOAT64:   {"double precision", "FLOAT"},
		types2.TIMESTAMP: {"TIMESTAMP_TZ(6)", "timestamp(6)", "TIMESTAMP_NTZ(6)", "TIMESTAMP"},
		types2.BOOL:      {"boolean", "BOOLEAN"},
		types2.JSON:      {"text", "VARCHAR(16777216)"},
		types2.UNKNOWN:   {"text", "VARCHAR(16777216)"},
	}
)

// SnowflakeConfig dto for deserialized datasource config for Snowflake
type SnowflakeConfig struct {
	Account    string             `mapstructure:"account,omitempty" json:"account,omitempty" yaml:"account,omitempty"`
	Port       int                `mapstructure:"port,omitempty" json:"port,omitempty" yaml:"port,omitempty"`
	Db         string             `mapstructure:"database,omitempty" json:"database,omitempty" yaml:"database,omitempty"`
	Schema     string             `mapstructure:"defaultSchema,omitempty" json:"defaultSchema,omitempty" yaml:"defaultSchema,omitempty"`
	Username   string             `mapstructure:"username,omitempty" json:"username,omitempty" yaml:"username,omitempty"`
	Password   string             `mapstructure:"password,omitempty" json:"password,omitempty" yaml:"password,omitempty"`
	Warehouse  string             `mapstructure:"warehouse,omitempty" json:"warehouse,omitempty" yaml:"warehouse,omitempty"`
	Parameters map[string]*string `mapstructure:"parameters,omitempty" json:"parameters,omitempty" yaml:"parameters,omitempty"`
}

func init() {
	for _, word := range sfReservedWords {
		sfReservedWordsSet.Put(strings.ToUpper(word))
	}
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

	return nil
}

// Snowflake is adapter for creating,patching (schema or table), inserting data to snowflake
type Snowflake struct {
	*SQLAdapterBase[SnowflakeConfig]
}

// NewSnowflake returns configured Snowflake adapter instance
func NewSnowflake(bulkerConfig bulker.Config) (bulker.Bulker, error) {
	config := &SnowflakeConfig{}
	if err := utils.ParseObject(bulkerConfig.DestinationConfig, config); err != nil {
		return nil, fmt.Errorf("failed to parse destination config: %v", err)
	}

	if config.Parameters == nil {
		config.Parameters = map[string]*string{}
	}
	minute := "60"
	utils.MapPutIfAbsent(config.Parameters, "loginTimeout", &minute)
	utils.MapPutIfAbsent(config.Parameters, "requestTimeout", &minute)
	utils.MapPutIfAbsent(config.Parameters, "clientTimeout", &minute)

	dbConnectFunction := func(config *SnowflakeConfig) (*sql.DB, error) {
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
		dataSource.SetConnMaxIdleTime(3 * time.Minute)
		dataSource.SetMaxIdleConns(10)
		return dataSource, nil
	}
	typecastFunc := func(placeholder string, column types2.SQLColumn) string {
		if column.Override {
			return placeholder + "::" + column.Type
		}
		return placeholder
	}
	var queryLogger *logging.QueryLogger
	if bulkerConfig.LogLevel == bulker.Verbose {
		queryLogger = logging.NewQueryLogger(bulkerConfig.Id, os.Stderr, os.Stderr)
	}
	sqlAdapter, err := newSQLAdapterBase(bulkerConfig.Id, SnowflakeBulkerTypeId, config, dbConnectFunction, snowflakeTypes, queryLogger, typecastFunc, QuestionMarkParameterPlaceholder, sfColumnDDL, unmappedValue, checkErr)
	s := &Snowflake{sqlAdapter}
	s.batchFileFormat = types2.FileFormatCSV
	s.valueMappingFunction = func(value any, valuePresent bool, column types2.SQLColumn) any {
		if !valuePresent {
			return nil
		}
		l := strings.ToLower(column.Type)
		if _, ok := value.(time.Time); ok && strings.Contains(l, "timestamp") {
			return value.(time.Time).Format(time.RFC3339Nano)
		}
		return value
	}

	s.tableHelper = NewTableHelper(255, '"')
	s.tableHelper.tableNameFunc = sfIdentifierFunction
	s.tableHelper.columnNameFunc = sfIdentifierFunction
	return s, err
}
func (s *Snowflake) CreateStream(id, tableName string, mode bulker.BulkMode, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	streamOptions = append(streamOptions, withLocalBatchFile(fmt.Sprintf("bulker_%s", utils.SanitizeString(id))))

	if err := s.validateOptions(streamOptions); err != nil {
		return nil, err
	}
	switch mode {
	case bulker.Stream:
		return newAutoCommitStream(id, s, tableName, streamOptions...)
	case bulker.Batch:
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
		options.Add(option)
	}
	return nil
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
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Schema:    s.config.Schema,
				Statement: query,
			})
	}

	return nil
}

// GetTableSchema returns table (name,columns with name and types) representation wrapped in Table struct
func (s *Snowflake) GetTableSchema(ctx context.Context, tableName string) (*Table, error) {
	quotedTableName, tableName := s.tableHelper.adaptTableName(tableName)
	table := &Table{Name: tableName, Columns: Columns{}, PKFields: utils.NewSet[string]()}

	query := fmt.Sprintf(sfDescTableQuery, quotedTableName)
	rows, err := s.txOrDb(ctx).QueryContext(ctx, query)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return table, nil
		}
		return nil, errorj.GetTableError.Wrap(err, "failed to get table columns").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Schema:    s.config.Schema,
				Table:     quotedTableName,
				Statement: query,
			})
	}
	defer rows.Close()

	for rows.Next() {
		var row map[string]any
		row, err = rowToMap(rows)
		if err != nil {
			return nil, errorj.GetTableError.Wrap(err, "failed to scan result").
				WithProperty(errorj.DBInfo, &types2.ErrorPayload{
					Schema:    s.config.Schema,
					Table:     quotedTableName,
					Statement: query,
				})
		}

		columnName := fmt.Sprint(row["name"])
		columnSnowflakeType := fmt.Sprint(row["type"])
		dt, _ := s.GetDataType(columnSnowflakeType)
		table.Columns[columnName] = types2.SQLColumn{Type: columnSnowflakeType, DataType: dt}
	}
	if err := rows.Err(); err != nil {
		return nil, errorj.GetTableError.Wrap(err, "failed read last row").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Schema:    s.config.Schema,
				Table:     quotedTableName,
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
	if primaryKeyName != "" && strings.ToLower(primaryKeyName) != strings.ToLower(jitsuPrimaryKeyName) {
		logging.Debugf("table: %s has a custom primary key with name: %s that isn't managed by Jitsu. Custom primary key will be used in rows deduplication and updates. primary_key_fields configuration provided in Jitsu config will be ignored.", table.Name, primaryKeyName)
	}

	return table, nil
}

// getPrimaryKey returns primary key name and fields
func (s *Snowflake) getPrimaryKey(ctx context.Context, tableName string) (string, utils.Set[string], error) {
	quotedTableName := s.quotedTableName(tableName)

	primaryKeys := utils.Set[string]{}
	statement := fmt.Sprintf(sfPrimaryKeyFieldsQuery, quotedTableName)
	pkFieldsRows, err := s.txOrDb(ctx).QueryContext(ctx, statement)
	if err != nil {
		return "", nil, errorj.GetPrimaryKeysError.Wrap(err, "failed to get primary key").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Schema:    s.config.Schema,
				Table:     quotedTableName,
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
				WithProperty(errorj.DBInfo, &types2.ErrorPayload{
					Schema:    s.config.Schema,
					Table:     quotedTableName,
					Statement: statement,
				})
		}
		constraintName, ok := row["constraint_name"].(string)
		if primaryKeyName == "" && ok {
			primaryKeyName = constraintName
		}
		keyColumn, ok := row["column_name"].(string)
		if ok && keyColumn != "" {
			pkFields = append(pkFields, keyColumn)
		}
	}

	if err := pkFieldsRows.Err(); err != nil {
		return "", nil, errorj.GetPrimaryKeysError.Wrap(err, "failed read last row").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Schema:    s.config.Schema,
				Table:     quotedTableName,
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
	quotedTableName := s.quotedTableName(targetTable.Name)

	if loadSource.Type != LocalFile {
		return fmt.Errorf("LoadTable: only local file is supported")
	}
	if loadSource.Format != s.batchFileFormat {
		return fmt.Errorf("LoadTable: only %s format is supported", s.batchFileFormat)
	}
	putStatement := fmt.Sprintf("PUT file://%s @~", loadSource.Path)
	if _, err = s.txOrDb(ctx).ExecContext(ctx, putStatement); err != nil {
		return errorj.LoadError.Wrap(err, "failed to put file to stage").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Schema:    s.config.Schema,
				Table:     quotedTableName,
				Statement: putStatement,
			})
	}
	defer func() {
		removeStatement := fmt.Sprintf("REMOVE @~/%s", path.Base(loadSource.Path))
		if _, err2 := s.txOrDb(ctx).ExecContext(ctx, removeStatement); err2 != nil {
			err2 = errorj.LoadError.Wrap(err, "failed to remove file from stage").
				WithProperty(errorj.DBInfo, &types2.ErrorPayload{
					Schema:    s.config.Schema,
					Table:     quotedTableName,
					Statement: putStatement,
				})
			err = multierror.Append(err, err2)
		}
	}()
	columns := targetTable.SortedColumnNames()
	columnNames := make([]string, len(columns))
	for i, name := range columns {
		columnNames[i] = s.quotedColumnName(name)
	}

	statement := fmt.Sprintf(sfCopyStatement, quotedTableName, strings.Join(columnNames, ","), path.Base(loadSource.Path))

	if _, err := s.txOrDb(ctx).ExecContext(ctx, statement); err != nil {
		return errorj.CopyError.Wrap(err, "failed to copy data from stage").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Schema:    s.config.Schema,
				Table:     quotedTableName,
				Statement: statement,
			})
	}

	return nil
}

// Insert inserts data with InsertContext as a single object or a batch into Snowflake
func (s *Snowflake) Insert(ctx context.Context, table *Table, merge bool, objects ...types2.Object) error {
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
		res, err := s.SQLAdapterBase.Select(ctx, table.Name, pkMatchConditions, nil)
		if err != nil {
			return errorj.ExecuteInsertError.Wrap(err, "failed check primary key collision").
				WithProperty(errorj.DBInfo, &types2.ErrorPayload{
					Schema:      s.config.Schema,
					Table:       table.Name,
					PrimaryKeys: table.GetPKFields(),
				})
		}
		if len(res) > 0 {
			return s.Update(ctx, table, object, pkMatchConditions)
		} else {
			return s.insert(ctx, table, []types2.Object{object})
		}
	}
	return nil
}

func (s *Snowflake) CopyTables(ctx context.Context, targetTable *Table, sourceTable *Table, mergeWindow int) error {
	if mergeWindow <= 0 {
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
func sfColumnDDL(quotedName, name string, table *Table) string {
	column := table.Columns[name]
	return fmt.Sprintf(`%s %s`, quotedName, column.GetDDLType())
}

func (s *Snowflake) Select(ctx context.Context, tableName string, whenConditions *WhenConditions, orderBy []string) ([]map[string]any, error) {
	ctx = sf.WithHigherPrecision(ctx)
	return s.SQLAdapterBase.Select(ctx, tableName, whenConditions, orderBy)
}

func sfIdentifierFunction(value string) (adapted string, needQuotes bool) {
	if sfReservedWordsSet.Contains(value) {
		return strings.ToUpper(value), true
	}
	if !sfUnquotedIdentifierPattern.MatchString(value) {
		return value, true
	} else {
		return strings.ToUpper(value), false
	}
}

func (s *Snowflake) CreateTable(ctx context.Context, schemaToCreate *Table) error {
	err := s.SQLAdapterBase.CreateTable(ctx, schemaToCreate)
	if err != nil {
		return err
	}
	if !schemaToCreate.Temporary && schemaToCreate.TimestampColumn != "" {
		err = s.createClusteringKey(ctx, schemaToCreate)
		if err != nil {
			s.DropTable(ctx, schemaToCreate.Name, true)
			return fmt.Errorf("failed to create sort key: %v", err)
		}
	}
	return nil
}

func (s *Snowflake) createClusteringKey(ctx context.Context, table *Table) error {
	if table.TimestampColumn == "" {
		return nil
	}
	quotedTableName := s.quotedTableName(table.Name)

	statement := fmt.Sprintf(sfAlterClusteringKeyTemplate,
		quotedTableName, s.quotedColumnName(table.TimestampColumn))

	if _, err := s.txOrDb(ctx).ExecContext(ctx, statement); err != nil {
		return errorj.AlterTableError.Wrap(err, "failed to set clustering key").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Table:       quotedTableName,
				PrimaryKeys: table.GetPKFields(),
				Statement:   statement,
			})
	}

	return nil
}
