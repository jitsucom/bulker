package sql

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"fmt"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	types2 "github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/errorj"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/jitsucom/bulker/jitsubase/utils"
	jsoniter "github.com/json-iterator/go"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"text/template"
	"time"

	_ "github.com/Kount/pq-timeouts"
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
	pgCreateIndexTemplate               = `CREATE INDEX ON %s (%s);`

	pgMergeQuery = `INSERT INTO {{.TableName}}({{.Columns}}) VALUES ({{.Placeholders}}) ON CONFLICT ON CONSTRAINT {{.PrimaryKeyName}} DO UPDATE set {{.UpdateSet}}`

	pgCopyTemplate = `COPY %s(%s) FROM STDIN`

	pgBulkMergeQuery       = `INSERT INTO {{.TableTo}}({{.Columns}}) SELECT {{.Columns}} FROM {{.TableFrom}} ON CONFLICT ON CONSTRAINT {{.PrimaryKeyName}} DO UPDATE SET {{.UpdateSet}}`
	pgBulkMergeSourceAlias = `excluded`
)

var (
	pgMergeQueryTemplate, _     = template.New("postgresMergeQuery").Parse(pgMergeQuery)
	pgBulkMergeQueryTemplate, _ = template.New("postgresBulkMergeQuery").Parse(pgBulkMergeQuery)

	postgresDataTypes = map[types2.DataType][]string{
		types2.STRING:    {"text"},
		types2.INT64:     {"bigint"},
		types2.FLOAT64:   {"double precision"},
		types2.TIMESTAMP: {"timestamp with time zone", "timestamp", "timestamp without time zone"},
		types2.BOOL:      {"boolean"},
		types2.JSON:      {"jsonb"},
		types2.UNKNOWN:   {"text"},
	}
)

type PostgresConfig struct {
	DataSourceConfig `mapstructure:",squash"`
	SSLConfig        `mapstructure:",squash"`
}

// Postgres is adapter for creating,patching (schema or table), inserting data to postgres
type Postgres struct {
	SQLAdapterBase[PostgresConfig]
	dbConnectFunction DbConnectFunction[PostgresConfig]
	tmpDir            string
}

// NewPostgres return configured Postgres bulker.Bulker instance
func NewPostgres(bulkerConfig bulker.Config) (bulker.Bulker, error) {
	config := &PostgresConfig{}
	if err := utils.ParseObject(bulkerConfig.DestinationConfig, config); err != nil {
		return nil, fmt.Errorf("failed to parse destination config: %w", err)
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	//create tmp dir for ssl certs if any
	tmpDir, err := os.MkdirTemp("", "postgres_"+bulkerConfig.Id)
	if err != nil && (config.SSLMode == "verify-ca" || config.SSLMode == "verify-full") {
		return nil, fmt.Errorf("failed to create tmp dir for postgres ssl certs: %w", err)
	}
	if err = ProcessSSL(tmpDir, config); err != nil {
		return nil, err
	}
	if config.Parameters == nil {
		config.Parameters = map[string]string{}
	}
	utils.MapPutIfAbsent(config.Parameters, "connect_timeout", "60")
	utils.MapPutIfAbsent(config.Parameters, "write_timeout", "60000")
	utils.MapPutIfAbsent(config.Parameters, "read_timeout", "60000")

	typecastFunc := func(placeholder string, column types2.SQLColumn) string {
		if column.Override {
			return placeholder + "::" + column.Type
		}
		return placeholder
	}
	valueMappingFunc := func(value any, valuePresent bool, sqlColumn types2.SQLColumn) any {
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

	dbConnectFunction := func(cfg *PostgresConfig) (*sql.DB, error) {
		connectionString := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s search_path=%s",
			config.Host, config.Port, config.Db, config.Username, config.Password, config.Schema)
		//concat provided connection parameters
		for k, v := range config.Parameters {
			connectionString += " " + k + "=" + v + " "
		}
		logging.Infof("[%s] connecting: %s", bulkerConfig.Id, connectionString)

		dataSource, err := sql.Open("pq-timeouts", connectionString)
		if err != nil {
			return nil, err
		}
		if err := dataSource.Ping(); err != nil {
			_ = dataSource.Close()
			return nil, err
		}
		dataSource.SetConnMaxIdleTime(3 * time.Minute)
		dataSource.SetMaxIdleConns(10)
		return dataSource, nil
	}
	sqlAdapterBase, err := newSQLAdapterBase(bulkerConfig.Id, PostgresBulkerTypeId, config, dbConnectFunction, postgresDataTypes, queryLogger, typecastFunc, IndexParameterPlaceholder, pgColumnDDL, valueMappingFunc, checkErr)
	p := &Postgres{sqlAdapterBase, dbConnectFunction, tmpDir}
	p.tableHelper = NewTableHelper(p, 63, '"')
	return p, err
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
		options.Add(option)
	}
	return nil
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
		p.Warnf("table: %s has a custom primary key with name: %s that isn't managed by Jitsu. Custom primary key will be used in rows deduplication and updates. primary_key_fields configuration provided in Jitsu config will be ignored.", table.Name, primaryKeyName)
	}
	return table, nil
}

func (p *Postgres) getTable(ctx context.Context, tableName string) (*Table, error) {
	tableName = p.TableName(tableName)
	table := &Table{Name: tableName, Columns: map[string]types2.SQLColumn{}, PKFields: utils.Set[string]{}}
	rows, err := p.txOrDb(ctx).QueryContext(ctx, pgTableSchemaQuery, p.config.Schema, tableName)
	if err != nil {
		return nil, errorj.GetTableError.Wrap(err, "failed to get table columns").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
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
				WithProperty(errorj.DBInfo, &types2.ErrorPayload{
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
		dt, _ := p.GetDataType(columnPostgresType)
		table.Columns[columnName] = types2.SQLColumn{Type: columnPostgresType, DataType: dt}
	}

	if err := rows.Err(); err != nil {
		return nil, errorj.GetTableError.Wrap(err, "failed read last row").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Schema:      p.config.Schema,
				Table:       tableName,
				PrimaryKeys: table.GetPKFields(),
				Statement:   pgTableSchemaQuery,
				Values:      []any{p.config.Schema, tableName},
			})
	}

	return table, nil
}

func (p *Postgres) Insert(ctx context.Context, table *Table, merge bool, objects ...types2.Object) error {
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
				WithProperty(errorj.DBInfo, &types2.ErrorPayload{
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
	//p.Infof("FILE: %s", f)

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
			l := types2.ReformatValue(object[v])
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
func pgColumnDDL(quotedName, name string, table *Table) string {
	var notNullClause string
	column := table.Columns[name]
	sqlType := column.GetDDLType()

	//not null
	if _, ok := table.PKFields[name]; ok {
		notNullClause = " not null " + getDefaultValueStatement(sqlType)
	}

	return fmt.Sprintf(`%s %s%s`, quotedName, sqlType, notNullClause)
}

// return default value statement for creating column
func getDefaultValueStatement(sqlType string) string {
	sqlType = strings.ToLower(sqlType)
	//get default value based on type
	if strings.Contains(sqlType, "var") || strings.Contains(sqlType, "text") {
		return "default ''"
	}
	if strings.Contains(sqlType, "timestamp") {
		return "default CURRENT_TIMESTAMP"
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
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
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
				WithProperty(errorj.DBInfo, &types2.ErrorPayload{
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
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
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

func (p *Postgres) CreateTable(ctx context.Context, schemaToCreate *Table) error {
	err := p.SQLAdapterBase.CreateTable(ctx, schemaToCreate)
	if err != nil {
		return err
	}
	if !schemaToCreate.Temporary && schemaToCreate.TimestampColumn != "" {
		err = p.createIndex(ctx, schemaToCreate)
		if err != nil {
			p.DropTable(ctx, schemaToCreate.Name, true)
			return fmt.Errorf("failed to create sort key: %v", err)
		}
	}
	return nil
}

func (p *Postgres) createIndex(ctx context.Context, table *Table) error {
	if table.TimestampColumn == "" {
		return nil
	}
	quotedTableName := p.quotedTableName(table.Name)

	statement := fmt.Sprintf(pgCreateIndexTemplate,
		quotedTableName, p.quotedColumnName(table.TimestampColumn))

	if _, err := p.txOrDb(ctx).ExecContext(ctx, statement); err != nil {
		return errorj.AlterTableError.Wrap(err, "failed to set sort key").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Table:       quotedTableName,
				PrimaryKeys: table.GetPKFields(),
				Statement:   statement,
			})
	}

	return nil
}

// Close underlying sql.DB
func (p *Postgres) Close() error {
	if p.tmpDir != "" {
		if err := os.RemoveAll(p.tmpDir); err != nil {
			logging.Errorf("failed to remove tmp dir '%s': %v", p.tmpDir, err)
		}
	}
	return p.SQLAdapterBase.Close()
}

const (
	SSLModeRequire    string = "require"
	SSLModeDisable    string = "disable"
	SSLModeVerifyCA   string = "verify-ca"
	SSLModeVerifyFull string = "verify-full"

	SSLModeNotProvided string = ""
)

// SSLConfig is a dto for deserialized SSL configuration for Postgres
type SSLConfig struct {
	SSLMode       string `mapstructure:"sslMode,omitempty"`
	SSLServerCA   string `mapstructure:"sslServerCA,omitempty"`
	SSLClientCert string `mapstructure:"sslClientCert,omitempty"`
	SSLClientKey  string `mapstructure:"sslClientKey,omitempty"`
}

// ValidateSSL returns err if the ssl configuration is invalid
func (sc *SSLConfig) ValidateSSL() error {
	if sc == nil {
		return fmt.Errorf("ssl config is required")
	}

	if sc.SSLMode == SSLModeVerifyCA || sc.SSLMode == SSLModeVerifyFull {
		if sc.SSLServerCA == "" {
			return fmt.Errorf("'sslServerCA' is required parameter for sslMode '%s'", sc.SSLMode)
		}

		if sc.SSLClientCert == "" {
			return fmt.Errorf("'sslClientCert' is required parameter for sslMode '%s'", sc.SSLMode)
		}

		if sc.SSLClientKey == "" {
			return fmt.Errorf("'sslClientKey' is required parameter for sslMode '%s'", sc.SSLMode)
		}
	}

	return nil
}

// ProcessSSL serializes SSL payload (ca, client cert, key) into files
// enriches input DataSourceConfig parameters with SSL config
// ssl configuration might be file path as well as string content
func ProcessSSL(dir string, dsc *PostgresConfig) error {
	dsc.ValidateSSL()

	if dsc.SSLMode == SSLModeNotProvided {
		//default driver ssl mode
		return nil
	}

	dsc.Parameters["sslmode"] = dsc.SSLMode

	switch dsc.SSLMode {
	case SSLModeRequire, SSLModeDisable:
		//other parameters aren't required for 'disable' and 'require' mode
		return nil
	case SSLModeVerifyCA, SSLModeVerifyFull:
		serverCAPath, err := getSSLFilePath("server_ca", dir, dsc.SSLServerCA)
		if err != nil {
			return fmt.Errorf("error saving server_ca: %v", err)
		}
		dsc.Parameters["sslrootcert"] = serverCAPath

		clientCertPath, err := getSSLFilePath("client_cert", dir, dsc.SSLClientCert)
		if err != nil {
			return fmt.Errorf("error saving client_cert: %v", err)
		}
		dsc.Parameters["sslcert"] = clientCertPath

		clientKeyPath, err := getSSLFilePath("client_key", dir, dsc.SSLClientKey)
		if err != nil {
			return fmt.Errorf("error saving client_key: %v", err)
		}
		dsc.Parameters["sslkey"] = clientKeyPath
	default:
		return fmt.Errorf("unsupported ssl mode: %s", dsc.SSLMode)
	}
	return nil
}

// getSSLFilePath checks if input payload is filepath - returns it
// otherwise write payload as a file and returns abs file path
func getSSLFilePath(name, dir, payload string) (string, error) {
	if path.IsAbs(payload) {
		return payload, nil
	}

	filepath := path.Join(dir, name)
	err := ioutil.WriteFile(filepath, []byte(payload), 0600)
	return filepath, err
}
