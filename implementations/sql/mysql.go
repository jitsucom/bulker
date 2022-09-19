package sql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jitsucom/bulker/base/errorj"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/timestamp"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/types"
	"os"
	"strings"
	"text/template"
	"time"
)

func init() {
	bulker.RegisterBulker(MySQLBulkerTypeId, NewMySQL)
}

const (
	MySQLBulkerTypeId = "mysql"

	mySQLTableSchemaQuery = `SELECT
									column_name AS name,
									column_type AS column_type
								FROM information_schema.columns
								WHERE table_schema = ? AND table_name = ?`
	mySQLPrimaryKeyFieldsQuery = `SELECT
									column_name AS name
								FROM information_schema.columns
								WHERE table_schema = ? AND table_name = ? AND column_key = 'PRI'`
	mySQLCreateDBIfNotExistsTemplate = "CREATE DATABASE IF NOT EXISTS `%s`"
	mySQLAllowLocalFile              = "SET GLOBAL local_infile = 1"
	mySQLLoadTemplate                = `LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 LINES (%s)`
	mySQLMergeQuery                  = `INSERT INTO {{.TableName}}({{.Columns}}) VALUES ({{.Placeholders}}) ON DUPLICATE KEY UPDATE {{.UpdateSet}}`
	mySQLBulkMergeQuery              = "INSERT INTO {{.TableTo}}({{.Columns}}) SELECT * FROM (SELECT {{.Columns}} FROM {{.TableFrom}}) AS S ON DUPLICATE KEY UPDATE {{.UpdateSet}}"
)

var (
	mySQLMergeQueryTemplate, _     = template.New("mysqlMergeQuery").Parse(mySQLMergeQuery)
	mySQLBulkMergeQueryTemplate, _ = template.New("mysqlBulkMergeQuery").Parse(mySQLBulkMergeQuery)

	SchemaToMySQL = map[types.DataType]string{
		types.STRING:    "TEXT",
		types.INT64:     "BIGINT",
		types.FLOAT64:   "DOUBLE",
		types.TIMESTAMP: "TIMESTAMP(6)",
		types.BOOL:      "BOOLEAN",
		types.UNKNOWN:   "TEXT",
	}

	//mySQLPrimaryKeyTypesMapping forces to use a special type in primary keys
	mySQLPrimaryKeyTypesMapping = map[string]string{
		"TEXT": "VARCHAR(255)",
	}
)

// MySQL is adapter for creating, patching (schema or table), inserting data to mySQL database
type MySQL struct {
	SQLAdapterBase[DataSourceConfig]
}

// NewMySQL returns configured MySQL adapter instance
func NewMySQL(bulkerConfig bulker.Config) (bulker.Bulker, error) {
	config := &DataSourceConfig{}
	if err := utils.ParseObject(bulkerConfig.DestinationConfig, config); err != nil {
		return nil, fmt.Errorf("failed to parse destination config: %w", err)
	}
	if _, ok := config.Parameters["tls"]; !ok {
		// similar to postgres default value of sslmode option
		config.Parameters["tls"] = "preferred"
	}
	connectionString := mySQLDriverConnectionString(config)
	dataSource, err := sql.Open("mysql", connectionString)
	if err != nil {
		return nil, err
	}

	if err := dataSource.Ping(); err != nil {
		dataSource.Close()
		return nil, err
	}

	//set default values
	dataSource.SetConnMaxLifetime(3 * time.Minute)
	dataSource.SetMaxIdleConns(10)

	tableNameFunc := func(config *DataSourceConfig, tableName string) string {
		return fmt.Sprintf("`%s`.`%s`", config.Db, tableName)
	}
	typecastFunc := func(placeholder string, column SQLColumn) string {
		return placeholder
	}
	queryLogger := logging.NewQueryLogger(bulkerConfig.Id, os.Stderr, os.Stderr)
	m := &MySQL{newSQLAdapterBase(MySQLBulkerTypeId, config, dataSource,
		queryLogger, typecastFunc, QuestionMarkParameterPlaceholder, tableNameFunc, mySQLQuoteColumnName, mySQLColumnDDL, mySQLMapColumnValue, checkErr)}
	m.batchFileFormat = CSV
	return m, nil
}

func (m *MySQL) CreateStream(id, tableName string, mode bulker.BulkMode, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	rows, err := m.dataSource.Query("SHOW GLOBAL VARIABLES LIKE 'local_infile'")
	localFileEnabled := false
	if err == nil && rows.Next() {
		varRow, _ := rowToMap(rows)
		localFileEnabled = varRow["value"] == "ON"
	}
	if !localFileEnabled {
		_, err = m.dataSource.Exec(mySQLAllowLocalFile)
		if err != nil {
			logging.Warnf("[%s] Loading tables from local batch file is disabled. Bulk loading will fallback to insert statements. To enable loading from files add to [mysql] and [mysqld] sections of my.cnf file the following line: local-infile=1", id)
		} else {
			localFileEnabled = true
		}
	}
	if localFileEnabled {
		streamOptions = append(streamOptions, withLocalBatchFile(fmt.Sprintf("bulker_%s_stream_%s_%s", mode, tableName, utils.SanitizeString(id))))
	}
	if err := m.validateOptions(streamOptions); err != nil {
		return nil, err
	}
	switch mode {
	case bulker.AutoCommit:
		return newAutoCommitStream(id, m, tableName, streamOptions...)
	case bulker.Transactional:
		return newTransactionalStream(id, m, tableName, streamOptions...)
	case bulker.ReplaceTable:
		return newReplaceTableStream(id, m, tableName, streamOptions...)
	case bulker.ReplacePartition:
		return newReplacePartitionStream(id, m, tableName, streamOptions...)
	}
	return nil, fmt.Errorf("unsupported bulk mode: %s", mode)
}

func (m *MySQL) validateOptions(streamOptions []bulker.StreamOption) error {
	options := &bulker.StreamOptions{}
	for _, option := range streamOptions {
		option(options)
	}
	return nil
}

// InitDatabase creates database instance if doesn't exist
func (m *MySQL) InitDatabase(ctx context.Context) error {
	query := fmt.Sprintf(mySQLCreateDBIfNotExistsTemplate, m.config.Db)
	if _, err := m.txOrDb(ctx).ExecContext(ctx, query); err != nil {
		return errorj.CreateSchemaError.Wrap(err, "failed to create db schema").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Database:  m.config.Db,
				Statement: query,
			})
	}

	return nil
}

func (m *MySQL) GetTypesMapping() map[types.DataType]string {
	return SchemaToMySQL
}

// OpenTx opens underline sql transaction and return wrapped instance
func (m *MySQL) OpenTx(ctx context.Context) (*TxSQLAdapter, error) {
	return m.openTx(ctx, m)
}

func (m *MySQL) Insert(ctx context.Context, table *Table, merge bool, objects []types.Object) error {
	if !merge {
		return m.insert(ctx, table, objects)
	} else {
		return m.insertOrMerge(ctx, table, objects, mySQLMergeQueryTemplate)
	}
}

func (m *MySQL) CopyTables(ctx context.Context, targetTable *Table, sourceTable *Table, merge bool) error {
	if !merge {
		return m.copy(ctx, targetTable, sourceTable)
	} else {
		return m.copyOrMerge(ctx, targetTable, sourceTable, mySQLBulkMergeQueryTemplate, "S")
	}
}

func (m *MySQL) LoadTable(ctx context.Context, targetTable *Table, loadSource *LoadSource) (err error) {
	if loadSource.Type != LocalFile {
		return fmt.Errorf("LoadTable: only local file is supported")
	}
	if loadSource.Format != m.batchFileFormat {
		return fmt.Errorf("LoadTable: only %s format is supported", m.batchFileFormat)
	}
	mysql.RegisterLocalFile(loadSource.Path)
	defer mysql.DeregisterLocalFile(loadSource.Path)

	columns := targetTable.SortedColumnNames()
	header := make([]string, len(columns))
	for i, name := range columns {
		header[i] = m.columnName(name)
	}
	loadStatement := fmt.Sprintf(mySQLLoadTemplate, loadSource.Path, m.fullTableName(targetTable.Name), strings.Join(header, ", "))
	if _, err := m.txOrDb(ctx).ExecContext(ctx, loadStatement); err != nil {
		return errorj.CopyError.Wrap(err, "failed to copy data from stage").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Database:  m.config.Db,
				Table:     targetTable.Name,
				Statement: loadStatement,
			})
	}

	return nil
}

// GetTableSchema returns table (name,columns with name and types) representation wrapped in Table struct
func (m *MySQL) GetTableSchema(ctx context.Context, tableName string) (*Table, error) {
	table, err := m.getTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	//don't select primary keys of non-existent table
	if len(table.Columns) == 0 {
		return table, nil
	}

	pkFields, err := m.getPrimaryKeys(ctx, tableName)
	if err != nil {
		return nil, err
	}

	table.PKFields = pkFields
	if len(pkFields) > 0 {
		//in MySQL primary key has always name: "PRIMARY"
		table.PrimaryKeyName = BuildConstraintName(table.Name)
	}
	return table, nil
}

func (m *MySQL) getTable(ctx context.Context, tableName string) (*Table, error) {
	table := &Table{Name: tableName, Columns: Columns{}, PKFields: utils.NewSet[string]()}
	ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()
	rows, err := m.dataSource.QueryContext(ctx, mySQLTableSchemaQuery, m.config.Db, tableName)
	if err != nil {
		return nil, errorj.GetTableError.Wrap(err, "failed to get table columns").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Database:    m.config.Db,
				Table:       table.Name,
				PrimaryKeys: table.GetPKFields(),
				Statement:   mySQLTableSchemaQuery,
				Values:      []any{m.config.Db, tableName},
			})
	}

	defer rows.Close()
	for rows.Next() {
		var columnName, columnType string
		if err := rows.Scan(&columnName, &columnType); err != nil {
			return nil, errorj.GetTableError.Wrap(err, "failed to scan result").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Database:    m.config.Db,
					Table:       table.Name,
					PrimaryKeys: table.GetPKFields(),
					Statement:   mySQLTableSchemaQuery,
					Values:      []any{m.config.Db, tableName},
				})
		}
		if columnType == "" {
			//skip dropped field
			continue
		}

		table.Columns[columnName] = SQLColumn{Type: columnType}
	}

	if err := rows.Err(); err != nil {
		return nil, errorj.GetTableError.Wrap(err, "failed read last row").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Database:    m.config.Db,
				Table:       table.Name,
				PrimaryKeys: table.GetPKFields(),
				Statement:   mySQLTableSchemaQuery,
				Values:      []any{m.config.Db, tableName},
			})
	}

	return table, nil
}

func (m *MySQL) getPrimaryKeys(ctx context.Context, tableName string) (utils.Set[string], error) {
	pkFieldsRows, err := m.dataSource.QueryContext(ctx, mySQLPrimaryKeyFieldsQuery, m.config.Db, tableName)
	if err != nil {
		return nil, errorj.GetPrimaryKeysError.Wrap(err, "failed to get primary key").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Database:  m.config.Db,
				Table:     tableName,
				Statement: mySQLPrimaryKeyFieldsQuery,
				Values:    []any{m.config.Db, tableName},
			})
	}

	defer pkFieldsRows.Close()

	pkFields := utils.NewSet[string]()
	for pkFieldsRows.Next() {
		var fieldName string
		if err := pkFieldsRows.Scan(&fieldName); err != nil {
			return nil, errorj.GetPrimaryKeysError.Wrap(err, "failed to scan result").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Database:  m.config.Db,
					Table:     tableName,
					Statement: mySQLPrimaryKeyFieldsQuery,
					Values:    []any{m.config.Db, tableName},
				})
		}
		pkFields.Put(fieldName)
	}
	if err := pkFieldsRows.Err(); err != nil {
		return nil, errorj.GetPrimaryKeysError.Wrap(err, "failed read last row").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Database:  m.config.Db,
				Table:     tableName,
				Statement: mySQLPrimaryKeyFieldsQuery,
				Values:    []any{m.config.Db, tableName},
			})
	}

	return pkFields, nil
}

func (m *MySQL) ReplaceTable(ctx context.Context, originalTable, replacementTable string, dropOldTable bool) (err error) {
	tmpTable := "deprecated_" + originalTable + timestamp.Now().Format("_20060102_150405")
	err1 := m.renameTable(ctx, true, originalTable, tmpTable)
	err = m.renameTable(ctx, false, replacementTable, originalTable)
	if dropOldTable && err1 == nil && err == nil {
		return m.DropTable(ctx, tmpTable, true)
	}
	return
}

func (m *MySQL) renameTable(ctx context.Context, ifExists bool, tableName, newTableName string) error {
	if ifExists {
		row := m.txOrDb(ctx).QueryRowContext(ctx, fmt.Sprintf(`SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s')`, m.config.Db, tableName))
		exists := false
		err := row.Scan(&exists)
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}
	}
	return m.SQLAdapterBase.renameTable(ctx, false, tableName, newTableName)
}

func mySQLDriverConnectionString(config *DataSourceConfig) string {
	// [user[:password]@][net[(addr)]]/dbname[?param1=value1&paramN=valueN]
	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		config.Username, config.Password, config.Host, config.Port, config.Db)
	if len(config.Parameters) > 0 {
		connectionString += "?"
		paramList := make([]string, 10)
		//concat provided connection parameters
		for k, v := range config.Parameters {
			paramList = append(paramList, k+"="+v)
		}
		connectionString += strings.Join(paramList, "&")
	}
	return connectionString
}

// mySQLColumnDDL returns column DDL (quoted column name, mapped sql type and 'not null' if pk field)
func mySQLColumnDDL(name string, column SQLColumn, pkFields utils.Set[string]) string {
	sqlType := column.GetDDLType()

	//map special types for primary keys (text -> varchar)
	//because old versions of MYSQL requires non null and default value on TEXT types
	if _, ok := pkFields[name]; ok {
		if typeForPKField, ok := mySQLPrimaryKeyTypesMapping[sqlType]; ok {
			sqlType = typeForPKField
		}
	}

	return fmt.Sprintf("%s %s", mySQLQuoteColumnName(name), sqlType)
}

func mySQLQuoteColumnName(str string) string {
	return fmt.Sprintf("`%s`", str)
}

func mySQLMapColumnValue(value any, valuePresent bool, column SQLColumn) any {
	if datetime, ok := value.(time.Time); ok {
		if datetime.IsZero() {
			// workaround for time.Time{} default value because of mysql driver internals
			return time.Date(1, 1, 1, 0, 0, 0, 1, time.UTC)
		}
	}
	return value
}
