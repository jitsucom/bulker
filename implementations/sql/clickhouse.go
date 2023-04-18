package sql

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jitsucom/bulker/base/errorj"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/implementations"
	"github.com/jitsucom/bulker/types"
	jsoniter "github.com/json-iterator/go"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// TODO: second test for http
// TODO: option Optimize table on Complete ?
// TODO: add flag &mutations_sync=2
func init() {
	bulker.RegisterBulker(ClickHouseBulkerTypeId, NewClickHouse)
}

// TODO tmp table
const (
	ClickHouseBulkerTypeId = "clickhouse"

	chLocalPrefix = "local_"

	chDatabaseQuery = "SELECT name FROM system.databases where name = ?"
	chClusterQuery  = "SELECT max(shard_num) > 1 FROM system.clusters where cluster = ?"

	chTableSchemaQuery       = `SELECT name, type, is_in_primary_key FROM system.columns WHERE database = ? and table = ? and default_kind not in ('MATERIALIZED', 'ALIAS', 'EPHEMERAL')`
	chCreateDatabaseTemplate = `CREATE DATABASE IF NOT EXISTS "%s" %s`

	chOnClusterClauseTemplate = " ON CLUSTER `%s` "
	chNullableColumnTemplate  = ` Nullable(%s) `

	chCreateDistributedTableTemplate = `CREATE TABLE %s %s AS %s ENGINE = Distributed(%s,%s,%s,%s)`
	chAlterTableTemplate             = `ALTER TABLE %s %s %s`
	chDeleteBeforeBulkMergeUsing     = `ALTER TABLE %s %s DELETE WHERE %s in (select %s from %s)`
	//chDeleteBeforeBulkMergeUsing = `DELETE FROM %s %s WHERE %s in (select %s from %s)`

	chDeleteQueryTemplate = `ALTER TABLE %s %s DELETE WHERE %s`

	chCreateTableTemplate   = `CREATE TABLE %s %s (%s) %s %s %s %s`
	chDropTableTemplate     = `DROP TABLE %s%s %s`
	chTruncateTableTemplate = `TRUNCATE TABLE IF EXISTS %s %s`
	chExchangeTableTemplate = `EXCHANGE TABLES %s AND %s %s`
	chRenameTableTemplate   = `RENAME TABLE %s TO %s %s`

	chSelectFinalStatement = `SELECT %s FROM %s FINAL %s%s`
	chLoadStatement        = `INSERT INTO %s (%s) VALUES %s`
)

var (
	clickhouseTypes = map[types.DataType][]string{
		types.STRING:    {"String"},
		types.INT64:     {"Int64"},
		types.FLOAT64:   {"Float64"},
		types.TIMESTAMP: {"DateTime64(6)"},
		types.BOOL:      {"UInt8"},
		types.JSON:      {"String"},
		types.UNKNOWN:   {"String"},
	}

	defaultValues = map[string]interface{}{
		"int8":                          0,
		"int16":                         0,
		"int32":                         0,
		"int64":                         0,
		"int128":                        0,
		"int256":                        0,
		"float32":                       0.0,
		"float64":                       0.0,
		"decimal":                       0.0,
		"numeric":                       0.0,
		"datetime":                      time.Time{},
		"datetime64(6)":                 time.Time{},
		"uint8":                         false,
		"uint16":                        0,
		"uint32":                        0,
		"uint64":                        0,
		"uint128":                       0,
		"uint256":                       0,
		"string":                        "",
		"lowcardinality(int8)":          0,
		"lowcardinality(int16)":         0,
		"lowcardinality(int32)":         0,
		"lowcardinality(int64)":         0,
		"lowcardinality(int128)":        0,
		"lowcardinality(int256)":        0,
		"lowcardinality(float32)":       0,
		"lowcardinality(float64)":       0,
		"lowcardinality(datetime)":      time.Time{},
		"lowcardinality(datetime64(6))": time.Time{},
		"lowcardinality(uint8)":         false,
		"lowcardinality(uint16)":        0,
		"lowcardinality(uint32)":        0,
		"lowcardinality(uint64)":        0,
		"lowcardinality(uint128)":       0,
		"lowcardinality(uint256)":       0,
		"lowcardinality(string)":        "",
		"uuid":                          "00000000-0000-0000-0000-000000000000",
	}
	nonLettersCharacters = regexp.MustCompile(`[^a-zA-Z0-9_]`)
)

type ClickHouseProtocol string

const (
	ClickHouseProtocolNative ClickHouseProtocol = "clickhouse"
	ClickHouseProtocolSecure ClickHouseProtocol = "clickhouse-secure"
	ClickHouseProtocolHTTP   ClickHouseProtocol = "http"
	ClickHouseProtocolHTTPS  ClickHouseProtocol = "https"
)

// ClickHouseConfig dto for deserialized clickhouse config
type ClickHouseConfig struct {
	Protocol   ClickHouseProtocol `mapstructure:"protocol,omitempty" json:"protocol,omitempty" yaml:"protocol,omitempty"`
	Hosts      []string           `mapstructure:"hosts,omitempty" json:"hosts,omitempty" yaml:"hosts,omitempty"`
	Parameters map[string]string  `mapstructure:"parameters,omitempty" json:"parameters,omitempty" yaml:"parameters,omitempty"`
	Username   string             `mapstructure:"username,omitempty" json:"username,omitempty" yaml:"username,omitempty"`
	Password   string             `mapstructure:"password,omitempty" json:"password,omitempty" yaml:"password,omitempty"`
	Database   string             `mapstructure:"database,omitempty" json:"database,omitempty" yaml:"database,omitempty"`
	Cluster    string             `mapstructure:"cluster,omitempty" json:"cluster,omitempty" yaml:"cluster,omitempty"`
	TLS        map[string]string  `mapstructure:"tls,omitempty" json:"tls,omitempty" yaml:"tls,omitempty"`
	Engine     *EngineConfig      `mapstructure:"engine,omitempty" json:"engine,omitempty" yaml:"engine,omitempty"`
}

// EngineConfig dto for deserialized clickhouse engine config
type EngineConfig struct {
	RawStatement    string        `mapstructure:"rawStatement,omitempty" json:"rawStatement,omitempty" yaml:"rawStatement,omitempty"`
	NullableFields  []string      `mapstructure:"nullableFields,omitempty" json:"nullableFields,omitempty" yaml:"nullableFields,omitempty"`
	PartitionFields []FieldConfig `mapstructure:"partitionFields,omitempty" json:"partitionFields,omitempty" yaml:"partitionFields,omitempty"`
	OrderFields     []FieldConfig `mapstructure:"orderFields,omitempty" json:"orderFields,omitempty" yaml:"orderFields,omitempty"`
	PrimaryKeys     []string      `mapstructure:"primaryKeys,omitempty" json:"primaryKeys,omitempty" yaml:"primaryKeys,omitempty"`
}

// FieldConfig dto for deserialized clickhouse engine fields
type FieldConfig struct {
	Function string `mapstructure:"function,omitempty" json:"function,omitempty" yaml:"function,omitempty"`
	Field    string `mapstructure:"field,omitempty" json:"field,omitempty" yaml:"field,omitempty"`
}

// ClickHouse is adapter for creating,patching (schema or table), inserting data to clickhouse
type ClickHouse struct {
	SQLAdapterBase[ClickHouseConfig]
	httpMode              bool
	distributed           bool
	tableStatementFactory *TableStatementFactory
}

// NewClickHouse returns configured ClickHouse adapter instance
func NewClickHouse(bulkerConfig bulker.Config) (bulker.Bulker, error) {
	config := &ClickHouseConfig{}
	if err := utils.ParseObject(bulkerConfig.DestinationConfig, config); err != nil {
		return nil, fmt.Errorf("failed to parse destination config: %w", err)
	}
	err := config.Validate()
	if err != nil {
		return nil, err
	}
	httpMode := false
	if config.Protocol == ClickHouseProtocolHTTP || config.Protocol == ClickHouseProtocolHTTPS {
		httpMode = true
	}
	if config.Parameters == nil {
		config.Parameters = map[string]string{}
	}
	if config.Protocol == ClickHouseProtocolSecure || config.Protocol == ClickHouseProtocolHTTPS {
		config.Parameters["secure"] = "true"
		utils.MapPutIfAbsent(config.Parameters, "skip_verify", "true")
	}
	utils.MapPutIfAbsent(config.Parameters, "connection_open_strategy", "round_robin")
	utils.MapPutIfAbsent(config.Parameters, "mutations_sync", "2")
	utils.MapPutIfAbsent(config.Parameters, "dial_timeout", "60s")
	utils.MapPutIfAbsent(config.Parameters, "read_timeout", "60s")

	dbConnectFunction := func(config *ClickHouseConfig) (*sql.DB, error) {
		dsn := clickhouseDriverConnectionString(config)
		dataSource, err := sql.Open("clickhouse", dsn)
		if err != nil {
			return nil, err
		}

		if err := chPing(dataSource, httpMode); err != nil {
			_ = dataSource.Close()
			return nil, err
		}

		dataSource.SetMaxIdleConns(10)
		dataSource.SetConnMaxIdleTime(time.Minute * 3)
		return dataSource, nil
	}

	tableStatementFactory, err := NewTableStatementFactory(config)
	if err != nil {
		return nil, err
	}

	var nullableFields []string
	if config.Engine != nil {
		nullableFields = config.Engine.NullableFields
	}
	columnDDlFunc := func(quotedName, name string, table *Table) string {
		return chColumnDDL(quotedName, name, table, nullableFields)
	}
	var queryLogger *logging.QueryLogger
	if bulkerConfig.LogLevel == bulker.Verbose {
		queryLogger = logging.NewQueryLogger(bulkerConfig.Id, os.Stderr, os.Stderr)
	}
	sqlAdapterBase, err := newSQLAdapterBase(bulkerConfig.Id, ClickHouseBulkerTypeId, config, dbConnectFunction, clickhouseTypes, queryLogger, chTypecastFunc, QuestionMarkParameterPlaceholder, columnDDlFunc, chReformatValue, checkErr)
	sqlAdapterBase.batchFileFormat = implementations.JSON

	c := &ClickHouse{
		SQLAdapterBase:        sqlAdapterBase,
		tableStatementFactory: tableStatementFactory,
		httpMode:              httpMode,
	}
	c.tableHelper = NewTableHelper(c, 63, '`')
	return c, err
}

func clickhouseDriverConnectionString(config *ClickHouseConfig) string {
	protocol := string(config.Protocol)
	if config.Protocol == ClickHouseProtocolSecure || config.Protocol == "" {
		protocol = "clickhouse"
	}
	hostWithPorts := make([]string, len(config.Hosts))
	for i, host := range config.Hosts {
		if strings.Contains(host, ":") {
			// host already contains port
			hostWithPorts[i] = host
		} else {
			switch config.Protocol {
			case ClickHouseProtocolHTTP:
				hostWithPorts[i] = host + ":8123"
			case ClickHouseProtocolHTTPS:
				hostWithPorts[i] = host + ":8443"
			case ClickHouseProtocolNative:
				hostWithPorts[i] = host + ":9000"
			case ClickHouseProtocolSecure:
				hostWithPorts[i] = host + ":9440"
			default:
				hostWithPorts[i] = host + ":9000"
			}
		}
	}
	hosts := strings.Join(hostWithPorts, ",")
	// protocol://[user[:password]@][host1:port],[host2:port]/dbname[?param1=value1&paramN=valueN]
	connectionString := fmt.Sprintf("%s://%s:%s@%s/%s", protocol,
		config.Username, config.Password, hosts, config.Database)
	if len(config.Parameters) > 0 {
		connectionString += "?"
		paramList := make([]string, 0, len(config.Parameters))
		//concat provided connection parameters
		for k, v := range config.Parameters {
			paramList = append(paramList, k+"="+v)
		}
		connectionString += strings.Join(paramList, "&")
	}
	//logging.Infof("Connection string: %s", connectionString)
	return connectionString
}

func (ch *ClickHouse) CreateStream(id, tableName string, mode bulker.BulkMode, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	streamOptions = append(streamOptions, withLocalBatchFile(fmt.Sprintf("bulker_%s", utils.SanitizeString(id))))

	switch mode {
	case bulker.Stream:
		return newAutoCommitStream(id, ch, tableName, streamOptions...)
	case bulker.Batch:
		return newTransactionalStream(id, ch, tableName, streamOptions...)
	case bulker.ReplaceTable:
		return newReplaceTableStream(id, ch, tableName, streamOptions...)
	case bulker.ReplacePartition:
		return newReplacePartitionStream(id, ch, tableName, streamOptions...)
	}
	return nil, fmt.Errorf("unsupported bulk mode: %s", mode)
}

func (ch *ClickHouse) Type() string {
	return ClickHouseBulkerTypeId
}

// OpenTx opens underline sql transaction and return wrapped instance
func (ch *ClickHouse) OpenTx(ctx context.Context) (*TxSQLAdapter, error) {
	//return ch.openTx(ctx, ch)
	return &TxSQLAdapter{sqlAdapter: ch, tx: NewDbWrapper(ch.Type(), ch.dataSource, ch.queryLogger, ch.checkErrFunc)}, nil
}

// InitDatabase create database instance if doesn't exist
func (ch *ClickHouse) InitDatabase(ctx context.Context) error {
	var dbname string
	row := ch.txOrDb(ctx).QueryRowContext(ctx, chDatabaseQuery, ch.config.Database)
	if row != nil {
		row.Scan(&dbname)
	}
	if dbname == "" {
		query := fmt.Sprintf(chCreateDatabaseTemplate, ch.config.Database, ch.getOnClusterClause())

		if _, err := ch.txOrDb(ctx).ExecContext(ctx, query); err != nil {
			return errorj.CreateSchemaError.Wrap(err, "failed to create db schema").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Database:  ch.config.Database,
					Cluster:   ch.config.Cluster,
					Statement: query,
				})
		}
	}
	if ch.config.Cluster != "" {
		err := ch.txOrDb(ctx).QueryRowContext(ctx, chClusterQuery, ch.config.Cluster).Scan(&ch.distributed)
		if err != nil {
			ch.Errorf("failed to get cluster info - assuming distributed mode. error: %v", err)
			//assuming that cluster exists and has multiple shards
			ch.distributed = true
			return nil
		}
		if ch.distributed {
			ch.Infof("cluster `%s` is distributed", ch.config.Cluster)
		} else {
			ch.Infof("cluster `%s` is not distributed", ch.config.Cluster)
		}
	}
	return nil
}

// CreateTable create database table with name,columns provided in Table representation
// New tables will have MergeTree() or ReplicatedMergeTree() engine depends on config.cluster empty or not
func (ch *ClickHouse) CreateTable(ctx context.Context, table *Table) error {
	if table.Temporary {
		table := table.Clone()
		table.PKFields = utils.NewSet[string]()
		columns := table.SortedColumnNames()
		columnsDDL := make([]string, len(columns))
		for i, columnName := range columns {
			columnsDDL[i] = ch.columnDDL(columnName, table)
		}

		query := fmt.Sprintf(createTableTemplate+" ENGINE = Memory", "", ch.quotedTableName(table.Name), strings.Join(columnsDDL, ", "))

		if _, err := ch.txOrDb(ctx).ExecContext(ctx, query); err != nil {
			return errorj.CreateTableError.Wrap(err, "failed to create table").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Table:     table.Name,
					Statement: query,
				})
		}
		return nil
	}
	columns := table.SortedColumnNames()
	columnsDDL := make([]string, len(columns))
	for i, columnName := range table.SortedColumnNames() {
		columnsDDL[i] = ch.columnDDL(columnName, table)
	}

	statementStr := ch.tableStatementFactory.CreateTableStatement(ch.quotedLocalTableName(table.Name), ch.TableName(table.Name), strings.Join(columnsDDL, ","), table)

	if _, err := ch.txOrDb(ctx).ExecContext(ctx, statementStr); err != nil {
		return errorj.CreateTableError.Wrap(err, "failed to create table").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Database:    ch.config.Database,
				Cluster:     ch.config.Cluster,
				Table:       ch.quotedTableName(table.Name),
				PrimaryKeys: table.GetPKFields(),
				Statement:   statementStr,
			})
	}

	//create distributed table
	if ch.distributed {
		return ch.createDistributedTableInTransaction(ctx, table)
	}

	return nil
}

// GetTableSchema return table (name,columns with name and types) representation wrapped in Table struct
func (ch *ClickHouse) GetTableSchema(ctx context.Context, tableName string) (*Table, error) {
	//local table name since schema of distributed table lacks primary keys
	queryTableName := ch.TableName(ch.localTableName(tableName))
	tableName = ch.TableName(tableName)
	table := &Table{Name: tableName, Columns: Columns{}, PKFields: utils.NewSet[string]()}
	rows, err := ch.txOrDb(ctx).QueryContext(ctx, chTableSchemaQuery, ch.config.Database, queryTableName)
	if err != nil {
		return nil, errorj.GetTableError.Wrap(err, "failed to get table columns").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Database:    ch.config.Database,
				Cluster:     ch.config.Cluster,
				Table:       tableName,
				PrimaryKeys: table.GetPKFields(),
				Statement:   chTableSchemaQuery,
				Values:      []interface{}{ch.config.Database, tableName},
			})
	}

	defer rows.Close()
	for rows.Next() {
		var columnName, columnClickhouseType string
		var isPk bool
		if err := rows.Scan(&columnName, &columnClickhouseType, &isPk); err != nil {
			return nil, errorj.GetTableError.Wrap(err, "failed to scan result").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Database:    ch.config.Database,
					Cluster:     ch.config.Cluster,
					Table:       tableName,
					PrimaryKeys: table.GetPKFields(),
					Statement:   chTableSchemaQuery,
					Values:      []interface{}{ch.config.Database, tableName},
				})
		}
		dt, _ := ch.GetDataType(columnClickhouseType)
		table.Columns[columnName] = SQLColumn{Type: columnClickhouseType, DataType: dt}
		if isPk {
			table.PKFields.Put(columnName)
			table.PrimaryKeyName = BuildConstraintName(tableName)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, errorj.GetTableError.Wrap(err, "failed read last row").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Database:    ch.config.Database,
				Cluster:     ch.config.Cluster,
				Table:       tableName,
				PrimaryKeys: table.GetPKFields(),
				Statement:   chTableSchemaQuery,
				Values:      []interface{}{ch.config.Database, tableName},
			})
	}

	return table, nil
}

// PatchTableSchema add new columns(from provided Table) to existing table
// drop and create distributed table
func (ch *ClickHouse) PatchTableSchema(ctx context.Context, patchSchema *Table) error {
	if len(patchSchema.Columns) == 0 {
		return nil
	}
	columns := patchSchema.SortedColumnNames()
	addedColumnsDDL := make([]string, len(patchSchema.Columns))
	for i, columnName := range columns {
		columnDDL := ch.columnDDL(columnName, patchSchema)
		addedColumnsDDL[i] = "ADD COLUMN " + columnDDL
	}

	query := fmt.Sprintf(chAlterTableTemplate, ch.quotedLocalTableName(patchSchema.Name), ch.getOnClusterClause(), strings.Join(addedColumnsDDL, ", "))

	if _, err := ch.txOrDb(ctx).ExecContext(ctx, query); err != nil {
		return errorj.PatchTableError.Wrap(err, "failed to patch table").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Database:    ch.config.Database,
				Cluster:     ch.config.Cluster,
				Table:       patchSchema.Name,
				PrimaryKeys: patchSchema.GetPKFields(),
				Statement:   query,
			})
	}

	if ch.distributed {
		query := fmt.Sprintf(chAlterTableTemplate, ch.quotedTableName(patchSchema.Name), ch.getOnClusterClause(), strings.Join(addedColumnsDDL, ", "))

		_, err := ch.txOrDb(ctx).ExecContext(ctx, query)
		if err != nil {
			ch.Errorf("Error altering distributed table for [%s] with statement [%s]: %v", patchSchema.Name, query, err)
			// fallback for older clickhouse versions: drop and create distributed table if ReplicatedMergeTree engine
			ch.dropTable(ctx, ch.quotedTableName(patchSchema.Name), true)
			return ch.createDistributedTableInTransaction(ctx, patchSchema)
		}

	}

	return nil
}

func (ch *ClickHouse) Select(ctx context.Context, tableName string, whenConditions *WhenConditions, orderBy []string) ([]map[string]any, error) {
	tableName = ch.TableName(tableName)
	table, err := ch.GetTableSchema(ctx, tableName)
	if err != nil {
		return nil, err
	}

	if len(table.PKFields) > 0 {
		return ch.selectFrom(ctx, chSelectFinalStatement, tableName, "*", whenConditions, orderBy)
	} else {
		return ch.selectFrom(ctx, selectQueryTemplate, tableName, "*", whenConditions, orderBy)
	}
}

func (ch *ClickHouse) Count(ctx context.Context, tableName string, whenConditions *WhenConditions) (int, error) {
	tableName = ch.TableName(tableName)
	table, err := ch.GetTableSchema(ctx, tableName)
	if err != nil {
		return -1, err
	}

	var res []map[string]any
	if len(table.PKFields) > 0 {
		res, err = ch.selectFrom(ctx, chSelectFinalStatement, tableName, "count(*) as jitsu_count", whenConditions, nil)
	} else {
		res, err = ch.selectFrom(ctx, selectQueryTemplate, tableName, "count(*) as jitsu_count", whenConditions, nil)
	}
	if err != nil {
		return -1, err
	}
	if len(res) == 0 {
		return -1, fmt.Errorf("select count * gave no result")
	}
	scnt := res[0]["jitsu_count"]
	return strconv.Atoi(fmt.Sprint(scnt))
}

func (ch *ClickHouse) Insert(ctx context.Context, table *Table, merge bool, objects ...types.Object) (err error) {
	return ch.insert(ctx, table, objects)
}

// LoadTable transfer data from local file to ClickHouse table
func (ch *ClickHouse) LoadTable(ctx context.Context, targetTable *Table, loadSource *LoadSource) (err error) {
	if loadSource.Type != LocalFile {
		return fmt.Errorf("LoadTable: only local file is supported")
	}
	if loadSource.Format != ch.batchFileFormat {
		return fmt.Errorf("LoadTable: only %s format is supported", ch.batchFileFormat)
	}
	tableName := ch.quotedTableName(targetTable.Name)

	columns := targetTable.SortedColumnNames()
	columnNames := make([]string, len(columns))
	for i, name := range columns {
		columnNames[i] = ch.quotedColumnName(name)
	}
	var placeholdersBuilder strings.Builder
	args := make([]any, 0, len(columns))
	var copyStatement string
	defer func() {
		if err != nil {
			err = errorj.LoadError.Wrap(err, "failed to load table").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Database:    ch.config.Database,
					Cluster:     ch.config.Cluster,
					Table:       targetTable.Name,
					PrimaryKeys: targetTable.GetPKFields(),
					Statement:   copyStatement,
				})
		}
	}()

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
		placeholdersBuilder.WriteString(",(")
		for i, v := range columns {
			column := targetTable.Columns[v]
			l, err := convertType(object[v], column)
			if err != nil {
				return err
			}
			//ch.Infof("%s: %v (%T) was %v", v, l, l, object[v])
			if i > 0 {
				placeholdersBuilder.WriteString(",")
			}
			placeholdersBuilder.WriteString(ch.typecastFunc(ch.parameterPlaceholder(i, ch.quotedColumnName(v)), column))
			args = append(args, l)
		}
		placeholdersBuilder.WriteString(")")

	}
	copyStatement = fmt.Sprintf(chLoadStatement, tableName, strings.Join(columnNames, ", "), placeholdersBuilder.String()[1:])
	if _, err := ch.txOrDb(ctx).ExecContext(ctx, copyStatement, args...); err != nil {
		return checkErr(err)
	}
	return nil
}

func (ch *ClickHouse) CopyTables(ctx context.Context, targetTable *Table, sourceTable *Table, merge bool) (err error) {
	return ch.copy(ctx, targetTable, sourceTable)
}

func (ch *ClickHouse) Delete(ctx context.Context, tableName string, deleteConditions *WhenConditions) error {
	deleteCondition, values := ch.ToWhenConditions(deleteConditions, ch.parameterPlaceholder, 0)
	deleteQuery := fmt.Sprintf(chDeleteQueryTemplate, ch.quotedLocalTableName(tableName), ch.getOnClusterClause(), deleteCondition)

	if _, err := ch.txOrDb(ctx).ExecContext(ctx, deleteQuery, values...); err != nil {
		return errorj.DeleteFromTableError.Wrap(err, "failed to delete data").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Database:  ch.config.Database,
				Cluster:   ch.config.Cluster,
				Table:     tableName,
				Statement: deleteQuery,
				Values:    values,
			})
	}
	return nil
}

// TruncateTable deletes all records in tableName table
func (ch *ClickHouse) TruncateTable(ctx context.Context, tableName string) error {
	tableName = ch.TableName(tableName)
	statement := fmt.Sprintf(chTruncateTableTemplate, ch.quotedLocalTableName(tableName), ch.getOnClusterClause())
	if _, err := ch.txOrDb(ctx).ExecContext(ctx, statement); err != nil {
		return errorj.TruncateError.Wrap(err, "failed to truncate table").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Table:     tableName,
				Statement: statement,
			})
	}

	return nil
}

func (ch *ClickHouse) DropTable(ctx context.Context, tableName string, ifExists bool) error {
	err := ch.dropTable(ctx, ch.quotedTableName(tableName), ifExists)
	if err != nil {
		return err
	}
	if ch.distributed {
		return ch.dropTable(ctx, ch.quotedLocalTableName(tableName), true)
	}
	return nil
}

func (ch *ClickHouse) dropTable(ctx context.Context, fullTableName string, ifExists bool) error {
	ifExs := ""
	if ifExists {
		ifExs = "IF EXISTS "
	}
	query := fmt.Sprintf(chDropTableTemplate, ifExs, fullTableName, ch.getOnClusterClause())

	if _, err := ch.txOrDb(ctx).ExecContext(ctx, query); err != nil {

		return errorj.DropError.Wrap(err, "failed to drop table").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:    ch.config.Database,
				Cluster:   ch.config.Cluster,
				Table:     fullTableName,
				Statement: query,
			})
	}

	return nil
}

func (ch *ClickHouse) ReplaceTable(ctx context.Context, targetTableName string, replacementTable *Table, dropOldTable bool) (err error) {
	targetTable, err := ch.GetTableSchema(ctx, targetTableName)
	if err != nil {
		return fmt.Errorf("failed to check existence of target table: %s : %w", targetTableName, err)
	}
	if targetTable.Exists() {
		//exchange local tables only.
		//For cluster no need to exchange distribute tables. they are linked by name and will represent new data
		query := fmt.Sprintf(chExchangeTableTemplate, ch.quotedLocalTableName(targetTableName), ch.quotedLocalTableName(replacementTable.Name), ch.getOnClusterClause())
		if _, err := ch.txOrDb(ctx).ExecContext(ctx, query); err != nil {
			return fmt.Errorf("error replacing [%s] table: %v", targetTableName, err)
		}
	} else {
		//if target table does not exist yet, just rename replacement table to target one
		query := fmt.Sprintf(chRenameTableTemplate, ch.quotedLocalTableName(replacementTable.Name), ch.quotedLocalTableName(targetTableName), ch.getOnClusterClause())
		if _, err := ch.txOrDb(ctx).ExecContext(ctx, query); err != nil {
			return fmt.Errorf("error renaming [%s] table: %v", replacementTable.Name, err)
		}
		//on cluster we also need to create distributed table for newly create target table
		if ch.distributed {
			targetTable := replacementTable.Clone()
			targetTable.Name = targetTableName
			return ch.createDistributedTableInTransaction(ctx, targetTable)
		}
		return nil
	}

	if targetTable.Exists() && dropOldTable {
		return ch.DropTable(ctx, replacementTable.Name, true)
	} else {
		return nil
	}

}

// return ON CLUSTER name clause or "" if config.cluster is empty
func (ch *ClickHouse) getOnClusterClause() string {
	if ch.config.Cluster == "" {
		return ""
	}

	return fmt.Sprintf(chOnClusterClauseTemplate, ch.config.Cluster)
}

// create distributed table, ignore errors
func (ch *ClickHouse) createDistributedTableInTransaction(ctx context.Context, originTable *Table) error {
	originTableName := originTable.Name
	shardingKey := "rand()"
	if len(originTable.PKFields) > 0 {
		shardingKey = "halfMD5(" + strings.Join(originTable.GetPKFields(), ",") + ")"
	}
	statement := fmt.Sprintf(chCreateDistributedTableTemplate,
		ch.quotedTableName(originTable.Name), ch.getOnClusterClause(), ch.quotedLocalTableName(originTableName), ch.config.Cluster, ch.config.Database, ch.quotedLocalTableName(originTableName), shardingKey)

	if _, err := ch.txOrDb(ctx).ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("error creating distributed table statement with statement [%s] for [%s] : %w", statement, ch.quotedTableName(originTableName), err)
	}
	return nil
}

func (ch *ClickHouse) quotedLocalTableName(tableName string) string {
	return ch.quotedTableName(ch.localTableName(tableName))
}

func (ch *ClickHouse) localTableName(tableName string) string {
	if ch.config.Cluster != "" && ch.distributed {
		return chLocalPrefix + tableName
	} else {
		return tableName
	}
}

func convertType(value any, column SQLColumn) (any, error) {
	v := types.ReformatValue(value)
	//ch.Infof("%v (%T) was %v (%T)", v, v, value, value)

	switch strings.ToLower(column.Type) {
	case "float64":
		switch n := v.(type) {
		case int64:
			return float64(n), nil
		case int:
			return float64(n), nil
		case string:
			f, err := strconv.ParseFloat(n, 64)
			if err != nil {
				return v, fmt.Errorf("error converting string to float64: %w", err)
			}
			return f, nil
		}
	case "int64":
		switch n := v.(type) {
		case int:
			return int64(n), nil
		case float64:
			if n == float64(int64(n)) {
				return int64(n), nil
			} else {
				return v, fmt.Errorf("error converting float to int64: %f", n)
			}
		case string:
			f, err := strconv.Atoi(n)
			if err != nil {
				return v, fmt.Errorf("error converting string to int: %w", err)
			}
			return int64(f), nil
		}
	case "bool":
		switch n := v.(type) {
		case string:
			f, err := strconv.ParseBool(n)
			if err != nil {
				return v, fmt.Errorf("error converting string to bool: %w", err)
			}
			return f, nil
		}
	case "uint8":
		switch n := v.(type) {
		case string:
			f, err := strconv.ParseBool(n)
			if err == nil {
				return f, nil
			}
		}
	case "string":
		switch n := v.(type) {
		case time.Time:
			return n.Format("2006-01-02 15:04:05Z"), nil
		case int64:
			return strconv.FormatInt(n, 10), nil
		case float64:
			return fmt.Sprint(n), nil
		case bool:
			return strconv.FormatBool(n), nil
		}
	}
	return v, nil
}

// chColumnDDL returns column DDL (column name, mapped sql type)
func chColumnDDL(quotedName, name string, table *Table, nullableFields []string) string {
	//get sql type
	column := table.Columns[name]
	columnSQLType := column.GetDDLType()

	//get nullable or plain
	var columnTypeDDL string
	if utils.ArrayContains(nullableFields, name) {
		columnTypeDDL = fmt.Sprintf(chNullableColumnTemplate, columnSQLType)
	} else {
		columnTypeDDL = columnSQLType
	}

	return fmt.Sprintf(`%s %s`, quotedName, columnTypeDDL)
}

// chTypecastFunc returns "?" placeholder or with typecast
func chTypecastFunc(placeholder string, column SQLColumn) string {
	if column.Override {
		return fmt.Sprintf("cast(%s, '%s')", placeholder, column.Type)
	}
	return placeholder
}

// return nil if column type is nullable or default value for input type
func chGetDefaultValue(sqlType string) any {
	if !strings.Contains(strings.ToLower(sqlType), "nullable") {
		//get default value based on type
		dv, ok := defaultValues[strings.ToLower(sqlType)]
		if ok {
			return dv
		}

		logging.SystemErrorf("Unknown clickhouse default value for %s", sqlType)
	}

	return nil
}

// if value is boolean - reformat it [true = 1; false = 0] ClickHouse supports UInt8 instead of boolean
// otherwise return value as is
func chReformatValue(value any, valuePresent bool, sqlColumn SQLColumn) any {
	if !valuePresent {
		return chGetDefaultValue(sqlColumn.Type)
	}
	//reformat boolean
	booleanValue, ok := value.(bool)
	if ok {
		if booleanValue {
			return 1
		}

		return 0
	}
	return value
}

func extractStatement(fieldConfigs []FieldConfig) string {
	var parameters []string
	for _, fieldConfig := range fieldConfigs {
		if fieldConfig.Function != "" {
			parameters = append(parameters, fieldConfig.Function+"("+fieldConfig.Field+")")
			continue
		}
		parameters = append(parameters, fieldConfig.Field)
	}
	return strings.Join(parameters, ",")
}

// Validate required fields in ClickHouseConfig
func (chc *ClickHouseConfig) Validate() error {
	if chc == nil {
		return errors.New("ClickHouse config is required")
	}

	if len(chc.Hosts) == 0 {
		return errors.New("hosts is required parameter")
	}

	for _, dsn := range chc.Hosts {
		if dsn == "" {
			return errors.New("Host value can't be empty")
		}
	}

	if chc.Cluster == "" && len(chc.Hosts) > 1 {
		return errors.New("cluster is required parameter when hosts count > 1")
	}

	if chc.Database == "" {
		return errors.New("database is required parameter")
	}

	return nil
}

// TableStatementFactory is used for creating CREATE TABLE statements depends on config
type TableStatementFactory struct {
	config *ClickHouseConfig

	onClusterClause string
}

func NewTableStatementFactory(config *ClickHouseConfig) (*TableStatementFactory, error) {
	if config == nil {
		return nil, errors.New("Clickhouse config can't be nil")
	}
	var onClusterClause string
	if config.Cluster != "" {
		onClusterClause = fmt.Sprintf(chOnClusterClauseTemplate, config.Cluster)
	}

	return &TableStatementFactory{
		config:          config,
		onClusterClause: onClusterClause,
	}, nil
}

// CreateTableStatement return clickhouse DDL for creating table statement
func (tsf TableStatementFactory) CreateTableStatement(quotedTableName, tableName, columnsClause string, table *Table) string {
	if tsf.config.Engine != nil && len(tsf.config.Engine.RawStatement) > 0 {
		return fmt.Sprintf(chCreateTableTemplate, quotedTableName, tsf.onClusterClause, columnsClause, tsf.config.Engine.RawStatement,
			"", "", "")
	}
	var engineStatement string
	var engineStatementFormat bool

	orderByClause := ""
	primaryKeyClause := ""
	partitionClause := ""

	baseEngine := "ReplacingMergeTree"
	pkFields := table.PKFields
	if tsf.config.Engine != nil && len(tsf.config.Engine.OrderFields) > 0 {
		orderByClause = "ORDER BY (" + extractStatement(tsf.config.Engine.OrderFields) + ")"
	} else if len(pkFields) > 0 {
		orderByClause = "ORDER BY (" + strings.Join(pkFields.ToSlice(), ", ") + ")"
	} else {
		orderByClause = "ORDER BY tuple()"
		baseEngine = "MergeTree"
	}
	if len(pkFields) > 0 {
		primaryKeyClause = "PRIMARY KEY (" + strings.Join(pkFields.ToSlice(), ", ") + ")"
	}
	if tsf.config.Engine != nil && len(tsf.config.Engine.PartitionFields) > 0 {
		partitionClause = "PARTITION BY (" + extractStatement(tsf.config.Engine.PartitionFields) + ")"
	} else if table.TimestampColumn != "" {
		partitionClause = "PARTITION BY toYYYYMM(`" + table.TimestampColumn + "`)"
	}

	if tsf.config.Cluster != "" {
		//create engine statement with ReplicatedReplacingMergeTree() engine. We need to replace %s with tableName on creating statement
		engineStatement = `ENGINE = Replicated` + baseEngine + `('/clickhouse/tables/{shard}/` + tsf.config.Database + `/%s', '{replica}')`
		engineStatementFormat = true
	} else {
		//create table template with ReplacingMergeTree() engine
		engineStatement = `ENGINE = ` + baseEngine + `()`
	}

	if engineStatementFormat {
		//clear table path from non-letter symbols
		keeperPath := strings.ToLower(tableName)
		keeperPath = nonLettersCharacters.ReplaceAllString(keeperPath, "_")
		keeperPath = fmt.Sprintf("%s_%x", keeperPath, utils.HashString(tableName))
		engineStatement = fmt.Sprintf(engineStatement, keeperPath)
	}
	return fmt.Sprintf(chCreateTableTemplate, quotedTableName, tsf.onClusterClause, columnsClause, engineStatement,
		partitionClause, orderByClause, primaryKeyClause)
}

func (ch *ClickHouse) Ping(ctx context.Context) error {
	if ch.dataSource != nil {
		err := chPing(ch.dataSource, ch.httpMode)
		if err != nil {
			dataSource, err := ch.dbConnectFunction(ch.config)
			if err == nil {
				_ = ch.dataSource.Close()
				ch.dataSource = dataSource
			}
			return err
		}
	} else {
		var err error
		ch.dataSource, err = ch.dbConnectFunction(ch.config)
		if err != nil {
			return err
		}
	}
	return nil
}

func chPing(db *sql.DB, httpMode bool) error {
	if httpMode {
		//keep select 1 and don't use Ping() because chproxy doesn't support /ping endpoint.
		_, err := db.Exec("SELECT 1")
		return err
	} else {
		return db.Ping()
	}
}
