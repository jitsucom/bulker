package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jitsucom/bulker/bulkerlib"
	"github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/errorj"
	"github.com/jitsucom/bulker/jitsubase/jsoniter"
	"github.com/jitsucom/bulker/jitsubase/logging"
	types2 "github.com/jitsucom/bulker/jitsubase/types"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"github.com/jitsucom/bulker/jitsubase/uuid"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// TODO: second test for http
// TODO: option Optimize table on Complete ?
func init() {
	bulkerlib.RegisterBulker(ClickHouseBulkerTypeId, NewClickHouse)
}

const (
	ClickHouseBulkerTypeId = "clickhouse"

	chLocalPrefix = "local_"

	chDatabaseQuery          = "SELECT name FROM system.databases where name = ?"
	chClusterQuery           = "SELECT max(shard_num) > 1 FROM system.clusters where cluster = ?"
	chCreateDatabaseTemplate = `CREATE DATABASE IF NOT EXISTS ? %s`

	chTableSchemaQuery        = `SELECT name, type FROM system.columns WHERE database = ? and table = ? and default_kind not in ('MATERIALIZED', 'ALIAS', 'EPHEMERAL') order by position`
	chPrimaryKeyFieldsQuery   = `SELECT primary_key FROM system.tables WHERE database = ? and table = ?`
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
	chLoadJSONStatement    = `INSERT INTO %s format JSONEachRow`

	chDateFormat = `2006-01-02 15:04:05.000000`
)

var (
	clickhouseTypes = map[types.DataType][]string{
		types.STRING:    {"String", "%String%", "%CHAR%", "%TEXT%", "%BLOB%", "%Enum%", "%UUID%"},
		types.INT64:     {"Int64", "Int", "LowCardinality(Int"},
		types.FLOAT64:   {"Float64", "Float32", "Decimal"},
		types.TIMESTAMP: {"DateTime64(6)", "DateTime", "Date"},
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
	LoadAsJSON bool               `mapstructure:"loadAsJson,omitempty" json:"loadAsJson,omitempty" yaml:"loadAsJson,omitempty"`
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
	*SQLAdapterBase[ClickHouseConfig]
	httpMode              bool
	distributed           atomic.Bool
	tableStatementFactory *TableStatementFactory
}

// NewClickHouse returns configured ClickHouse adapter instance
func NewClickHouse(bulkerConfig bulkerlib.Config) (bulkerlib.Bulker, error) {
	config := &ClickHouseConfig{}
	if err := utils.ParseObject(bulkerConfig.DestinationConfig, config); err != nil {
		return nil, fmt.Errorf("failed to parse destination config: %v", err)
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
	utils.MapPutIfAbsent(config.Parameters, "date_time_input_format", "best_effort")

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

	var nullableFields []string
	if config.Engine != nil {
		nullableFields = config.Engine.NullableFields
	}
	columnDDlFunc := func(quotedName, name string, table *Table, column types.SQLColumn) string {
		return chColumnDDL(quotedName, name, table, column, nullableFields)
	}
	var queryLogger *logging.QueryLogger
	if bulkerConfig.LogLevel == bulkerlib.Verbose {
		queryLogger = logging.NewQueryLogger(bulkerConfig.Id, os.Stderr, os.Stderr)
	}
	sqlAdapterBase, err := newSQLAdapterBase(bulkerConfig.Id, ClickHouseBulkerTypeId, config, dbConnectFunction, clickhouseTypes, queryLogger, chTypecastFunc, QuestionMarkParameterPlaceholder, columnDDlFunc, chReformatValue, checkErr)
	sqlAdapterBase.batchFileFormat = types.FileFormatNDJSON

	c := &ClickHouse{
		SQLAdapterBase: sqlAdapterBase,
		httpMode:       httpMode,
		distributed:    atomic.Bool{},
	}
	tableStatementFactory := NewTableStatementFactory(c)
	c.tableStatementFactory = tableStatementFactory
	c.tableHelper = NewTableHelper(63, '`')
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

func (ch *ClickHouse) CreateStream(id, tableName string, mode bulkerlib.BulkMode, streamOptions ...bulkerlib.StreamOption) (bulkerlib.BulkerStream, error) {
	streamOptions = append(streamOptions, withLocalBatchFile(fmt.Sprintf("bulker_%s", utils.SanitizeString(id))))

	switch mode {
	case bulkerlib.Stream:
		return newAutoCommitStream(id, ch, tableName, streamOptions...)
	case bulkerlib.Batch:
		return newTransactionalStream(id, ch, tableName, streamOptions...)
	case bulkerlib.ReplaceTable:
		return newReplaceTableStream(id, ch, tableName, streamOptions...)
	case bulkerlib.ReplacePartition:
		return newReplacePartitionStream(id, ch, tableName, streamOptions...)
	}
	return nil, fmt.Errorf("unsupported bulk mode: %s", mode)
}

func (ch *ClickHouse) Type() string {
	return ClickHouseBulkerTypeId
}

// OpenTx relies on ClickHouse session by creating new connection and wrapping it with TxSQLAdapter
// it makes sure that all activity happens in one connection.
func (ch *ClickHouse) OpenTx(ctx context.Context) (*TxSQLAdapter, error) {
	var db DB
	if ch.httpMode {
		configCopy := *ch.config
		configCopy.Parameters = utils.MapCopy(ch.config.Parameters)
		utils.MapPutIfAbsent(configCopy.Parameters, "session_id", uuid.New())
		utils.MapPutIfAbsent(configCopy.Parameters, "session_timeout", "3600")
		// create db pool just for one session because 'session_id' config parameter defines session
		sessionDb, err := ch.dbConnectFunction(&configCopy)
		if err != nil {
			return nil, fmt.Errorf("failed to open session: %v", err)
		}
		c, err := sessionDb.Conn(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to open connection: %v", err)
		}
		db = NewConWithDB(sessionDb, c)
	} else {
		var err error
		db, err = ch.dataSource.Conn(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to open connection: %v", err)
		}
	}
	return &TxSQLAdapter{sqlAdapter: ch, tx: NewDbWrapper(ch.Type(), db, ch.queryLogger, ch.checkErrFunc, true)}, nil
}

// InitDatabase create database instance if doesn't exist
func (ch *ClickHouse) InitDatabase(ctx context.Context) error {
	var dbname string
	row := ch.txOrDb(ctx).QueryRowContext(ctx, chDatabaseQuery, ch.config.Database)
	if row != nil {
		_ = row.Scan(&dbname)
	}
	if dbname == "" {
		query := fmt.Sprintf(chCreateDatabaseTemplate, ch.getOnClusterClause())

		if _, err := ch.txOrDb(ctx).ExecContext(ctx, query, ch.config.Database); err != nil {
			return errorj.CreateSchemaError.Wrap(err, "failed to create db schema").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Database:  ch.config.Database,
					Cluster:   ch.config.Cluster,
					Statement: query,
				})
		}
	}
	if ch.config.Cluster != "" {
		var distributed bool
		err := ch.txOrDb(ctx).QueryRowContext(ctx, chClusterQuery, ch.config.Cluster).Scan(&distributed)
		if err != nil {
			ch.Errorf("failed to get cluster info - assuming distributed mode. error: %v", err)
			//assuming that cluster exists and has multiple shards
			ch.distributed.Store(true)
			return nil
		}
		ch.distributed.Store(distributed)
		if distributed {
			ch.Debugf("cluster `%s` is distributed", ch.config.Cluster)
		} else {
			ch.Debugf("cluster `%s` is not distributed", ch.config.Cluster)
		}
	}
	return nil
}

// CreateTable create database table with name,columns provided in Table representation
// New tables will have MergeTree() or ReplicatedMergeTree() engine depends on config.cluster empty or not
func (ch *ClickHouse) CreateTable(ctx context.Context, table *Table) error {
	if table.Temporary {
		table := table.Clone()
		table.PKFields = types2.NewOrderedSet[string]()
		columnsDDL := table.MappedColumns(func(name string, column types.SQLColumn) string {
			return ch.columnDDL(name, table, column)
		})

		query := fmt.Sprintf(createTableTemplate, "TEMPORARY", ch.quotedTableName(table.Name), strings.Join(columnsDDL, ", "))

		if _, err := ch.txOrDb(ctx).ExecContext(ctx, query); err != nil {
			return errorj.CreateTableError.Wrap(err, "failed to create table").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Table:     table.Name,
					Statement: query,
				})
		}
		return nil
	}
	columnsDDL := table.MappedColumns(func(name string, column types.SQLColumn) string {
		return ch.columnDDL(name, table, column)
	})

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
	if ch.distributed.Load() {
		return ch.createDistributedTableInTransaction(ctx, table)
	}

	return nil
}

// GetTableSchema return table (name,columns with name and types) representation wrapped in Table struct
func (ch *ClickHouse) GetTableSchema(ctx context.Context, tableName string) (*Table, error) {
	//local table name since schema of distributed table lacks primary keys
	queryTableName := ch.TableName(ch.localTableName(tableName))
	tableName = ch.TableName(tableName)
	table := &Table{Name: tableName, Columns: NewColumns(), PKFields: types2.NewOrderedSet[string]()}
	rows, err := ch.txOrDb(ctx).QueryContext(ctx, chTableSchemaQuery, ch.config.Database, queryTableName)
	if err != nil {
		return nil, errorj.GetTableError.Wrap(err, "failed to get table columns").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Database:    ch.config.Database,
				Cluster:     ch.config.Cluster,
				Table:       tableName,
				PrimaryKeys: table.GetPKFields(),
				Statement:   chTableSchemaQuery,
				Values:      []interface{}{ch.config.Database, queryTableName},
			})
	}

	defer rows.Close()
	for rows.Next() {
		var columnName, columnClickhouseType string
		if err := rows.Scan(&columnName, &columnClickhouseType); err != nil {
			return nil, errorj.GetTableError.Wrap(err, "failed to scan result").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Database:    ch.config.Database,
					Cluster:     ch.config.Cluster,
					Table:       tableName,
					PrimaryKeys: table.GetPKFields(),
					Statement:   chTableSchemaQuery,
					Values:      []interface{}{ch.config.Database, queryTableName},
				})
		}
		dt, _ := ch.GetDataType(columnClickhouseType)
		table.Columns.Set(columnName, types.SQLColumn{Type: columnClickhouseType, DataType: dt})
	}
	if err := rows.Err(); err != nil {
		return nil, errorj.GetTableError.Wrap(err, "failed read last row").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Database:    ch.config.Database,
				Cluster:     ch.config.Cluster,
				Table:       tableName,
				PrimaryKeys: table.GetPKFields(),
				Statement:   chTableSchemaQuery,
				Values:      []interface{}{ch.config.Database, queryTableName},
			})
	}
	primaryKeyName, pkFields, err := ch.getPrimaryKey(ctx, tableName)
	if err != nil {
		return nil, err
	}
	if !pkFields.Empty() {
		table.PKFields = pkFields
		table.PrimaryKeyName = primaryKeyName
	}

	return table, nil
}

// getPrimaryKey returns primary key name and fields
func (ch *ClickHouse) getPrimaryKey(ctx context.Context, tableName string) (string, types2.OrderedSet[string], error) {
	tableName = ch.TableName(tableName)
	queryTableName := ch.TableName(ch.localTableName(tableName))
	pkFieldsRows, err := ch.txOrDb(ctx).QueryContext(ctx, chPrimaryKeyFieldsQuery, ch.config.Database, queryTableName)
	if err != nil {
		return "", types2.OrderedSet[string]{}, errorj.GetPrimaryKeysError.Wrap(err, "failed to get primary key").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Database:  ch.config.Database,
				Cluster:   ch.config.Cluster,
				Table:     tableName,
				Statement: chPrimaryKeyFieldsQuery,
				Values:    []interface{}{ch.config.Database, queryTableName},
			})
	}
	defer pkFieldsRows.Close()
	var pkString string
	if pkFieldsRows.Next() {
		if err = pkFieldsRows.Scan(&pkString); err != nil {
			return "", types2.OrderedSet[string]{}, errorj.GetPrimaryKeysError.Wrap(err, "failed to scan result").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Database:  ch.config.Database,
					Cluster:   ch.config.Cluster,
					Table:     tableName,
					Statement: chPrimaryKeyFieldsQuery,
					Values:    []interface{}{ch.config.Database, queryTableName},
				})
		}
	}

	if pkString == "" {
		return "", types2.OrderedSet[string]{}, nil
	}
	primaryKeys := types2.NewOrderedSet[string]()
	primaryKeys.PutAll(utils.ArrayMap(strings.Split(pkString, ","), strings.TrimSpace))
	return ch.BuildConstraintName(tableName), primaryKeys, nil
}

// PatchTableSchema add new columns(from provided Table) to existing table
// drop and create distributed table
func (ch *ClickHouse) PatchTableSchema(ctx context.Context, patchSchema *Table) error {
	if patchSchema.ColumnsCount() == 0 {
		return nil
	}
	addedColumnsDDL := patchSchema.MappedColumns(func(columnName string, column types.SQLColumn) string {
		return "ADD COLUMN " + ch.columnDDL(columnName, patchSchema, column)
	})

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

	if ch.distributed.Load() {
		query := fmt.Sprintf(chAlterTableTemplate, ch.quotedTableName(patchSchema.Name), ch.getOnClusterClause(), strings.Join(addedColumnsDDL, ", "))

		_, err := ch.txOrDb(ctx).ExecContext(ctx, query)
		if err != nil {
			ch.Errorf("Error altering distributed table for [%s] with statement [%s]: %v", patchSchema.Name, query, err)
			// fallback for older clickhouse versions: drop and create distributed table if ReplicatedMergeTree engine
			ch.dropTable(ctx, ch.quotedTableName(patchSchema.Name), ch.getOnClusterClause(), true)
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

	if table.PKFields.Size() > 0 {
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
	if table.PKFields.Size() > 0 {
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

func (ch *ClickHouse) Insert(ctx context.Context, table *Table, _ bool, objects ...types.Object) (err error) {
	return ch.insert(ctx, table, objects)
}

// LoadTable transfer data from local file to ClickHouse table
func (ch *ClickHouse) LoadTable(ctx context.Context, targetTable *Table, loadSource *LoadSource) (state bulkerlib.WarehouseState, err error) {
	if loadSource.Type != LocalFile {
		return state, fmt.Errorf("LoadTable: only local file is supported")
	}
	if loadSource.Format != ch.batchFileFormat {
		return state, fmt.Errorf("LoadTable: only %s format is supported", ch.batchFileFormat)
	}
	startTime := time.Now()
	tableName := ch.quotedTableName(targetTable.Name)

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
		return state, err
	}
	defer func() {
		_ = file.Close()
	}()
	if ch.config.LoadAsJSON {
		stat, err := file.Stat()
		if err != nil {
			return state, err
		}
		copyStatement = fmt.Sprintf(chLoadJSONStatement+"\n", tableName)
		builder := strings.Builder{}
		builder.Grow(int(stat.Size()) + len(copyStatement))
		builder.WriteString(copyStatement)
		_, err = io.Copy(&builder, file)
		if err != nil {
			return state, err
		}

		if _, err := ch.txOrDb(ctx).ExecContext(ctx, builder.String()); err != nil {
			return state, checkErr(err)
		}
		state = bulkerlib.WarehouseState{
			Name:            "clickhouse_load_data",
			TimeProcessedMs: time.Since(startTime).Milliseconds(),
		}
		return state, nil
	} else {
		columnNames := targetTable.MappedColumnNames(ch.quotedColumnName)
		var placeholdersBuilder strings.Builder
		args := make([]any, 0, targetTable.ColumnsCount())

		decoder := jsoniter.NewDecoder(file)
		decoder.UseNumber()
		for {
			object := map[string]any{}
			err = decoder.Decode(&object)
			if err != nil {
				if err == io.EOF {
					break
				}
				return state, err
			}
			placeholdersBuilder.WriteString(",(")
			err = targetTable.Columns.ForEachIndexedE(func(i int, name string, column types.SQLColumn) error {
				l, err2 := convertType(object[name], column)
				if err2 != nil {
					return err2
				}
				if i > 0 {
					placeholdersBuilder.WriteString(",")
				}
				placeholdersBuilder.WriteString(ch.typecastFunc(ch.parameterPlaceholder(i, columnNames[i]), column))
				args = append(args, l)
				return nil
			})
			if err != nil {
				return state, err
			}
			placeholdersBuilder.WriteString(")")
		}
		if len(args) > 0 {
			loadTime := time.Now()
			state = bulkerlib.WarehouseState{
				Name:            "clickhouse_prepare_data",
				TimeProcessedMs: loadTime.Sub(startTime).Milliseconds(),
			}
			copyStatement = fmt.Sprintf(chLoadStatement, tableName, strings.Join(columnNames, ", "), placeholdersBuilder.String()[1:])
			if _, err := ch.txOrDb(ctx).ExecContext(ctx, copyStatement, args...); err != nil {
				return state, checkErr(err)
			}
			state.Merge(bulkerlib.WarehouseState{
				Name:            "clickhouse_load_data",
				TimeProcessedMs: time.Since(loadTime).Milliseconds(),
			})
		}
		return state, nil
	}
}

func (ch *ClickHouse) CopyTables(ctx context.Context, targetTable *Table, sourceTable *Table, mergeWindow int) (state bulkerlib.WarehouseState, err error) {
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
	err := ch.dropTable(ctx, ch.quotedTableName(tableName), ch.getOnClusterClause(), ifExists)
	if err != nil {
		return err
	}
	if ch.distributed.Load() {
		return ch.dropTable(ctx, ch.quotedLocalTableName(tableName), ch.getOnClusterClause(), true)
	}
	return nil
}

func (ch *ClickHouse) Drop(ctx context.Context, table *Table, ifExists bool) error {
	if table.Temporary {
		return ch.dropTable(ctx, ch.quotedTableName(table.Name), "", ifExists)
	} else {
		return ch.DropTable(ctx, table.Name, ifExists)
	}
}

func (ch *ClickHouse) dropTable(ctx context.Context, fullTableName string, onClusterClause string, ifExists bool) error {
	ifExs := ""
	if ifExists {
		ifExs = "IF EXISTS "
	}
	query := fmt.Sprintf(chDropTableTemplate, ifExs, fullTableName, onClusterClause)

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
		return fmt.Errorf("failed to check existence of target table: %s : %v", targetTableName, err)
	}
	if targetTable.Exists() {
		if ch.distributed.Load() {
			// we need to adjust distributed table schema to mach new table schema
			targetTable = replacementTable.Clone()
			targetTable.Name = targetTableName
			_, err = ch.tableHelper.EnsureTableWithoutCaching(ctx, ch, ch.ID, targetTable)
			if err != nil {
				return err
			}
		}
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
		if ch.distributed.Load() {
			targetTable = replacementTable.Clone()
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
	if originTable.PKFields.Size() > 0 {
		shardingKey = "halfMD5(" + strings.Join(originTable.GetPKFields(), ",") + ")"
	}
	statement := fmt.Sprintf(chCreateDistributedTableTemplate,
		ch.quotedTableName(originTable.Name), ch.getOnClusterClause(), ch.quotedLocalTableName(originTableName), ch.config.Cluster, ch.config.Database, ch.quotedLocalTableName(originTableName), shardingKey)

	if _, err := ch.txOrDb(ctx).ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("error creating distributed table statement with statement [%s] for [%s] : %v", statement, ch.quotedTableName(originTableName), err)
	}
	return nil
}

func (ch *ClickHouse) quotedLocalTableName(tableName string) string {
	return ch.quotedTableName(ch.localTableName(tableName))
}

func (ch *ClickHouse) localTableName(tableName string) string {
	if ch.config.Cluster != "" && ch.distributed.Load() {
		return chLocalPrefix + tableName
	} else {
		return tableName
	}
}

func convertType(value any, column types.SQLColumn) (any, error) {
	v := types.ReformatValue(value)
	lt := strings.ToLower(column.Type)
	switch lt {
	case "float64":
		switch n := v.(type) {
		case int64:
			return float64(n), nil
		case int:
			return float64(n), nil
		case string:
			f, err := strconv.ParseFloat(n, 64)
			if err != nil {
				return v, fmt.Errorf("error converting string to float64: %v", err)
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
				return v, fmt.Errorf("error converting string to int: %v", err)
			}
			return int64(f), nil
		}
	case "bool":
		switch n := v.(type) {
		case string:
			f, err := strconv.ParseBool(n)
			if err != nil {
				return v, fmt.Errorf("error converting string to bool: %v", err)
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
	default:
		if strings.HasPrefix(lt, "datetime64") {
			switch n := v.(type) {
			case time.Time:
				return n.Format(chDateFormat), nil
			}

		}

	}

	return v, nil
}

// chColumnDDL returns column DDL (column name, mapped sql type)
func chColumnDDL(quotedName, name string, _ *Table, column types.SQLColumn, nullableFields []string) string {
	//get sql type
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
func chTypecastFunc(placeholder string, column types.SQLColumn) string {
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
// Pass time.Time in full precision for datetime64 columns
func chReformatValue(value any, valuePresent bool, sqlColumn types.SQLColumn) any {
	if !valuePresent {
		return chGetDefaultValue(sqlColumn.Type)
	}
	lt := strings.ToLower(sqlColumn.Type)
	switch v := value.(type) {
	case time.Time:
		if strings.Contains(lt, "datetime64") {
			return v.Format(chDateFormat)
		}
	case bool:
		if v {
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

func (ch *ClickHouse) Config() *ClickHouseConfig {
	return ch.config
}

func (ch *ClickHouse) IsDistributed() bool {
	return ch.distributed.Load()
}

type ClickHouseCluster interface {
	IsDistributed() bool
	Config() *ClickHouseConfig
}

// TableStatementFactory is used for creating CREATE TABLE statements depends on config
type TableStatementFactory struct {
	ch              ClickHouseCluster
	onClusterClause string
}

func NewTableStatementFactory(ch ClickHouseCluster) *TableStatementFactory {
	var onClusterClause string
	if ch.Config().Cluster != "" {
		onClusterClause = fmt.Sprintf(chOnClusterClauseTemplate, ch.Config().Cluster)
	}

	return &TableStatementFactory{
		ch:              ch,
		onClusterClause: onClusterClause,
	}
}

// CreateTableStatement return clickhouse DDL for creating table statement
func (tsf TableStatementFactory) CreateTableStatement(quotedTableName, tableName, columnsClause string, table *Table) string {
	config := tsf.ch.Config()
	if config.Engine != nil && len(config.Engine.RawStatement) > 0 {
		return fmt.Sprintf(chCreateTableTemplate, quotedTableName, tsf.onClusterClause, columnsClause, config.Engine.RawStatement,
			"", "", "")
	}
	var engineStatement string
	var engineStatementFormat bool

	orderByClause := ""
	primaryKeyClause := ""
	partitionClause := ""

	baseEngine := "ReplacingMergeTree"
	pkFields := table.PKFields
	if config.Engine != nil && len(config.Engine.OrderFields) > 0 {
		orderByClause = "ORDER BY (" + extractStatement(config.Engine.OrderFields) + ")"
	} else if pkFields.Size() > 0 {
		orderByClause = "ORDER BY (" + strings.Join(pkFields.ToSlice(), ", ") + ")"
	} else {
		orderByClause = "ORDER BY tuple()"
		baseEngine = "MergeTree"
	}
	if pkFields.Size() > 0 {
		primaryKeyClause = "PRIMARY KEY (" + strings.Join(pkFields.ToSlice(), ", ") + ")"
	}
	if config.Engine != nil && len(config.Engine.PartitionFields) > 0 {
		partitionClause = "PARTITION BY (" + extractStatement(config.Engine.PartitionFields) + ")"
	} else if table.TimestampColumn != "" {
		partitionClause = "PARTITION BY toYYYYMM(`" + table.TimestampColumn + "`)"
	}

	if config.Cluster != "" {
		shardsMacros := "{shard}/"
		if !tsf.ch.IsDistributed() {
			shardsMacros = "1/"
		}
		//create engine statement with ReplicatedReplacingMergeTree() engine. We need to replace %s with tableName on creating statement
		engineStatement = `ENGINE = Replicated` + baseEngine + `('/clickhouse/tables/` + shardsMacros + config.Database + `/%s', '{replica}')`
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

func (ch *ClickHouse) Ping(_ context.Context) error {
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
