package sql

import (
	"context"
	"fmt"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	types2 "github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/errorj"
	"github.com/jitsucom/bulker/jitsubase/types"
	"github.com/jitsucom/bulker/jitsubase/utils"
	_ "github.com/lib/pq"
	"strings"
	"time"
)

func init() {
	bulker.RegisterBulker(RedshiftBulkerTypeId, NewRedshift)
}

const (
	RedshiftBulkerTypeId = "redshift"

	redshiftCopyTemplate = `copy %s (%s)
					from 's3://%s/%s'
    				ACCESS_KEY_ID '%s'
    				SECRET_ACCESS_KEY '%s'
    				region '%s'
    				csv
					gzip
					IGNOREHEADER 1
                    dateformat 'auto'
                    timeformat 'auto'`

	redshiftAlterSortKeyTemplate       = `ALTER TABLE %s ALTER SORTKEY (%s)`
	redshiftDeleteBeforeBulkMergeUsing = `DELETE FROM %s using %s where %s`

	redshiftPrimaryKeyFieldsQuery = `select tco.constraint_name as constraint_name, kcu.column_name as key_column
									 from information_schema.table_constraints tco
         							   join information_schema.key_column_usage kcu
             						   on kcu.constraint_name = tco.constraint_name
                                          and kcu.constraint_schema = tco.constraint_schema
 										  and kcu.constraint_name = tco.constraint_name
				                     where tco.table_schema ilike $1 and tco.table_name = $2 and tco.constraint_type = 'PRIMARY KEY'
                                     order by kcu.ordinal_position`

	credentialsMask = "*****"
)

var (
	redshiftTypes = map[types2.DataType][]string{
		types2.STRING:    {"character varying(65535)", "character varying"},
		types2.INT64:     {"bigint"},
		types2.FLOAT64:   {"double precision"},
		types2.TIMESTAMP: {"timestamp with time zone", "timestamp", "timestamp without time zone"},
		types2.BOOL:      {"boolean"},
		types2.JSON:      {"character varying(65535)", "character varying"},
		types2.UNKNOWN:   {"character varying(65535)", "character varying"},
	}
)

type RedshiftConfig struct {
	DataSourceConfig `mapstructure:",squash"`
	S3OptionConfig   `mapstructure:",squash" yaml:"-,inline"`
}

// Redshift adapter for creating,patching (schema or table), inserting and copying data from s3 to redshift
type Redshift struct {
	//Aws Redshift uses Postgres fork under the hood
	*Postgres
	s3Config *S3OptionConfig
}

// NewRedshift returns configured Redshift adapter instance
func NewRedshift(bulkerConfig bulker.Config) (bulker.Bulker, error) {
	config := &RedshiftConfig{}
	if err := utils.ParseObject(bulkerConfig.DestinationConfig, config); err != nil {
		return nil, fmt.Errorf("failed to parse destination config: %v", err)
	}
	if config.Port == 0 {
		config.Port = 5439
	}

	bulkerConfig.DestinationConfig = PostgresConfig{DataSourceConfig: config.DataSourceConfig}
	postgres, err := NewPostgres(bulkerConfig)
	if err != nil {
		return nil, err
	}
	r := &Redshift{Postgres: postgres.(*Postgres), s3Config: &config.S3OptionConfig}
	r.batchFileFormat = types2.FileFormatCSV
	r.batchFileCompression = types2.FileCompressionGZIP
	r._columnDDLFunc = redshiftColumnDDL
	r.initTypes(redshiftTypes)
	r.tableHelper = NewTableHelper(127, '"')
	r.temporaryTables = true
	//// Redshift is case insensitive by default
	//r._columnNameFunc = strings.ToLower
	//r._tableNameFunc = func(config *DataSourceConfig, tableName string) string { return tableName }
	return r, err
}

func (p *Redshift) CreateStream(id, tableName string, mode bulker.BulkMode, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	streamOptions = append(streamOptions, withLocalBatchFile(fmt.Sprintf("bulker_%s", utils.SanitizeString(id))))
	if p.s3Config != nil {
		streamOptions = append(streamOptions, withS3BatchFile(p.s3Config))
	}
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

func (p *Redshift) validateOptions(streamOptions []bulker.StreamOption) error {
	options := &bulker.StreamOptions{}
	for _, option := range streamOptions {
		options.Add(option)
	}
	return nil
}

// Type returns Postgres type
func (p *Redshift) Type() string {
	return RedshiftBulkerTypeId
}

// OpenTx opens underline sql transaction and return wrapped instance
func (p *Redshift) OpenTx(ctx context.Context) (*TxSQLAdapter, error) {
	return p.openTx(ctx, p)
}

func (p *Redshift) Insert(ctx context.Context, table *Table, merge bool, objects ...types2.Object) error {
	if !merge || len(table.GetPKFields()) == 0 {
		return p.insert(ctx, table, objects)
	}
	for _, object := range objects {
		pkMatchConditions := &WhenConditions{}
		for _, pkColumn := range table.GetPKFields() {
			value := object.GetN(pkColumn)
			if value == nil {
				pkMatchConditions = pkMatchConditions.Add(pkColumn, "IS NULL", nil)
			} else {
				pkMatchConditions = pkMatchConditions.Add(pkColumn, "=", value)
			}
		}
		res, err := p.Select(ctx, table.Name, pkMatchConditions, nil)
		if err != nil {
			return errorj.ExecuteInsertError.Wrap(err, "failed check primary key collision").
				WithProperty(errorj.DBInfo, &types2.ErrorPayload{
					Schema:      p.config.Schema,
					Table:       p.quotedTableName(table.Name),
					PrimaryKeys: table.GetPKFields(),
				})
		}
		if len(res) > 0 {
			return p.Update(ctx, table, object, pkMatchConditions)
		} else {
			return p.insert(ctx, table, []types2.Object{object})
		}
	}
	return nil
}

// LoadTable copy transfer data from s3 to redshift by passing COPY request to redshift
func (p *Redshift) LoadTable(ctx context.Context, targetTable *Table, loadSource *LoadSource) (state *bulker.WarehouseState, err error) {
	quotedTableName := p.quotedTableName(targetTable.Name)
	if loadSource.Type != AmazonS3 {
		return state, fmt.Errorf("LoadTable: only Amazon S3 file is supported")
	}
	if loadSource.Format != p.batchFileFormat {
		return state, fmt.Errorf("LoadTable: only %s format is supported", p.batchFileFormat)
	}
	columnNames := targetTable.MappedColumnNames(p.quotedColumnName)
	s3Config := loadSource.S3Config
	fileKey := loadSource.Path
	//add folder prefix if configured
	if s3Config.Folder != "" {
		fileKey = s3Config.Folder + "/" + fileKey
	}
	statement := fmt.Sprintf(redshiftCopyTemplate, quotedTableName, strings.Join(columnNames, ","), s3Config.Bucket, fileKey, s3Config.AccessKeyID, s3Config.SecretKey, s3Config.Region)
	if _, err := p.txOrDb(ctx).ExecContext(ctx, statement); err != nil {
		return state, errorj.CopyError.Wrap(err, "failed to copy data from s3").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Schema:    p.config.Schema,
				Table:     quotedTableName,
				Statement: fmt.Sprintf(redshiftCopyTemplate, quotedTableName, strings.Join(columnNames, ","), s3Config.Bucket, fileKey, credentialsMask, credentialsMask, s3Config.Region),
			})
	}

	return state, nil
}

func (p *Redshift) CopyTables(ctx context.Context, targetTable *Table, sourceTable *Table, mergeWindow int) (state *bulker.WarehouseState, err error) {
	quotedTargetTableName := p.quotedTableName(targetTable.Name)
	quotedSourceTableName := p.quotedTableName(sourceTable.Name)

	if mergeWindow > 0 && len(targetTable.PKFields) > 0 {
		//delete duplicates from table
		var pkMatchConditions string
		for i, pkColumn := range targetTable.GetPKFields() {
			if i > 0 {
				pkMatchConditions += " AND "
			}
			pkMatchConditions += fmt.Sprintf(`%s.%s = %s.%s`, quotedTargetTableName, pkColumn, quotedSourceTableName, pkColumn)
		}
		deleteStatement := fmt.Sprintf(redshiftDeleteBeforeBulkMergeUsing, quotedTargetTableName, quotedSourceTableName, pkMatchConditions)

		if _, err = p.txOrDb(ctx).ExecContext(ctx, deleteStatement); err != nil {

			return state, errorj.BulkMergeError.Wrap(err, "failed to delete duplicated rows").
				WithProperty(errorj.DBInfo, &types2.ErrorPayload{
					Schema:      p.config.Schema,
					Table:       quotedTargetTableName,
					PrimaryKeys: targetTable.GetPKFields(),
					Statement:   deleteStatement,
				})
		}
	}
	return state, p.copy(ctx, targetTable, sourceTable)
}

func (p *Redshift) ReplaceTable(ctx context.Context, targetTableName string, replacementTable *Table, dropOldTable bool) (err error) {
	tmpTable := "deprecated_" + targetTableName + time.Now().Format("_20060102_150405")
	err1 := p.renameTable(ctx, true, targetTableName, tmpTable)
	err = p.renameTable(ctx, false, replacementTable.Name, targetTableName)
	if dropOldTable && err1 == nil && err == nil {
		return p.DropTable(ctx, tmpTable, true)
	}
	return
}

func (p *Redshift) renameTable(ctx context.Context, ifExists bool, tableName, newTableName string) error {
	if ifExists {
		row := p.txOrDb(ctx).QueryRowContext(ctx, fmt.Sprintf(`SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_schema ilike '%s' AND table_name = '%s')`, p.config.Schema, tableName))
		exists := false
		err := row.Scan(&exists)
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}
	}
	return p.SQLAdapterBase.renameTable(ctx, false, tableName, newTableName)
}

// GetTableSchema return table (name,columns, primary key) representation wrapped in Table struct
func (p *Redshift) GetTableSchema(ctx context.Context, tableName string) (*Table, error) {
	table, err := p.getTable(ctx, strings.ToLower(tableName))
	if err != nil {
		return nil, err
	}

	//don't select primary keys of non-existent table
	if table.ColumnsCount() == 0 {
		return table, nil
	}

	primaryKeyName, pkFields, err := p.getPrimaryKeys(ctx, table.Name)
	if err != nil {
		return nil, err
	}

	table.PKFields = pkFields
	table.PrimaryKeyName = primaryKeyName

	if primaryKeyName != "" && !strings.HasPrefix(primaryKeyName, BulkerManagedPkConstraintPrefix) {
		p.Infof("table: %s.%s has a primary key with name: %s that isn't managed by Jitsu. Custom primary key will be used in rows deduplication and updates. primary_key configuration provided in Jitsu config will be ignored.", p.config.Schema, table.Name, primaryKeyName)
	}
	return table, nil
}

func (p *Redshift) getPrimaryKeys(ctx context.Context, tableName string) (string, types.Set[string], error) {
	tableName = p.TableName(tableName)
	primaryKeys := types.NewSet[string]()
	pkFieldsRows, err := p.txOrDb(ctx).QueryContext(ctx, redshiftPrimaryKeyFieldsQuery, p.config.Schema, tableName)
	if err != nil {
		return "", nil, errorj.GetPrimaryKeysError.Wrap(err, "failed to get primary key").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Schema:    p.config.Schema,
				Table:     tableName,
				Statement: redshiftPrimaryKeyFieldsQuery,
				Values:    []any{p.config.Schema, tableName},
			})
	}

	defer pkFieldsRows.Close()
	var pkFields []string
	var primaryKeyName string
	for pkFieldsRows.Next() {
		var constraintName, fieldName string
		if err := pkFieldsRows.Scan(&constraintName, &fieldName); err != nil {
			return "", nil, errorj.GetPrimaryKeysError.Wrap(err, "failed to scan result").
				WithProperty(errorj.DBInfo, &types2.ErrorPayload{
					Schema:    p.config.Schema,
					Table:     tableName,
					Statement: redshiftPrimaryKeyFieldsQuery,
					Values:    []any{p.config.Schema, tableName},
				})
		}
		if primaryKeyName == "" && constraintName != "" {
			primaryKeyName = constraintName
		}
		pkFields = append(pkFields, fieldName)
	}
	if err := pkFieldsRows.Err(); err != nil {
		return "", nil, errorj.GetPrimaryKeysError.Wrap(err, "failed read last row").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Schema:    p.config.Schema,
				Table:     tableName,
				Statement: redshiftPrimaryKeyFieldsQuery,
				Values:    []any{p.config.Schema, tableName},
			})
	}
	for _, field := range pkFields {
		primaryKeys.Put(field)
	}

	return primaryKeyName, primaryKeys, nil
}

func (p *Redshift) CreateTable(ctx context.Context, schemaToCreate *Table) error {
	err := p.SQLAdapterBase.CreateTable(ctx, schemaToCreate)
	if err != nil {
		return err
	}
	if !schemaToCreate.Temporary && schemaToCreate.TimestampColumn != "" {
		err = p.createSortKey(ctx, schemaToCreate)
		if err != nil {
			p.DropTable(ctx, schemaToCreate.Name, true)
			return fmt.Errorf("failed to create sort key: %v", err)
		}
	}
	return nil
}

func (p *Redshift) createSortKey(ctx context.Context, table *Table) error {
	if table.TimestampColumn == "" {
		return nil
	}
	quotedTableName := p.quotedTableName(table.Name)

	statement := fmt.Sprintf(redshiftAlterSortKeyTemplate,
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

// redshiftColumnDDL returns column DDL (quoted column name, mapped sql type and 'not null' if pk field)
func redshiftColumnDDL(quotedName, name string, table *Table, column types2.SQLColumn) string {
	var columnConstaints string
	var columnAttributes string

	sqlType := column.GetDDLType()

	if _, ok := table.PKFields[name]; ok {
		columnConstaints = " not null " + getDefaultValueStatement(sqlType)
		if len(table.PKFields) == 1 {
			columnAttributes = " DISTKEY "
		}
	}

	return fmt.Sprintf(`%s %s%s%s`, quotedName, sqlType, columnAttributes, columnConstaints)
}
