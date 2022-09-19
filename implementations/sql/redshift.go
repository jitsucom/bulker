package sql

import (
	"context"
	"fmt"
	"github.com/jitsucom/bulker/base/errorj"
	"github.com/jitsucom/bulker/base/logging"
	"github.com/jitsucom/bulker/base/timestamp"
	"github.com/jitsucom/bulker/base/utils"
	"github.com/jitsucom/bulker/bulker"
	"github.com/jitsucom/bulker/types"
	_ "github.com/lib/pq"
	"strings"
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
					IGNOREHEADER 1
                    dateformat 'auto'
                    timeformat 'auto'`

	redshiftDeleteBeforeBulkMergeUsing = `DELETE FROM %s using %s where %s`

	redshiftPrimaryKeyFieldsQuery = `select tco.constraint_name as constraint_name, kcu.column_name as key_column
									 from information_schema.table_constraints tco
         							   join information_schema.key_column_usage kcu
             						   on kcu.constraint_name = tco.constraint_name
                                          and kcu.constraint_schema = tco.constraint_schema
 										  and kcu.constraint_name = tco.constraint_name
				                     where tco.table_schema = $1 and tco.table_name = $2 and tco.constraint_type = 'PRIMARY KEY'
                                     order by kcu.ordinal_position`

	credentialsMask = "*****"
)

var (
	SchemaToRedshift = map[types.DataType]string{
		types.STRING:    "character varying(65535)",
		types.INT64:     "bigint",
		types.FLOAT64:   "double precision",
		types.TIMESTAMP: "timestamp",
		types.BOOL:      "boolean",
		types.UNKNOWN:   "character varying(65535)",
	}
)

type RedshiftConfig struct {
	DataSourceConfig
	*S3OptionConfig `mapstructure:"s3_config,omitempty" json:"s3_config,omitempty" yaml:"s3_config,omitempty"`
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
		return nil, fmt.Errorf("failed to parse destination config: %w", err)
	}
	if config.Port == 0 {
		config.Port = 5439
	}
	bulkerConfig.DestinationConfig = config.DataSourceConfig
	postgres, err := NewPostgres(bulkerConfig)
	if err != nil {
		return nil, err
	}

	r := &Redshift{Postgres: postgres.(*Postgres), s3Config: config.S3OptionConfig}
	r.batchFileFormat = CSV
	return r, nil
}

func (p *Redshift) CreateStream(id, tableName string, mode bulker.BulkMode, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	streamOptions = append(streamOptions, withLocalBatchFile(fmt.Sprintf("bulker_%s_stream_%s_%s", mode, tableName, utils.SanitizeString(id))))
	if p.s3Config != nil {
		streamOptions = append(streamOptions, withS3BatchFile(p.s3Config))
	}
	if err := p.validateOptions(streamOptions); err != nil {
		return nil, err
	}
	switch mode {
	case bulker.AutoCommit:
		return newAutoCommitStream(id, p, tableName, streamOptions...)
	case bulker.Transactional:
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
		option(options)
	}
	return nil
}

// Type returns Postgres type
func (p *Redshift) Type() string {
	return RedshiftBulkerTypeId
}
func (p *Redshift) GetTypesMapping() map[types.DataType]string {
	return SchemaToRedshift
}

// OpenTx opens underline sql transaction and return wrapped instance
func (p *Redshift) OpenTx(ctx context.Context) (*TxSQLAdapter, error) {
	return p.openTx(ctx, p)
}

func (p *Redshift) Insert(ctx context.Context, table *Table, merge bool, objects []types.Object) error {
	if !merge || len(table.GetPKFields()) == 0 {
		return p.insert(ctx, table, objects)
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
		res, err := p.Select(ctx, table.Name, pkMatchConditions, "")
		if err != nil {
			return errorj.ExecuteInsertError.Wrap(err, "failed check primary key collision").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Schema:      p.config.Schema,
					Table:       table.Name,
					PrimaryKeys: table.GetPKFields(),
				})
		}
		if len(res) > 0 {
			return p.Update(ctx, table.Name, object, pkMatchConditions)
		} else {
			return p.insert(ctx, table, []types.Object{object})
		}
	}
	return nil
}

// LoadTable copy transfer data from s3 to redshift by passing COPY request to redshift
func (p *Redshift) LoadTable(ctx context.Context, targetTable *Table, loadSource *LoadSource) (err error) {
	if loadSource.Type != AmazonS3 {
		return fmt.Errorf("LoadTable: only Amazon S3 file is supported")
	}
	if loadSource.Format != p.batchFileFormat {
		return fmt.Errorf("LoadTable: only %s format is supported", p.batchFileFormat)
	}
	columns := targetTable.SortedColumnNames()
	columnNames := make([]string, len(columns))
	for i, name := range columns {
		columnNames[i] = p.columnName(name)
	}
	tableName := targetTable.Name
	s3Config := loadSource.S3Config
	fileKey := loadSource.Path
	//add folder prefix if configured
	if s3Config.Folder != "" {
		fileKey = s3Config.Folder + "/" + fileKey
	}
	statement := fmt.Sprintf(redshiftCopyTemplate, p.fullTableName(tableName), strings.Join(columns, ","), s3Config.Bucket, fileKey, s3Config.AccessKeyID, s3Config.SecretKey, s3Config.Region)
	if _, err := p.txOrDb(ctx).ExecContext(ctx, statement); err != nil {
		return errorj.CopyError.Wrap(err, "failed to copy data from s3").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
				Schema:    p.config.Schema,
				Table:     tableName,
				Statement: fmt.Sprintf(redshiftCopyTemplate, p.fullTableName(tableName), columns, s3Config.Bucket, fileKey, credentialsMask, credentialsMask, s3Config.Region),
			})
	}

	return nil
}

func (p *Redshift) CopyTables(ctx context.Context, targetTable *Table, sourceTable *Table, merge bool) (err error) {
	if merge && len(targetTable.PKFields) > 0 {
		//delete duplicates from table
		var pkMatchConditions string
		for i, pkColumn := range targetTable.GetPKFields() {
			if i > 0 {
				pkMatchConditions += " AND "
			}
			pkMatchConditions += fmt.Sprintf(`%s.%s = %s.%s`, p.fullTableName(targetTable.Name), pkColumn, p.fullTableName(sourceTable.Name), pkColumn)
		}
		deleteStatement := fmt.Sprintf(redshiftDeleteBeforeBulkMergeUsing, p.fullTableName(targetTable.Name), p.fullTableName(sourceTable.Name), pkMatchConditions)

		if _, err = p.txOrDb(ctx).ExecContext(ctx, deleteStatement); err != nil {

			return errorj.BulkMergeError.Wrap(err, "failed to delete duplicated rows").
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
					Schema:      p.config.Schema,
					Table:       targetTable.Name,
					PrimaryKeys: targetTable.GetPKFields(),
					Statement:   deleteStatement,
				})
		}
	}
	return p.copy(ctx, targetTable, sourceTable)
}

func (p *Redshift) ReplaceTable(ctx context.Context, originalTable, replacementTable string, dropOldTable bool) (err error) {
	tmpTable := "deprecated_" + originalTable + timestamp.Now().Format("_20060102_150405")
	err1 := p.renameTable(ctx, true, originalTable, tmpTable)
	err = p.renameTable(ctx, false, replacementTable, originalTable)
	if dropOldTable && err1 == nil && err == nil {
		return p.DropTable(ctx, tmpTable, true)
	}
	return
}

func (p *Redshift) renameTable(ctx context.Context, ifExists bool, tableName, newTableName string) error {
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
	return p.SQLAdapterBase.renameTable(ctx, false, tableName, newTableName)
}

// GetTableSchema return table (name,columns, primary key) representation wrapped in Table struct
func (p *Redshift) GetTableSchema(ctx context.Context, tableName string) (*Table, error) {
	table, err := p.getTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	//don't select primary keys of non-existent table
	if len(table.Columns) == 0 {
		return table, nil
	}

	primaryKeyName, pkFields, err := p.getPrimaryKeys(ctx, tableName)
	if err != nil {
		return nil, err
	}

	table.PKFields = pkFields
	table.PrimaryKeyName = primaryKeyName

	jitsuPrimaryKeyName := BuildConstraintName(table.Name)
	if primaryKeyName != "" && primaryKeyName != jitsuPrimaryKeyName {
		logging.Warnf("table: %s.%s has a custom primary key with name: %s that isn't managed by Jitsu. Custom primary key will be used in rows deduplication and updates. primary_key_fields configuration provided in Jitsu config will be ignored.", p.config.Schema, table.Name, primaryKeyName)
	}
	return table, nil
}

func (p *Redshift) getPrimaryKeys(ctx context.Context, tableName string) (string, utils.Set[string], error) {
	primaryKeys := utils.NewSet[string]()
	pkFieldsRows, err := p.txOrDb(ctx).QueryContext(ctx, redshiftPrimaryKeyFieldsQuery, p.config.Schema, tableName)
	if err != nil {
		return "", nil, errorj.GetPrimaryKeysError.Wrap(err, "failed to get primary key").
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
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
				WithProperty(errorj.DBInfo, &types.ErrorPayload{
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
			WithProperty(errorj.DBInfo, &types.ErrorPayload{
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
