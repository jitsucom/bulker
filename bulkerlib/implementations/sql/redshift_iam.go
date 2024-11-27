package sql

import (
	"context"
	"database/sql"
	"fmt"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	driver "github.com/jitsucom/bulker/bulkerlib/implementations/sql/redshift_driver"
	types2 "github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/errorj"
	"github.com/jitsucom/bulker/jitsubase/logging"
	"github.com/jitsucom/bulker/jitsubase/types"
	"github.com/jitsucom/bulker/jitsubase/utils"
	"os"
	"strings"
	"time"
)

const redshiftCopyTemplateIam = `copy %s%s (%s)
					from 's3://%s/%s'
    				IAM_ROLE '%s'
    				region '%s'
    				csv
					gzip
					IGNOREHEADER 1
                    dateformat 'auto'
                    timeformat 'auto'`

// Postgres is adapter for creating,patching (schema or table), inserting data to postgres
type RedshiftIAM struct {
	*SQLAdapterBase[driver.RedshiftConfig]
}

// NewPostgres return configured Postgres bulker.Bulker instance
func NewRedshiftIAM(bulkerConfig bulker.Config) (bulker.Bulker, error) {
	config := &driver.RedshiftConfig{}
	if err := utils.ParseObject(bulkerConfig.DestinationConfig, config); err != nil {
		return nil, fmt.Errorf("failed to parse destination config: %v", err)
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if config.Parameters == nil {
		config.Parameters = map[string]string{}
	}

	typecastFunc := func(placeholder string, column types2.SQLColumn) string {
		if column.DataType == types2.JSON || column.Type == "super" {
			return fmt.Sprintf("JSON_PARSE(%s)", placeholder)
		}
		if column.Override {
			return placeholder + "::" + column.Type
		}
		return placeholder
	}
	valueMappingFunc := func(value any, valuePresent bool, sqlColumn types2.SQLColumn) any {
		//replace zero byte character for text fields
		if valuePresent {
			if sqlColumn.DataType == types2.STRING {
				if v, ok := value.(string); ok {
					value = strings.ReplaceAll(v, "\u0000", "")
				}
			} else if sqlColumn.DataType == types2.JSON {
				if v, ok := value.(string); ok {
					value = strings.ReplaceAll(v, "\\u0000", "")
				}
			}
			dt, ok := value.(time.Time)
			if ok {
				if dt.IsZero() {
					return nil
				}
				return dt.Format(time.RFC3339Nano)
			}
		}
		return value
	}
	var queryLogger *logging.QueryLogger
	if bulkerConfig.LogLevel == bulker.Verbose {
		queryLogger = logging.NewQueryLogger(bulkerConfig.Id, os.Stderr, os.Stderr)
	}

	dbConnectFunction := func(cfg *driver.RedshiftConfig) (*sql.DB, error) {
		connectionString := cfg.String()
		logging.Infof("[%s] connecting: %s", bulkerConfig.Id, connectionString)

		dataSource, err := sql.Open("redshift-data", connectionString)
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
	sqlAdapterBase, err := newSQLAdapterBase(bulkerConfig.Id, RedshiftBulkerTypeId, config, config.Schema, dbConnectFunction, redshiftTypes, queryLogger, typecastFunc, IndexParameterPlaceholder, redshiftColumnDDL, valueMappingFunc, checkErr, true)
	p := &RedshiftIAM{sqlAdapterBase}
	// some clients have no permission to create tmp tables
	p.batchFileFormat = types2.FileFormatCSV
	p.batchFileCompression = types2.FileCompressionGZIP
	p.tableHelper = NewTableHelper(127, '"')
	p.temporaryTables = false
	p.renameToSchemaless = true
	return p, err
}

func (p *RedshiftIAM) CreateStream(id, tableName string, mode bulker.BulkMode, streamOptions ...bulker.StreamOption) (bulker.BulkerStream, error) {
	streamOptions = append(streamOptions, withLocalBatchFile(fmt.Sprintf("bulker_%s", utils.SanitizeString(id))))
	if p.config.Bucket != "" {
		streamOptions = append(streamOptions, withS3BatchFile(&S3OptionConfig{
			AccessKeyID:     p.config.AccessKeyID,
			SecretAccessKey: p.config.SecretAccessKey,
			RoleARN:         p.config.RoleARN,
			ExternalID:      p.config.ExternalID,
			Bucket:          p.config.Bucket,
			Region:          p.config.Region,
			Folder:          p.config.Folder,
		}))
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

func (p *RedshiftIAM) validateOptions(streamOptions []bulker.StreamOption) error {
	options := &bulker.StreamOptions{}
	for _, option := range streamOptions {
		options.Add(option)
	}
	return nil
}

// OpenTx opens underline sql transaction and return wrapped instance
func (p *RedshiftIAM) OpenTx(ctx context.Context) (*TxSQLAdapter, error) {
	return &TxSQLAdapter{sqlAdapter: p, tx: NewDbWrapper(p.Type(), p.dataSource, p.queryLogger, p.checkErrFunc, false)}, nil
	//return p.openTx(ctx, p)
}
func (p *RedshiftIAM) createSchemaIfNotExists(ctx context.Context, schema string) error {
	if schema == "" {
		return nil
	}
	n := p.namespaceName(schema)
	if n == "" {
		return nil
	}
	query := fmt.Sprintf(pgCreateDbSchemaIfNotExistsTemplate, n)

	if _, err := p.txOrDb(ctx).ExecContext(ctx, query); err != nil {
		return errorj.CreateSchemaError.Wrap(err, "failed to create db schema").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Schema:    n,
				Statement: query,
			})
	}
	return nil
}

// InitDatabase creates database schema instance if doesn't exist
func (p *RedshiftIAM) InitDatabase(ctx context.Context) error {
	_ = p.createSchemaIfNotExists(ctx, p.config.Schema)

	return nil
}

// GetTableSchema return table (name,columns, primary key) representation wrapped in Table struct
func (p *RedshiftIAM) GetTableSchema(ctx context.Context, namespace string, tableName string) (*Table, error) {
	table, err := p.getTable(ctx, namespace, strings.ToLower(tableName))
	if err != nil {
		return nil, err
	}

	//don't select primary keys of non-existent table
	if table.ColumnsCount() == 0 {
		return table, nil
	}

	primaryKeyName, pkFields, err := p.getPrimaryKeys(ctx, table.Namespace, table.Name)
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

func (p *RedshiftIAM) getPrimaryKeys(ctx context.Context, namespace, tableName string) (string, types.OrderedSet[string], error) {
	tableName = p.TableName(tableName)
	namespace = p.namespaceName(namespace)
	primaryKeys := types.NewOrderedSet[string]()
	pkFieldsRows, err := p.txOrDb(ctx).QueryContext(ctx, redshiftPrimaryKeyFieldsQuery, namespace, tableName)
	if err != nil {
		return "", types.OrderedSet[string]{}, errorj.GetPrimaryKeysError.Wrap(err, "failed to get primary key").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Schema:    namespace,
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
			return "", types.OrderedSet[string]{}, errorj.GetPrimaryKeysError.Wrap(err, "failed to scan result").
				WithProperty(errorj.DBInfo, &types2.ErrorPayload{
					Schema:    namespace,
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
		return "", types.OrderedSet[string]{}, errorj.GetPrimaryKeysError.Wrap(err, "failed read last row").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Schema:    namespace,
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

func (p *RedshiftIAM) getTable(ctx context.Context, namespace string, tableName string) (*Table, error) {
	tableName = p.TableName(tableName)
	namespace = p.namespaceName(namespace)
	table := &Table{Name: tableName, Namespace: namespace, Columns: NewColumns(), PKFields: types.NewOrderedSet[string]()}
	rows, err := p.txOrDb(ctx).QueryContext(ctx, pgTableSchemaQuery, utils.DefaultString(namespace, "pg_temp_11"), tableName)
	if err != nil {
		return nil, errorj.GetTableError.Wrap(err, "failed to get table columns").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Schema:      namespace,
				Table:       tableName,
				PrimaryKeys: table.GetPKFields(),
				Statement:   pgTableSchemaQuery,
				Values:      []any{namespace, tableName},
			})
	}

	defer rows.Close()
	for rows.Next() {
		var columnName, columnPostgresType string
		if err := rows.Scan(&columnName, &columnPostgresType); err != nil {
			return nil, errorj.GetTableError.Wrap(err, "failed to scan result").
				WithProperty(errorj.DBInfo, &types2.ErrorPayload{
					Schema:      namespace,
					Table:       tableName,
					PrimaryKeys: table.GetPKFields(),
					Statement:   pgTableSchemaQuery,
					Values:      []any{namespace, tableName},
				})
		}
		if columnPostgresType == "-" {
			//skip dropped postgres field
			continue
		}
		dt, _ := p.GetDataType(columnPostgresType)
		table.Columns.Set(columnName, types2.SQLColumn{Type: columnPostgresType, DataType: dt})
	}

	if err := rows.Err(); err != nil {
		return nil, errorj.GetTableError.Wrap(err, "failed read last row").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Schema:      namespace,
				Table:       tableName,
				PrimaryKeys: table.GetPKFields(),
				Statement:   pgTableSchemaQuery,
				Values:      []any{namespace, tableName},
			})
	}

	return table, nil
}

func (p *RedshiftIAM) Insert(ctx context.Context, table *Table, merge bool, objects ...types2.Object) error {
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
		res, err := p.Select(ctx, table.Namespace, table.Name, pkMatchConditions, nil)
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

func (p *RedshiftIAM) CopyTables(ctx context.Context, targetTable *Table, sourceTable *Table, mergeWindow int) (bulker.WarehouseState, error) {
	if mergeWindow <= 0 {
		return p.copy(ctx, targetTable, sourceTable)
	} else {
		return p.copyOrMerge(ctx, targetTable, sourceTable, redshiftMergeQueryTemplate, p.quotedTableName(targetTable.Name), "S")
	}
}

// LoadTable copy transfer data from s3 to redshift by passing COPY request to redshift
func (p *RedshiftIAM) LoadTable(ctx context.Context, targetTable *Table, loadSource *LoadSource) (state bulker.WarehouseState, err error) {
	quotedTableName := p.quotedTableName(targetTable.Name)
	namespace := p.namespacePrefix(targetTable.Namespace)
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
	_, _ = p.txOrDb(ctx).ExecContext(ctx, "SET json_parse_truncate_strings=ON")
	statement := fmt.Sprintf(redshiftCopyTemplateIam, namespace, quotedTableName, strings.Join(columnNames, ","), s3Config.Bucket, fileKey, s3Config.RoleARN, s3Config.Region)
	if _, err := p.txOrDb(ctx).ExecContext(ctx, statement); err != nil {
		return state, errorj.CopyError.Wrap(err, "failed to copy data from s3").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Schema:    p.config.Schema,
				Table:     quotedTableName,
				Statement: fmt.Sprintf(redshiftCopyTemplateIam, namespace, quotedTableName, strings.Join(columnNames, ","), s3Config.Bucket, fileKey, credentialsMask, s3Config.Region),
			})
	}

	return state, nil
}

func (p *RedshiftIAM) CreateTable(ctx context.Context, schemaToCreate *Table) error {
	err := p.createSchemaIfNotExists(ctx, schemaToCreate.Namespace)
	if err != nil {
		return err
	}
	err = p.SQLAdapterBase.CreateTable(ctx, schemaToCreate)
	if err != nil {
		return err
	}
	if !schemaToCreate.Temporary && schemaToCreate.TimestampColumn != "" {
		err = p.createSortKey(ctx, schemaToCreate)
		if err != nil {
			p.DropTable(ctx, schemaToCreate.Namespace, schemaToCreate.Name, true)
			return fmt.Errorf("failed to create sort key: %v", err)
		}
	}
	return nil
}

func (p *RedshiftIAM) createSortKey(ctx context.Context, table *Table) error {
	if table.TimestampColumn == "" {
		return nil
	}
	quotedTableName := p.quotedTableName(table.Name)
	namespace := p.namespacePrefix(table.Namespace)

	statement := fmt.Sprintf(redshiftAlterSortKeyTemplate,
		namespace, quotedTableName, p.quotedColumnName(table.TimestampColumn))

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

func (p *RedshiftIAM) ReplaceTable(ctx context.Context, targetTableName string, replacementTable *Table, dropOldTable bool) (err error) {
	row := p.txOrDb(ctx).QueryRowContext(ctx, fmt.Sprintf(`SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_schema ilike '%s' AND table_name = '%s')`, p.namespaceName(replacementTable.Namespace), targetTableName))
	exists := false
	err = row.Scan(&exists)
	if err != nil {
		return err
	}
	if !exists {
		return p.renameTable(ctx, p.txOrDb(ctx), replacementTable.Namespace, replacementTable.Name, targetTableName)
	}
	// rename in 2 ops in transaction
	tx, err := p.openTx(ctx, p)
	if err != nil {
		return err
	}
	tmpTable := "deprecated_" + targetTableName + time.Now().Format("_20060102_150405")
	err1 := p.renameTable(ctx, tx.tx, replacementTable.Namespace, targetTableName, tmpTable)
	err = p.renameTable(ctx, tx.tx, replacementTable.Namespace, replacementTable.Name, targetTableName)
	if err == nil && err1 == nil {
		err = tx.Commit()
		if dropOldTable && err == nil {
			return p.DropTable(ctx, replacementTable.Namespace, tmpTable, true)
		}
	} else {
		_ = tx.Rollback()
	}
	return
}

func (p *RedshiftIAM) renameTable(ctx context.Context, tx TxOrDB, namespace, tableName, newTableName string) error {
	quotedTableName := p.quotedTableName(tableName)
	quotedSchema := p.namespacePrefix(namespace)
	quotedNewTableName := p.quotedTableName(newTableName)
	renameToSchema := utils.Ternary(p.renameToSchemaless, "", quotedSchema)
	query := fmt.Sprintf(renameTableTemplate, "", quotedSchema, quotedTableName, renameToSchema, quotedNewTableName)

	if _, err := tx.ExecContext(ctx, query); err != nil {
		return errorj.RenameError.Wrap(err, "failed to rename table").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Table:     quotedTableName,
				Statement: query,
			})
	}
	return nil
}

func (p *RedshiftIAM) Ping(ctx context.Context) error {
	err := p.SQLAdapterBase.Ping(ctx)
	if err != nil {
		return err
	}
	return nil
}

//func (p *Postgres) TmpNamespace(namespace string) string {
//	return NoNamespaceValue
//}

// Close underlying sql.DB
func (p *RedshiftIAM) Close() error {
	return p.SQLAdapterBase.Close()
}

func (p *RedshiftIAM) Type() string {
	return RedshiftBulkerTypeId
}
