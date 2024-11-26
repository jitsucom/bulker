package sql

import (
	"context"
	"fmt"
	bulker "github.com/jitsucom/bulker/bulkerlib"
	driver "github.com/jitsucom/bulker/bulkerlib/implementations/sql/redshift_driver"
	types2 "github.com/jitsucom/bulker/bulkerlib/types"
	"github.com/jitsucom/bulker/jitsubase/errorj"
	"github.com/jitsucom/bulker/jitsubase/types"
	"github.com/jitsucom/bulker/jitsubase/utils"
	_ "github.com/lib/pq"
	"strings"
	"text/template"
	"time"
)

func init() {
	bulker.RegisterBulker(RedshiftBulkerTypeId, NewRedshift)
}

const (
	RedshiftBulkerTypeId = "redshift"

	redshiftCopyTemplate = `copy %s%s (%s)
					from 's3://%s/%s'
    				ACCESS_KEY_ID '%s'
    				SECRET_ACCESS_KEY '%s'
    				region '%s'
    				csv
					gzip
					IGNOREHEADER 1
                    dateformat 'auto'
                    timeformat 'auto'`

	redshiftAlterSortKeyTemplate = `ALTER TABLE %s%s ALTER SORTKEY (%s)`
	redshiftMergeStatement       = `MERGE INTO {{.Namespace}}{{.TableTo}} USING {{.NamespaceFrom}}{{.TableFrom}} S ON {{.JoinConditions}} WHEN MATCHED THEN UPDATE SET {{.UpdateSet}} WHEN NOT MATCHED THEN INSERT ({{.Columns}}) VALUES ({{.SourceColumns}})`

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
	redshiftMergeQueryTemplate, _ = template.New("redshiftMergeStatement").Parse(redshiftMergeStatement)

	redshiftTypes = map[types2.DataType][]string{
		types2.STRING:    {"character varying(65535)"},
		types2.INT64:     {"bigint"},
		types2.FLOAT64:   {"double precision"},
		types2.TIMESTAMP: {"timestamp with time zone", "timestamp", "timestamp without time zone"},
		types2.BOOL:      {"boolean"},
		types2.JSON:      {"super"},
		types2.UNKNOWN:   {"character varying(65535)"},
	}
)

// Redshift adapter for creating,patching (schema or table), inserting and copying data from s3 to redshift
type Redshift struct {
	//Aws Redshift uses Postgres fork under the hood
	*Postgres
	s3Config *S3OptionConfig
}

func NewRedshift(bulkerConfig bulker.Config) (bulker.Bulker, error) {
	config := &driver.RedshiftConfig{}
	if err := utils.ParseObject(bulkerConfig.DestinationConfig, config); err != nil {
		return nil, fmt.Errorf("failed to parse destination config: %v", err)
	}
	if config.AuthenticationMethod == "iam" {
		return NewRedshiftIAM(bulkerConfig)
	} else {
		return NewRedshiftClassic(bulkerConfig)
	}
}

// NewRedshift returns configured Redshift adapter instance
func NewRedshiftClassic(bulkerConfig bulker.Config) (bulker.Bulker, error) {
	config := &driver.RedshiftConfig{}
	if err := utils.ParseObject(bulkerConfig.DestinationConfig, config); err != nil {
		return nil, fmt.Errorf("failed to parse destination config: %v", err)
	}
	if config.Port == 0 {
		config.Port = 5439
	}

	bulkerConfig.DestinationConfig = PostgresConfig{DataSourceConfig: DataSourceConfig{
		Host:       config.Host,
		Port:       config.Port,
		Db:         config.Db,
		Schema:     config.Schema,
		Username:   config.Username,
		Password:   config.Password,
		Parameters: config.Parameters,
	}}
	postgres, err := NewPostgres(bulkerConfig)
	if err != nil {
		return nil, err
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
	r := &Redshift{Postgres: postgres.(*Postgres), s3Config: &S3OptionConfig{
		AccessKeyID:     config.AccessKeyID,
		SecretAccessKey: config.SecretAccessKey,
		Bucket:          config.Bucket,
		Region:          config.Region,
		Folder:          config.Folder,
	}}
	r.batchFileFormat = types2.FileFormatCSV
	r.batchFileCompression = types2.FileCompressionGZIP
	r._columnDDLFunc = redshiftColumnDDL
	r.typecastFunc = typecastFunc
	r.typesMapping, r.reverseTypesMapping = InitTypes(redshiftTypes, true)
	r.tableHelper = NewTableHelper(127, '"')
	r.temporaryTables = true
	r.renameToSchemaless = true
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
	return &TxSQLAdapter{sqlAdapter: p, tx: NewDbWrapper(p.Type(), p.dataSource, p.queryLogger, p.checkErrFunc, false)}, nil
	//return p.openTx(ctx, p)
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

// LoadTable copy transfer data from s3 to redshift by passing COPY request to redshift
func (p *Redshift) LoadTable(ctx context.Context, targetTable *Table, loadSource *LoadSource) (state bulker.WarehouseState, err error) {
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
	statement := fmt.Sprintf(redshiftCopyTemplate, namespace, quotedTableName, strings.Join(columnNames, ","), s3Config.Bucket, fileKey, s3Config.AccessKeyID, s3Config.SecretAccessKey, s3Config.Region)
	if _, err := p.txOrDb(ctx).ExecContext(ctx, statement); err != nil {
		return state, errorj.CopyError.Wrap(err, "failed to copy data from s3").
			WithProperty(errorj.DBInfo, &types2.ErrorPayload{
				Schema:    p.config.Schema,
				Table:     quotedTableName,
				Statement: fmt.Sprintf(redshiftCopyTemplate, namespace, quotedTableName, strings.Join(columnNames, ","), s3Config.Bucket, fileKey, credentialsMask, credentialsMask, s3Config.Region),
			})
	}

	return state, nil
}

func (p *Redshift) CopyTables(ctx context.Context, targetTable *Table, sourceTable *Table, mergeWindow int) (bulker.WarehouseState, error) {
	if mergeWindow <= 0 {
		return p.copy(ctx, targetTable, sourceTable)
	} else {
		return p.copyOrMerge(ctx, targetTable, sourceTable, redshiftMergeQueryTemplate, p.quotedTableName(targetTable.Name), "S")
	}
}

func (p *Redshift) ReplaceTable(ctx context.Context, targetTableName string, replacementTable *Table, dropOldTable bool) (err error) {
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

func (p *Redshift) renameTable(ctx context.Context, tx TxOrDB, namespace, tableName, newTableName string) error {
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

// GetTableSchema return table (name,columns, primary key) representation wrapped in Table struct
func (p *Redshift) GetTableSchema(ctx context.Context, namespace string, tableName string) (*Table, error) {
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

func (p *Redshift) getPrimaryKeys(ctx context.Context, namespace, tableName string) (string, types.OrderedSet[string], error) {
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
func (p *Redshift) TmpNamespace(string) string {
	return NoNamespaceValue
}

func (p *Redshift) CreateTable(ctx context.Context, schemaToCreate *Table) error {
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

func (p *Redshift) createSortKey(ctx context.Context, table *Table) error {
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

// redshiftColumnDDL returns column DDL (quoted column name, mapped sql type and 'not null' if pk field)
func redshiftColumnDDL(quotedName, name string, table *Table, column types2.SQLColumn) string {
	var columnConstaints string
	var columnAttributes string

	sqlType := column.GetDDLType()

	if table.PKFields.Contains(name) {
		columnConstaints = " not null " + getDefaultValueStatement(sqlType)
		if table.PKFields.Size() == 1 {
			columnAttributes = " DISTKEY "
		}
	}

	return fmt.Sprintf(`%s %s%s%s`, quotedName, sqlType, columnAttributes, columnConstaints)
}
