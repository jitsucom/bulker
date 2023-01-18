package sql

import (
	"context"
	"errors"
	"fmt"
	"github.com/jitsucom/bulker/implementations"
	"github.com/jitsucom/bulker/types"
	"regexp"
)

const ContextTransactionKey = "transaction"

var notExistRegexp = regexp.MustCompile(`(?i)(not|doesn't)\sexist`)

var ErrTableNotExist = errors.New("table doesn't exist")

// TODO Use prepared statements?
// TODO: Avoid SQL injection - use own method instead of printf

// SQLAdapter is a manager for DWH tables
type SQLAdapter interface {
	Type() string

	//GetSQLType return mapping from generic bulker type to SQL type specific for this database
	GetSQLType(dataType types.DataType) (string, bool)
	//GetDataType return mapping from sql type to generic bulker type
	GetDataType(sqlType string) (types.DataType, bool)
	GetBatchFileFormat() implementations.FileFormat
	OpenTx(ctx context.Context) (*TxSQLAdapter, error)
	Insert(ctx context.Context, table *Table, merge bool, objects ...types.Object) error
	Ping(ctx context.Context) error
	// InitDatabase setups required db objects like 'schema' or 'dataset' if they don't exist
	InitDatabase(ctx context.Context) error
	GetTableSchema(ctx context.Context, tableName string) (*Table, error)
	CreateTable(ctx context.Context, schemaToCreate *Table) error
	CopyTables(ctx context.Context, targetTable *Table, sourceTable *Table, merge bool) error
	LoadTable(ctx context.Context, targetTable *Table, loadSource *LoadSource) error
	PatchTableSchema(ctx context.Context, patchTable *Table) error
	TruncateTable(ctx context.Context, tableName string) error
	//(ctx context.Context, tableName string, object types.Object, whenConditions *WhenConditions) error
	Delete(ctx context.Context, tableName string, deleteConditions *WhenConditions) error
	DropTable(ctx context.Context, tableName string, ifExists bool) error
	ReplaceTable(ctx context.Context, targetTableName string, replacementTable *Table, dropOldTable bool) error

	Select(ctx context.Context, tableName string, whenConditions *WhenConditions, orderBy []string) ([]map[string]any, error)
	Count(ctx context.Context, tableName string, whenConditions *WhenConditions) (int, error)

	// ColumnName adapts column name to sql identifier rules of database
	ColumnName(rawColumn string) string
	// TableName adapts table name to sql identifier rules of database
	TableName(rawTableName string) string
}

type LoadSourceType string

const (
	LocalFile        LoadSourceType = "local_file"
	GoogleCloudStore LoadSourceType = "google_cloud_store"
	AmazonS3         LoadSourceType = "amazon_s3"
)

type LoadSource struct {
	Type     LoadSourceType
	Format   implementations.FileFormat
	Path     string
	S3Config *S3OptionConfig
}

type TxSQLAdapter struct {
	sqlAdapter SQLAdapter
	tx         *TxWrapper
}

func (tx *TxSQLAdapter) Type() string {
	return tx.sqlAdapter.Type()
}

func (tx *TxSQLAdapter) GetBatchFileFormat() implementations.FileFormat {
	return tx.sqlAdapter.GetBatchFileFormat()
}

func (tx *TxSQLAdapter) GetSQLType(dataType types.DataType) (string, bool) {
	return tx.sqlAdapter.GetSQLType(dataType)
}

func (tx *TxSQLAdapter) GetDataType(sqlType string) (types.DataType, bool) {
	return tx.sqlAdapter.GetDataType(sqlType)
}

func (tx *TxSQLAdapter) OpenTx(ctx context.Context) (*TxSQLAdapter, error) {
	return nil, fmt.Errorf("can't open transaction inside transaction")
}
func (tx *TxSQLAdapter) Insert(ctx context.Context, table *Table, merge bool, objects ...types.Object) error {
	ctx = context.WithValue(ctx, ContextTransactionKey, tx.tx)
	return tx.sqlAdapter.Insert(ctx, table, merge, objects...)
}
func (tx *TxSQLAdapter) Ping(ctx context.Context) error {
	ctx = context.WithValue(ctx, ContextTransactionKey, tx.tx)
	return tx.sqlAdapter.Ping(ctx)
}
func (tx *TxSQLAdapter) InitDatabase(ctx context.Context) error {
	ctx = context.WithValue(ctx, ContextTransactionKey, tx.tx)
	return tx.sqlAdapter.InitDatabase(ctx)
}
func (tx *TxSQLAdapter) GetTableSchema(ctx context.Context, tableName string) (*Table, error) {
	ctx = context.WithValue(ctx, ContextTransactionKey, tx.tx)
	return tx.sqlAdapter.GetTableSchema(ctx, tableName)
}
func (tx *TxSQLAdapter) CreateTable(ctx context.Context, schemaToCreate *Table) error {
	ctx = context.WithValue(ctx, ContextTransactionKey, tx.tx)
	return tx.sqlAdapter.CreateTable(ctx, schemaToCreate)
}
func (tx *TxSQLAdapter) CopyTables(ctx context.Context, targetTable *Table, sourceTable *Table, merge bool) error {
	ctx = context.WithValue(ctx, ContextTransactionKey, tx.tx)
	return tx.sqlAdapter.CopyTables(ctx, targetTable, sourceTable, merge)
}
func (tx *TxSQLAdapter) LoadTable(ctx context.Context, targetTable *Table, loadSource *LoadSource) error {
	ctx = context.WithValue(ctx, ContextTransactionKey, tx.tx)
	return tx.sqlAdapter.LoadTable(ctx, targetTable, loadSource)
}
func (tx *TxSQLAdapter) PatchTableSchema(ctx context.Context, patchTable *Table) error {
	ctx = context.WithValue(ctx, ContextTransactionKey, tx.tx)
	return tx.sqlAdapter.PatchTableSchema(ctx, patchTable)
}
func (tx *TxSQLAdapter) TruncateTable(ctx context.Context, tableName string) error {
	ctx = context.WithValue(ctx, ContextTransactionKey, tx.tx)
	return tx.sqlAdapter.TruncateTable(ctx, tableName)
}

//	func (tx *TxSQLAdapter) Update(ctx context.Context, tableName string, object types.Object, whenConditions *WhenConditions) error {
//		ctx = context.WithValue(ctx, ContextTransactionKey, tx.tx)
//		return tx.sqlAdapter.Update(ctx, tableName, object, whenConditions)
//	}
func (tx *TxSQLAdapter) Delete(ctx context.Context, tableName string, deleteConditions *WhenConditions) error {
	ctx = context.WithValue(ctx, ContextTransactionKey, tx.tx)
	return tx.sqlAdapter.Delete(ctx, tableName, deleteConditions)
}
func (tx *TxSQLAdapter) DropTable(ctx context.Context, tableName string, ifExists bool) error {
	ctx = context.WithValue(ctx, ContextTransactionKey, tx.tx)
	return tx.sqlAdapter.DropTable(ctx, tableName, ifExists)
}
func (tx *TxSQLAdapter) ReplaceTable(ctx context.Context, targetTableName string, replacementTable *Table, dropOldTable bool) error {
	ctx = context.WithValue(ctx, ContextTransactionKey, tx.tx)
	return tx.sqlAdapter.ReplaceTable(ctx, targetTableName, replacementTable, dropOldTable)
}

func (tx *TxSQLAdapter) Select(ctx context.Context, tableName string, whenConditions *WhenConditions, orderBy []string) ([]map[string]any, error) {
	ctx = context.WithValue(ctx, ContextTransactionKey, tx.tx)
	return tx.sqlAdapter.Select(ctx, tableName, whenConditions, orderBy)
}
func (tx *TxSQLAdapter) Count(ctx context.Context, tableName string, whenConditions *WhenConditions) (int, error) {
	ctx = context.WithValue(ctx, ContextTransactionKey, tx.tx)
	return tx.sqlAdapter.Count(ctx, tableName, whenConditions)
}

func (tx *TxSQLAdapter) Commit() error {
	return tx.tx.Commit()
}

func (tx *TxSQLAdapter) Rollback() error {
	return tx.tx.Rollback()
}

func (tx *TxSQLAdapter) ColumnName(identifier string) string {
	return tx.sqlAdapter.ColumnName(identifier)
}

func (tx *TxSQLAdapter) TableName(identifier string) string {
	return tx.sqlAdapter.TableName(identifier)
}
