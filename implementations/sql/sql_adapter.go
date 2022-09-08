package sql

import (
	"context"
	"errors"
	"fmt"
	"github.com/jitsucom/bulker/types"
)

const ContextTransactionKey = "transaction"

var ErrTableNotExist = errors.New("table doesn't exist")

// TODO Use prepared statements?
// TODO: Avoid SQL injection - use own method instead of printf

// SQLAdapter is a manager for DWH tables
type SQLAdapter interface {
	Type() string

	//GetTypesMapping return mapping from generic types to SQL types specific for this database
	GetTypesMapping() map[types.DataType]string
	OpenTx(ctx context.Context) (*TxSQLAdapter, error)
	Insert(ctx context.Context, table *Table, merge bool, objects []types.Object) error
	// InitDatabase setups required db objects like 'schema' or 'dataset' if they don't exist
	InitDatabase(ctx context.Context) error
	GetTableSchema(ctx context.Context, tableName string) (*Table, error)
	CreateTable(ctx context.Context, schemaToCreate *Table) error
	CopyTables(ctx context.Context, targetTable *Table, sourceTable *Table, merge bool) error
	LoadTable(ctx context.Context, targetTable *Table, loadSource *LoadSource) error
	PatchTableSchema(ctx context.Context, patchTable *Table) error
	TruncateTable(ctx context.Context, tableName string) error
	//TODO tests for Update
	Update(ctx context.Context, tableName string, object types.Object, whenConditions *WhenConditions) error
	Delete(ctx context.Context, tableName string, deleteConditions *WhenConditions) error
	DropTable(ctx context.Context, tableName string, ifExists bool) error
	ReplaceTable(ctx context.Context, originalTable, replacementTable string, dropOldTable bool) error

	Select(ctx context.Context, tableName string, whenConditions *WhenConditions, orderBy string) ([]map[string]any, error)
	Count(ctx context.Context, tableName string, whenConditions *WhenConditions) (int, error)
}

type LoadSourceType string

const (
	LocalFile        LoadSourceType = "local_file"
	GoogleCloudStore LoadSourceType = "google_cloud_store"
	AmazonS3         LoadSourceType = "amazon_s3"
)

type LoadSourceFormat string

const (
	CSV  LoadSourceFormat = "csv"
	JSON LoadSourceFormat = "json"
)

type LoadSource struct {
	Type     LoadSourceType
	Format   LoadSourceFormat
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

func (tx *TxSQLAdapter) GetTypesMapping() map[types.DataType]string {
	return tx.sqlAdapter.GetTypesMapping()
}
func (tx *TxSQLAdapter) OpenTx(ctx context.Context) (*TxSQLAdapter, error) {
	return nil, fmt.Errorf("can't open transaction inside transaction")
}
func (tx *TxSQLAdapter) Insert(ctx context.Context, table *Table, merge bool, objects []types.Object) error {
	ctx = context.WithValue(ctx, ContextTransactionKey, tx.tx)
	return tx.sqlAdapter.Insert(ctx, table, merge, objects)
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

func (tx *TxSQLAdapter) Update(ctx context.Context, tableName string, object types.Object, whenConditions *WhenConditions) error {
	ctx = context.WithValue(ctx, ContextTransactionKey, tx.tx)
	return tx.sqlAdapter.Update(ctx, tableName, object, whenConditions)
}
func (tx *TxSQLAdapter) Delete(ctx context.Context, tableName string, deleteConditions *WhenConditions) error {
	ctx = context.WithValue(ctx, ContextTransactionKey, tx.tx)
	return tx.sqlAdapter.Delete(ctx, tableName, deleteConditions)
}
func (tx *TxSQLAdapter) DropTable(ctx context.Context, tableName string, ifExists bool) error {
	ctx = context.WithValue(ctx, ContextTransactionKey, tx.tx)
	return tx.sqlAdapter.DropTable(ctx, tableName, ifExists)
}
func (tx *TxSQLAdapter) ReplaceTable(ctx context.Context, originalTable, replacementTable string, dropOldTable bool) error {
	ctx = context.WithValue(ctx, ContextTransactionKey, tx.tx)
	return tx.sqlAdapter.ReplaceTable(ctx, originalTable, replacementTable, dropOldTable)
}

func (tx *TxSQLAdapter) Select(ctx context.Context, tableName string, whenConditions *WhenConditions, orderBy string) ([]map[string]any, error) {
	ctx = context.WithValue(ctx, ContextTransactionKey, tx.tx)
	return tx.sqlAdapter.Select(ctx, tableName, whenConditions, "")
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
