package sql

import (
	"context"
	"errors"
	"github.com/jitsucom/bulker/types"
)

var ErrTableNotExist = errors.New("table doesn't exist")

// TODO Use prepared statements?
// TODO: Avoid SQL injection - use own method instead of printf

// SQLAdapter is a manager for DWH tables
type SQLAdapter interface {
	Type() string

	//GetTypesMapping return mapping from generic types to SQL types specific for this database
	GetTypesMapping() map[types.DataType]string
	OpenTx(ctx context.Context) (*TxOrDBWrapper, error)
	Insert(ctx context.Context, txOrDb TxOrDB, table *Table, merge bool, objects []types.Object) error
	// InitDatabase setups required db objects like 'schema' or 'dataset' if they don't exist
	InitDatabase(ctx context.Context, txOrDb TxOrDB) error
	GetTableSchema(ctx context.Context, txOrDb TxOrDB, tableName string) (*Table, error)
	CreateTable(ctx context.Context, txOrDb TxOrDB, schemaToCreate *Table) error
	CopyTables(ctx context.Context, txOrDb TxOrDB, targetTable *Table, sourceTable *Table, merge bool) error
	PatchTableSchema(ctx context.Context, txOrDb TxOrDB, schemaToAdd *Table) error
	TruncateTable(ctx context.Context, txOrDb TxOrDB, tableName string) error
	//TODO test for Update
	Update(ctx context.Context, txOrDb TxOrDB, tableName string, object types.Object, whenConditions *WhenConditions) error
	Delete(ctx context.Context, txOrDb TxOrDB, tableName string, deleteConditions *WhenConditions) error
	DropTable(ctx context.Context, txOrDb TxOrDB, tableName string, ifExists bool) error
	ReplaceTable(ctx context.Context, txOrDb TxOrDB, originalTable, replacementTable string, dropOldTable bool) error

	Select(ctx context.Context, tableName string, whenConditions *WhenConditions) ([]map[string]any, error)
	Count(ctx context.Context, tableName string, whenConditions *WhenConditions) (int, error)

	//DbWrapper returns *TxOrDBWrapper that wraps sql.DB instance to run queries outside transactions
	DbWrapper() *TxOrDBWrapper
}
