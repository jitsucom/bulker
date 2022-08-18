package sql

import (
	"context"
	"github.com/jitsucom/bulker/types"
)

// SQLAdapter is a manager for DWH tables
type SQLAdapter interface {
	Type() string
	GetConfig() *DataSourceConfig
	//GetTypesMapping return mapping from generic types to SQL types specific for this database
	GetTypesMapping() map[types.DataType]string
	OpenTx(ctx context.Context) (*TxOrDBWrapper, error)
	Insert(ctx context.Context, txOrDb TxOrDB, table *Table, merge bool, objects []types.Object) error
	CreateDbSchema(ctx context.Context, txOrDb TxOrDB, dbSchemaName string) error
	GetTableSchema(ctx context.Context, txOrDb TxOrDB, tableName string) (*Table, error)
	CreateTable(ctx context.Context, txOrDb TxOrDB, schemaToCreate *Table) error
	CopyTables(ctx context.Context, txOrDb TxOrDB, targetTable *Table, sourceTable *Table, merge bool) error
	PatchTableSchema(ctx context.Context, txOrDb TxOrDB, schemaToAdd *Table) error
	//Truncate(tableName string) error
	Update(ctx context.Context, txOrDb TxOrDB, table *Table, object map[string]any, whereKey string, whereValue any) error
	Delete(ctx context.Context, txOrDb TxOrDB, tableName string, deleteConditions *WhenConditions) error
	Select(ctx context.Context, tableName string, deleteConditions *WhenConditions) ([]map[string]any, error)
	DropTable(ctx context.Context, txOrDb TxOrDB, table *Table, ifExists bool) error
	ReplaceTable(ctx context.Context, txOrDb TxOrDB, originalTable, replacementTable string, dropOldTable bool) error

	//private
	dbWrapper() *TxOrDBWrapper
}
