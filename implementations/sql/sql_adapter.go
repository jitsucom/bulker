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
	OpenTx(ctx context.Context) (*Transaction, error)
	Insert(ctx context.Context, wrappedTx TxOrDatasource, table *Table, merge bool, objects []types.Object) error
	CreateDbSchema(ctx context.Context, dbSchemaName string) error
	GetTableSchema(ctx context.Context, wrappedTx TxOrDatasource, tableName string) (*Table, error)
	CreateTable(ctx context.Context, wrappedTx TxOrDatasource, schemaToCreate *Table) error
	CopyTables(ctx context.Context, wrappedTx TxOrDatasource, targetTable *Table, sourceTable *Table, merge bool) error
	PatchTableSchema(ctx context.Context, wrappedTx TxOrDatasource, schemaToAdd *Table) error
	//Truncate(tableName string) error
	Update(ctx context.Context, wrappedTx TxOrDatasource, table *Table, object map[string]interface{}, whereKey string, whereValue interface{}) error
	Delete(ctx context.Context, wrappedTx TxOrDatasource, tableName string, deleteConditions *DeleteConditions) error
	DropTable(ctx context.Context, wrappedTx TxOrDatasource, table *Table, ifExists bool) error
	ReplaceTable(ctx context.Context, wrappedTx TxOrDatasource, originalTable, replacementTable string, dropOldTable bool) error
}
