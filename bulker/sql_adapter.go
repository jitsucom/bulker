package bulker

import (
	"github.com/jitsucom/bulker/types"
)

// SQLAdapter is a manager for DWH tables
type SQLAdapter interface {
	Type() string
	GetConfig() *types.DataSourceConfig
	//GetTypesMapping return mapping from generic types to SQL types specific for this database
	GetTypesMapping() map[types.DataType]string
	OpenTx() (*types.Transaction, error)
	GetAutoCommitTx() *types.Transaction
	Insert(wrappedTx types.TxOrDatasource, table *types.Table, merge bool, objects []types.Object) error
	GetTableSchema(wrappedTx types.TxOrDatasource, tableName string) (*types.Table, error)
	CreateTable(wrappedTx types.TxOrDatasource, schemaToCreate *types.Table) error
	MergeTables(wrappedTx types.TxOrDatasource, targetTable *types.Table, sourceTable *types.Table) error
	PatchTableSchema(wrappedTx types.TxOrDatasource, schemaToAdd *types.Table) error
	//Truncate(tableName string) error
	Update(wrappedTx types.TxOrDatasource, table *types.Table, object map[string]interface{}, whereKey string, whereValue interface{}) error
	Delete(wrappedTx types.TxOrDatasource, tableName string, deleteConditions *types.DeleteConditions) error
	DropTable(wrappedTx types.TxOrDatasource, table *types.Table, ifExists bool) error
	ReplaceTable(wrappedTx types.TxOrDatasource, originalTable, replacementTable string, dropOldTable bool) error
}
