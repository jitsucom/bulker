package bulker

import "github.com/jitsucom/bulker/types"

//SQLAdapter is a manager for DWH tables
type SQLAdapter interface {
	GetTableSchema(tableName string) (*types.Table, error)
	CreateTable(schemaToCreate *types.Table) error
	PatchTableSchema(schemaToAdd *types.Table) error
	//Truncate(tableName string) error
	Update(table *types.Table, object map[string]interface{}, whereKey string, whereValue interface{}) error
	DropTable(table *types.Table) (err error)
	ReplaceTable(originalTable, replacementTable string, dropOldTable bool) error
}
