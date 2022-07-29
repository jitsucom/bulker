package bulker

type StoreResult struct {
}

type StoreContext struct {
	TableSchema   interface{}
	Transactional bool
	WholeTable    bool
	//Replace
}

type Bulker interface {
	// do we need such method that also do routing to tables?
	//Store(objects []map[string]interface{}) (map[string]*StoreResult, error)
	//TODO: Streaming interface.
	//TODO: Use chunks in whole table.
	//TODO: meta type for map[string]interface{}
	StoreTable(tableName string, storeContext StoreContext, objects []map[string]interface{}) (map[string]*StoreResult, error)

	//ReplaceTable(originalTable, replacementTable string, dropOldTable bool) error
	//TruncateTable(tableName string) error
	DropTable(tableName string) error
}

//TODO: cleanup deprecated tables

//TODO: no primary keys and similar rows
//
//type Storage interface {
//	io.Closer
//	DryRun(payload events.Event) ([][]adapters.TableField, error)
//	Store(fileName string, objects []map[string]interface{}, alreadyUploadedTables map[string]bool, needCopyEvent bool) (map[string]*StoreResult, *events.FailedEvents, *events.SkippedEvents, error)
//	SyncStore(overriddenDataSchema *schema.BatchHeader, objects []map[string]interface{}, deleteConditions *base.DeleteConditions, cacheTable bool, needCopyEvent bool) error
//	storeTable(fdata *schema.ProcessedFile) (*adapters.Table, error)
//	ReplaceTable(originalTable, replacementTable string, dropOldTable bool) error
//	DropTable(tableName string) error
//
//	//Update(object map[string]interface{}) error
//	Fallback(events ...*events.FailedEvent)
//	GetUsersRecognition() *UserRecognitionConfiguration
//	GetSyncWorker() *SyncWorker
//	GetUniqueIDField() *identifiers.UniqueID
//	getAdapters() (adapters.SQLAdapter, *TableHelper)
//	Processor() *schema.Processor
//	Init(config *Config, impl Storage, preinstalledJavaScript string, defaultUserTransform string) error
//	Start(config *Config) error
//	ID() string
//	Type() string
//	IsStaging() bool
//	IsCachingDisabled() bool
//	Clean(tableName string) error
//}
