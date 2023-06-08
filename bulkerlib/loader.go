package bulkerlib

//
//// TODO: return failed objects back to the caller
//// Loader helper class that allows to use batch approach with bulker instead of streaming object 1 by 1.
//type Loader struct {
//	bulker  Bulker
//	mode    BulkMode
//	options []StreamOption
//}
//
//// Load array of object to the `tableName` table
//func (b *Loader) Load(id, tableName string, objects []types.Object) (State, error) {
//	return b.load(id, tableName, objects)
//}
//
//// LoadWithSchema array of object to the table with name and structure defined by provided `table` object.
//func (b *Loader) LoadWithSchema(id string, table *sql.BatchHeader, objects []types.Object) (State, error) {
//	return b.load(id, table.TableName, objects, WithCustomTypes(table.Fields))
//}
//
//// load - underlying implementation that creates a bulker stream, loads objects to it, calls Complete and returns results.
//func (b *Loader) load(id, tableName string, objects []types.Object, options ...StreamOption) (State, error) {
//	stream, err := b.bulker.CreateStream(id, tableName, b.mode, append(b.options, options...)...)
//	if err != nil {
//		return State{LastError: err, Status: Failed}, err
//	}
//	for _, object := range objects {
//		err := stream.Consume(object)
//		if err != nil {
//			return stream.Abort()
//		}
//
//	}
//	return stream.Complete()
//}
//
//// NewAutoCommitLoader helper method that creates Loader for bulker BulkerStream in Stream mode
//func NewAutoCommitLoader(bulker Bulker, options ...StreamOption) *Loader {
//	bulkLoader := &Loader{
//		bulker:  bulker,
//		options: options,
//		mode:    Stream,
//	}
//	return bulkLoader
//}
//
//// NewTransactionalLoader helper method that creates Loader for bulker BulkerStream in Batch mode
//func NewTransactionalLoader(bulker Bulker, options ...StreamOption) *Loader {
//	bulkLoader := &Loader{
//		bulker:  bulker,
//		options: options,
//		mode:    Batch,
//	}
//	return bulkLoader
//}
//
//// NewReplaceTableLoader helper method that creates Loader for bulker BulkerStream in ReplaceTable mode
//func NewReplaceTableLoader(bulker Bulker, options ...StreamOption) *Loader {
//	bulkLoader := &Loader{
//		bulker:  bulker,
//		options: options,
//		mode:    ReplaceTable,
//	}
//	return bulkLoader
//}
//
//// NewReplacePartitionLoader helper method that creates Loader for bulker stream in ReplacePartition mode
////
//// partitionId - value of partitionId property for current BulkerStream e.g. id of current partition
//func NewReplacePartitionLoader(bulker Bulker, partitionId string, options ...StreamOption) *Loader {
//	bulkLoader := &Loader{
//		bulker:  bulker,
//		options: append(options, WithPartition(partitionId)),
//		mode:    ReplacePartition,
//	}
//	return bulkLoader
//}
