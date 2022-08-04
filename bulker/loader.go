package bulker

import "github.com/jitsucom/bulker/types"

//Loader helper class that allows to use batch approach with bulker instead of streaming object 1 by 1.
type Loader struct {
	bulker  Bulker
	mode    BulkMode
	options []StreamOption
}

//Load array of object to the `tableName` table
func (b *Loader) Load(tableName string, objects []Object) (*Result, error) {
	return b.load(tableName, objects)
}

//LoadWithSchema array of object to the table with name and structure defined by provided `table` object.
func (b *Loader) LoadWithSchema(table *types.Table, objects []Object) (*Result, error) {
	return b.load(table.Schema+"."+table.Name, objects, WithTable(table))
}

//load - underlying implementation that creates a bulker stream, loads objects to it, calls Complete and returns results.
func (b *Loader) load(tableName string, objects []Object, options ...StreamOption) (*Result, error) {
	stream, err := b.bulker.CreateStream(tableName, b.mode, append(b.options, options...)...)
	if err != nil {
		return nil, err
	}
	for _, object := range objects {
		err := stream.Consume(object)
		if err != nil {
			res := stream.Complete()
			return &res, err
		}

	}
	res := stream.Complete()
	return &res, nil
}

//NewAutoCommitLoader helper method that creates Loader for bulker Stream in AutoCommit mode
func NewAutoCommitLoader(bulker Bulker, options ...StreamOption) *Loader {
	bulkLoader := &Loader{
		bulker:  bulker,
		options: options,
		mode:    AutoCommit,
	}
	return bulkLoader
}

//NewTransactionalLoader helper method that creates Loader for bulker Stream in Transactional mode
func NewTransactionalLoader(bulker Bulker, options ...StreamOption) *Loader {
	bulkLoader := &Loader{
		bulker:  bulker,
		options: options,
		mode:    Transactional,
	}
	return bulkLoader
}

//NewWholeTableLoader helper method that creates Loader for bulker Stream in WholeTable mode
func NewWholeTableLoader(bulker Bulker, options ...StreamOption) *Loader {
	bulkLoader := &Loader{
		bulker:  bulker,
		options: options,
		mode:    WholeTable,
	}
	return bulkLoader
}

//NewWholePartitionLoader helper method that creates Loader for bulker stream in WholePartition mode
//
//partitionProperty - name of object property used as a partition index
//
//partitionValue - value of that property for current Stream e.g. id of current partition
func NewWholePartitionLoader(bulker Bulker, partitionColumn string, partitionValue interface{}, options ...StreamOption) *Loader {
	bulkLoader := &Loader{
		bulker:  bulker,
		options: append(options, WithPartition(partitionColumn, partitionValue)),
		mode:    WholePartition,
	}
	return bulkLoader
}
