package bulker

type BulkMode int

const (
	AutoCommit BulkMode = iota
	Transactional
	//DatePartitions will work similarly to chunkedStreamSink from js sdk
	DatePartitions
	//WholeTable atomically replaces target tables similarly to js sdk sources
	WholeTable
)

type Result int

const (
	Completed Result = iota
	CompletedWithErrors
	Aborted
	Failed
)

type Object map[string]interface{}

//BulkResult is used as a Batch storing result
type BulkResult struct {
	Result         Result
	LastError      error
	ProcessedRows  int
	SuccessfulRows int
	RowsErrors     map[int]error
}

type Bulker interface {
	StartStream(tableName string, mode BulkMode, streamOptions ...StreamOption) (BulkerStream, error)
}

type BulkerStream interface {
	Consume(object Object) error
	Abort() BulkResult
	Close() BulkResult
}

type BulkLoader struct {
	bulker  Bulker
	mode    BulkMode
	options []StreamOption
}

func NewAutoCommitLoader(bulker Bulker, options ...StreamOption) *BulkLoader {
	bulkLoader := &BulkLoader{
		bulker:  bulker,
		options: options,
		mode:    AutoCommit,
	}
	return bulkLoader
}

func NewTransactionalLoader(bulker Bulker, options ...StreamOption) *BulkLoader {
	bulkLoader := &BulkLoader{
		bulker:  bulker,
		options: options,
		mode:    Transactional,
	}
	return bulkLoader
}

func NewWholeTableLoader(bulker Bulker, options ...StreamOption) *BulkLoader {
	bulkLoader := &BulkLoader{
		bulker:  bulker,
		options: options,
		mode:    WholeTable,
	}
	return bulkLoader
}

func NewDatePartitionsLoader(bulker Bulker, datePartitionField string, granularity Granularity, options ...StreamOption) *BulkLoader {
	bulkLoader := &BulkLoader{
		bulker:  bulker,
		options: append(options, WithDatePartitions(datePartitionField, granularity)),
		mode:    DatePartitions,
	}
	return bulkLoader
}

func (b *BulkLoader) Load(tableName string, objects []Object) (*BulkResult, error) {
	return b.load(tableName, objects)
}

func (b *BulkLoader) LoadWithSchema(table interface{}, objects []Object) (*BulkResult, error) {
	//TODO: replace "tableName" with table.Name
	return b.load("tableName", objects, WithTable(table))
}

func (b *BulkLoader) load(tableName string, objects []Object, options ...StreamOption) (*BulkResult, error) {
	stream, err := b.bulker.StartStream(tableName, b.mode, append(b.options, options...)...)
	if err != nil {
		return nil, err
	}
	for _, object := range objects {
		err := stream.Consume(object)
		if err != nil {
			res := stream.Close()
			return &res, err
		}

	}
	res := stream.Close()
	return &res, nil
}

//Granularity is a granularity of TimeInterval
type Granularity string

const (
	HOUR    Granularity = "HOUR"
	DAY     Granularity = "DAY"
	WEEK    Granularity = "WEEK"
	MONTH   Granularity = "MONTH"
	QUARTER Granularity = "QUARTER"
	YEAR    Granularity = "YEAR"
	ALL     Granularity = "ALL"
)

type DatePartitionsSettings struct {
	Field       string
	Granularity Granularity
}

type streamOptions struct {
	Table interface{}

	//TODO: Chunk => Partition. Use not date. Set chunk id.
	DatePartitionsSettings DatePartitionsSettings
	IgnoreErrors           bool
}

type StreamOption func(*streamOptions)

func WithTable(table interface{}) StreamOption {
	return func(options *streamOptions) {
		options.Table = table
	}
}

func WithIgnoreErrors() StreamOption {
	return func(options *streamOptions) {
		options.IgnoreErrors = true
	}
}

func WithDatePartitions(datePartitionField string, granularity Granularity) StreamOption {
	return func(options *streamOptions) {
		options.DatePartitionsSettings = DatePartitionsSettings{
			Field:       datePartitionField,
			Granularity: granularity,
		}
	}
}

//TODO: no primary keys and similar rows
