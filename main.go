package bulker

type BulkMode int

const (
	AutoCommit BulkMode = iota
	Transactional
	//DateChunks will work similarly to chunkedStreamSink from js sdk
	DateChunks
	//WholeTable atomically replaces target tables similarly to js sdk soruces
	WholeTable
)

type Result int

const (
	Completed Result = iota
	CompletedWithErrors
	Aborted
	Failed
)

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

type DateChunksSettings struct {
	Field       string
	Granularity Granularity
}

type Object map[string]interface{}

//StreamsResult is used as a Batch storing result
type StreamsResult struct {
	Result         Result
	LastError      error
	ProcessedRows  int
	SuccessfulRows int
	RowsErrors     map[int]error
}

type StreamOptions struct {
	//TODO: Use abstract data types (STRING, BOOL, TIMESTAMP...) or SQL types?
	TableSchema        interface{}
	BulkMode           BulkMode
	DateChunksSettings DateChunksSettings
	IgnoreErrors       bool
}

type StreamOption func(*StreamOptions)

type Bulker interface {
	StartStream(tableName string, streamOptions StreamOptions) (*BulkerStream, error)
	DropTable(tableName string) error

	//TODO: consider dedicated methods for stream types (to avoid mess with incompatible Options combinations)
	StartDateChunksStream(tableName string, dateField string, granularity Granularity, streamOptions ...StreamOptions) (*BulkerStream, error)
	StartWholeTableStream(tableName string, streamOptions ...StreamOptions) (*BulkerStream, error)
	StartTransactionalStream(tableName string, streamOptions ...StreamOptions) (*BulkerStream, error)
	StartAutoCommitStream(tableName string, streamOptions ...StreamOptions) (*BulkerStream, error)
}

type BulkerStream interface {
	Consume(object Object) error
	Abort() StreamsResult
	Close() StreamsResult
}

//TODO: cleanup deprecated tables
//TODO: no primary keys and similar rows
