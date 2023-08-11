package airbyte

// MessageTracker is used to encap State tracking, Record tracking and Log tracking
// It's thread safe
type MessageTracker struct {
	// State will save an arbitrary JSON blob to airbyte state
	State StateWriter
	// Record will emit a record (data point) out to airbyte to sync with appropriate timestamps
	Record RecordWriter
	// Log logs out to airbyte
	Log LogWriter
}

// LogTracker is a single struct which holds a tracker which can be used for logs
type LogTracker struct {
	Log LogWriter
}
