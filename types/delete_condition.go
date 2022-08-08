package types

import (
	"time"
)

//DeleteCondition is a representation of SQL delete condition
type DeleteCondition struct {
	Field  string
	Value  interface{}
	Clause string
}

type DatePartition struct {
	Field       string
	Value       time.Time
	Granularity Granularity
}

//DeleteConditions is a dto for multiple DeleteCondition instances with Joiner
type DeleteConditions struct {
	Conditions    []DeleteCondition
	Partition     DatePartition
	JoinCondition string
}

//IsEmpty returns true if there is no conditions
func (dc *DeleteConditions) IsEmpty() bool {
	return dc == nil || len(dc.Conditions) == 0
}

////DeleteByTimeChunkCondition return delete condition that removes objects based on eventn_ctx_time_interval value
////or empty condition if timeIntervalValue is empty
//func DeleteByTimeChunkCondition(timeInterval *TimeInterval) *DeleteConditions {
//	if timeInterval == nil {
//		return &DeleteConditions{}
//	}
//
//	return &DeleteConditions{
//		JoinCondition: "AND",
//		Partition:     DatePartition{Field: events.TimeIntervalStart, Value: timeInterval.LowerEndpoint(), Granularity: timeInterval.Granularity()},
//		Conditions:    []DeleteCondition{{Field: events.TimeChunkKey, Clause: "=", Value: timeInterval.String()}},
//	}
//}
