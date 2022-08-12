package types

import (
	"github.com/jitsucom/bulker/base/logging"
	"regexp"
	"time"
)

const PartitonIdKeyword = "__partition_id_"

var BigQueryPartitonIdRegex = regexp.MustCompile("(\\w+)/(\\d\\d\\d\\d-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\dZ)")

// DeleteCondition is a representation of SQL delete condition
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

// DeleteConditions is a dto for multiple DeleteCondition instances with Joiner
type DeleteConditions struct {
	Conditions    []DeleteCondition
	Partition     DatePartition
	JoinCondition string
}

// IsEmpty returns true if there is no conditions
func (dc *DeleteConditions) IsEmpty() bool {
	return dc == nil || len(dc.Conditions) == 0
}

// DeleteByPartitionId return delete condition that removes objects based on __partition_id value
// or empty condition if partitonId is empty
func DeleteByPartitionId(partitonId string) *DeleteConditions {
	if partitonId == "" {
		return &DeleteConditions{}
	}
	datePartition := DatePartition{}
	matches := BigQueryPartitonIdRegex.FindStringSubmatch(partitonId)
	if len(matches) == 2 {
		granularity, err := ParseGranularity(matches[1])
		if err == nil {
			partitionTime, err := time.Parse(time.RFC3339Nano, matches[2])
			if err == nil {
				datePartition = DatePartition{Field: PartitonIdKeyword, Value: granularity.Lower(partitionTime), Granularity: granularity}
			} else {
				logging.SystemErrorf("DeleteByPartitionId: failed to parse time from partitionId: %s error:", partitonId, err)
			}
		} else {
			logging.SystemErrorf("DeleteByPartitionId: failed to parse granularity from partitionId: %s error:", partitonId, err)
		}

	}
	return &DeleteConditions{
		JoinCondition: "AND",
		Partition:     datePartition,
		Conditions:    []DeleteCondition{{Field: PartitonIdKeyword, Clause: "=", Value: partitonId}},
	}
}
