package sql

import (
	"fmt"
	"github.com/jitsucom/bulker/base/logging"
	"regexp"
	"time"
)

const PartitonIdKeyword = "__partition_id_"

var BigQueryPartitonIdRegex = regexp.MustCompile("(\\w+)/(\\d\\d\\d\\d-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\dZ)")

// WhenCondition is a representation of SQL delete condition
type WhenCondition struct {
	Field  string
	Value  any
	Clause string
}

type DatePartition struct {
	Field       string
	Value       time.Time
	Granularity Granularity
}

// WhenConditions is a dto for multiple WhenCondition instances with Joiner
type WhenConditions struct {
	Conditions    []WhenCondition
	JoinCondition string
}

// IsEmpty returns true if there is no conditions
func (dc *WhenConditions) IsEmpty() bool {
	return dc == nil || len(dc.Conditions) == 0
}

// ByPartitionId return delete condition that removes objects based on __partition_id value
// or empty condition if partitonId is empty
func ByPartitionId(partitonId string) *WhenConditions {
	if partitonId == "" {
		return &WhenConditions{}
	}

	return &WhenConditions{
		JoinCondition: "AND",
		Conditions:    []WhenCondition{{Field: PartitonIdKeyword, Clause: "=", Value: partitonId}},
	}
}

// Granularity is a granularity of TimeInterval
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

// Lower returns the lower value of interval
func (g Granularity) Lower(t time.Time) time.Time {
	switch g {
	case HOUR:
		return t.UTC().Truncate(time.Hour)
	case DAY:
		return t.UTC().Truncate(time.Hour * 24)
	case WEEK:
		return t.UTC().Truncate(time.Hour * 24 * 7)
	case MONTH:
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
	case QUARTER:
		return time.Date(t.Year(), t.Month()-(t.Month()-1)%3, 1, 0, 0, 0, 0, t.Location())
	case YEAR:
		return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location())
	case ALL:
		return time.Time{}
	default:
		logging.SystemError("Unknown granularity:", g)
		return time.Time{}
	}
}

// Upper returns the upper value of interval
func (g Granularity) Upper(t time.Time) time.Time {
	switch g {
	case HOUR:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location()).Add(time.Hour).Add(-time.Nanosecond)
	case DAY:
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).AddDate(0, 0, 1).Add(-time.Nanosecond)
	case WEEK:
		return t.UTC().Truncate(time.Hour*24*7).AddDate(0, 0, 7).Add(-time.Nanosecond)
	case MONTH:
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location()).AddDate(0, 1, 0).Add(-time.Nanosecond)
	case QUARTER:
		return time.Date(t.Year(), t.Month()-(t.Month()-1)%3, 1, 0, 0, 0, 0, t.Location()).AddDate(0, 3, 0).Add(-time.Nanosecond)
	case YEAR:
		return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location()).AddDate(1, 0, 0).Add(-time.Nanosecond)
	case ALL:
		return time.Time{}
	default:
		logging.SystemError("Unknown granularity:", g)
		return time.Time{}
	}
}

// Format returns formatted string value representation
func (g Granularity) Format(t time.Time) string {
	switch g {
	case HOUR:
		return t.Format("2006-01-02_15")
	case DAY:
		return t.Format("2006-01-02")
	case WEEK:
		return t.Format("2006-01-02")
	case MONTH:
		return t.Format("2006-01")
	case QUARTER:
		return t.Format("2006-01")
	case YEAR:
		return t.Format("2006")
	case ALL:
		return "ALL"
	default:
		logging.SystemError("Unknown granularity:", g)
		return ""
	}
}

// String returns string value representation
func (g Granularity) String() string {
	switch g {
	case HOUR:
		return string(HOUR)
	case DAY:
		return string(DAY)
	case WEEK:
		return string(WEEK)
	case MONTH:
		return string(MONTH)
	case QUARTER:
		return string(QUARTER)
	case YEAR:
		return string(YEAR)
	case ALL:
		return string(ALL)
	default:
		return ""
	}
}

// ParseGranularity returns Granularity value from string
func ParseGranularity(s string) (Granularity, error) {
	switch s {
	case string(HOUR):
		return HOUR, nil
	case string(DAY):
		return DAY, nil
	case string(WEEK):
		return WEEK, nil
	case string(MONTH):
		return MONTH, nil
	case string(QUARTER):
		return QUARTER, nil
	case string(YEAR):
		return YEAR, nil
	case string(ALL):
		return ALL, nil
	default:
		return "", fmt.Errorf("unknown granularity: %s", s)
	}
}
