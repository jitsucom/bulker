package sql

import (
	"github.com/jitsucom/bulker/types"
	"strconv"
	"strings"
	"time"
)

type ColumnScanner struct {
	value any
}

func (s *ColumnScanner) Scan(src any) error {
	switch v := src.(type) {
	case []byte:
		s.value = append([]byte{}, v...)
	case int64:
		s.value = int(v)
	case time.Time:
		if v.Location().String() == "" {
			s.value = v.UTC()
		} else {
			s.value = v
		}
	default:
		s.value = src
	}
	return nil
}

func (s *ColumnScanner) Get() any {
	return s.value
}

// ToWhenConditions generates WHEN clause for SQL query based on provided WhenConditions
//
// paramExpression - SQLParameterExpression function that produce parameter placeholder for parametrized query,
// depending on database can be: IndexParameterPlaceholder, QuestionMarkParameterPlaceholder, NamedParameterPlaceholder
//
// valuesShift - for parametrized query index of first when clause value in all values provided to query
// (for UPDATE queries 'valuesShift' = len(object fields))
func ToWhenConditions(conditions *WhenConditions, paramExpression ParameterPlaceholder, valuesShift int) (string, []any) {
	if conditions == nil {
		return "", []any{}
	}
	var queryConditions []string
	var values []any

	for i, condition := range conditions.Conditions {
		switch strings.ToLower(condition.Clause) {
		case "is null":
		case "is not null":
			queryConditions = append(queryConditions, condition.Field+" "+condition.Clause)
		default:
			queryConditions = append(queryConditions, condition.Field+" "+condition.Clause+" "+paramExpression(i+valuesShift+1, condition.Field))
			values = append(values, types.ReformatValue(condition.Value))
		}
	}

	return strings.Join(queryConditions, " "+conditions.JoinCondition+" "), values
}

type ParameterPlaceholder func(i int, name string) string

var IndexParameterPlaceholder = func(i int, name string) string {
	return "$" + strconv.Itoa(i)
}

var QuestionMarkParameterPlaceholder = func(i int, name string) string {
	return "?"
}

var NamedParameterPlaceholder = func(i int, name string) string {
	return "@" + name
}
