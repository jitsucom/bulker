package sql

import (
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
