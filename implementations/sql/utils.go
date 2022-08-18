package sql

type ColumnScanner struct {
	value any
}

func (s *ColumnScanner) Scan(src any) error {
	switch v := src.(type) {
	case []byte:
		s.value = append([]byte{}, v...)
	default:
		s.value = src
	}
	return nil
}

func (s *ColumnScanner) Get() any {
	return s.value
}
