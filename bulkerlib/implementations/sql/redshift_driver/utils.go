package driver

func nullStringIfEmpty(str string) *string {
	if str == "" {
		return nil
	}
	return &str
}

func coalesce[T any](strs ...*T) T {
	for _, str := range strs {
		if str != nil {
			return *str
		}
	}
	var zero T
	return zero
}
