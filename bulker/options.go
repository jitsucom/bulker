package bulker

type StreamOption func(*StreamOptions)

type StreamOptions struct {
	ImplementationOptions map[string]interface{}
}

type ImplementationOption[V any] struct {
	Key          string
	DefaultValue V
}

func (io ImplementationOption[V]) Get(so *StreamOptions) V {
	opt, ok := so.ImplementationOptions[io.Key].(V)
	if ok {
		return opt
	}
	return io.DefaultValue
}

func (io ImplementationOption[V]) Set(so *StreamOptions, value V) {
	if so.ImplementationOptions == nil {
		so.ImplementationOptions = map[string]interface{}{io.Key: value}
	} else {
		so.ImplementationOptions[io.Key] = value
	}
}
