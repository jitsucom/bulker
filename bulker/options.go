package bulker

import "fmt"

type StreamOption func(*StreamOptions)

var optionsRegistry = make(map[string]ParseableOption)

//type StreamOption interface {
//	Apply(*StreamOptions)
//}

func RegisterOption[V any](option *ImplementationOption[V]) {
	optionsRegistry[option.Key] = option
}

func ParseOption(name string, serialized any) (StreamOption, error) {
	option, ok := optionsRegistry[name]
	if !ok {
		return nil, fmt.Errorf("unknown option %s", name)
	}
	return option.Parse(serialized)

}

type StreamOptions struct {
	ImplementationOptions map[string]any
}

type ParseableOption interface {
	Parse(serialized any) (StreamOption, error)
}

type ImplementationOption[V any] struct {
	Key          string
	DefaultValue V
	ParseFunc    func(*ImplementationOption[V], any) (StreamOption, error)
}

func (io *ImplementationOption[V]) Parse(serializedValue any) (StreamOption, error) {
	return io.ParseFunc(io, serializedValue)
}

func (io *ImplementationOption[V]) Get(so *StreamOptions) V {
	opt, ok := so.ImplementationOptions[io.Key].(V)
	if ok {
		return opt
	}
	return io.DefaultValue
}

func (io *ImplementationOption[V]) Set(so *StreamOptions, value V) {
	if so.ImplementationOptions == nil {
		so.ImplementationOptions = map[string]any{io.Key: value}
	} else {
		so.ImplementationOptions[io.Key] = value
	}
}
