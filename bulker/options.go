package bulker

import (
	"fmt"
	"github.com/jitsucom/bulker/base/utils"
)

type StreamOption func(*StreamOptions)

var optionsRegistry = make(map[string]ParseableOption)

var (
	BatchSizeOption = ImplementationOption[int]{
		Key:          "batchSize",
		DefaultValue: 0,
		ParseFunc:    utils.ParseInt,
	}

	// BatchFrequencyOption frequency of running batches in minutes
	BatchFrequencyOption = ImplementationOption[float64]{
		Key:          "frequency",
		DefaultValue: 0,
		ParseFunc:    utils.ParseFloat,
	}

	RetryBatchSizeOption = ImplementationOption[int]{
		Key:          "retryBatchSize",
		DefaultValue: 0,
		ParseFunc:    utils.ParseInt,
	}
	// RetryFrequencyOption frequency of running retry consumer in minutes
	RetryFrequencyOption = ImplementationOption[float64]{
		Key:          "retryFrequency",
		DefaultValue: 0,
		ParseFunc:    utils.ParseFloat,
	}

	ModeOption = ImplementationOption[BulkMode]{Key: "mode", ParseFunc: func(serialized any) (BulkMode, error) {
		switch v := serialized.(type) {
		case string:
			if v == "stream" {
				return Stream, nil
			} else if v == "batch" {
				return Batch, nil
			} else {
				return Unknown, fmt.Errorf("unknown mode: %s", v)
			}
		default:
			return Unknown, fmt.Errorf("invalid value type of mode option: %T", v)
		}
	},
	}

	// Not used by bulker. Just added here to be treated as known options
	FunctionsOption  = ImplementationOption[any]{Key: "functions", ParseFunc: func(serialized any) (any, error) { return nil, nil }}
	DataLayoutOption = ImplementationOption[string]{Key: "dataLayout", ParseFunc: utils.ParseString}
	EventsOption     = ImplementationOption[string]{Key: "events", ParseFunc: utils.ParseString}
	HostsOption      = ImplementationOption[string]{Key: "hosts", ParseFunc: utils.ParseString}
)

func init() {
	RegisterOption(&ModeOption)
	RegisterOption(&BatchSizeOption)
	RegisterOption(&BatchFrequencyOption)
	RegisterOption(&RetryFrequencyOption)
	RegisterOption(&RetryBatchSizeOption)

	// Not used by bulker. Just added here to be treated as known options
	RegisterOption(&FunctionsOption)
	RegisterOption(&DataLayoutOption)
	RegisterOption(&EventsOption)
	RegisterOption(&HostsOption)

}

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
	// Implementation options - map by option key. Values are parsed and validated
	// Don't access this map directly, use 'Get' method of specific option instance. E.g. `PartitionIdOption.Get(&so)`
	valuesMap map[string]any
	// options slice. To pass to CreateStream method
	Options []StreamOption
}

func (so *StreamOptions) Add(option StreamOption) {
	option(so)
	so.Options = append(so.Options, option)
}

type ParseableOption interface {
	Parse(serialized any) (StreamOption, error)
}

type ImplementationOption[V any] struct {
	Key               string
	DefaultValue      V
	ParseFunc         func(serialized any) (V, error)
	AdvancedParseFunc func(*ImplementationOption[V], any) (StreamOption, error)
}

func (io *ImplementationOption[V]) Parse(serializedValue any) (StreamOption, error) {
	if io.ParseFunc != nil {
		val, err := io.ParseFunc(serializedValue)
		if err != nil {
			return nil, fmt.Errorf("failed to parse '%s' option: %w", io.Key, err)
		}
		return func(options *StreamOptions) {
			io.Set(options, val)
		}, nil
	} else {
		return io.AdvancedParseFunc(io, serializedValue)
	}
}

func (io *ImplementationOption[V]) Get(so *StreamOptions) V {
	opt, ok := so.valuesMap[io.Key].(V)
	if ok {
		return opt
	}
	return io.DefaultValue
}

func (io *ImplementationOption[V]) Set(so *StreamOptions, value V) {
	if so.valuesMap == nil {
		so.valuesMap = map[string]any{io.Key: value}
	} else {
		so.valuesMap[io.Key] = value
	}
}
