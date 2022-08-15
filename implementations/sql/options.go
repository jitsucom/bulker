package sql

import "github.com/jitsucom/bulker/bulker"

// WithCustomTypes provides overrides for types of current BulkerStream object fields
func WithCustomTypes(fields Fields) bulker.StreamOption {
	return func(options *bulker.StreamOptions) {
		//options.CustomTypes = fields
	}
}

// TODO: default options depending on destination implementation
var DefaultStreamOptions = bulker.StreamOptions{}
