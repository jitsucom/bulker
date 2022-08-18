package logging

import "io"

type ObjectLogger interface {
	io.Closer
	Consume(event map[string]any, tokenID string)
	ConsumeAny(obj any)
}
