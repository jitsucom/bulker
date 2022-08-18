package utils

type RichError struct {
	error   string
	payload any
}

func NewRichError(error string, payload any) *RichError {
	return &RichError{error: error, payload: payload}
}

func (r *RichError) Error() string {
	return r.error
}

func (r *RichError) Payload() any {
	return r.payload
}
