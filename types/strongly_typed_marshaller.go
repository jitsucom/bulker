package types

type StronglyTypedMarshaller interface {
	Marshal(bh *BatchHeader, data []Object) ([]byte, error)
}
