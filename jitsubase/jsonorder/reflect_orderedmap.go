package jsonorder

import (
	"fmt"
	"unsafe"

	"github.com/modern-go/reflect2"
)

var orderedMapPtr = &OrderedMap[string, any]{}
var orderedMapType = reflect2.TypeOf(orderedMapPtr)

func decoderOfOrderedMap(ctx *ctx, typ reflect2.Type) ValDecoder {
	if !typ.AssignableTo(orderedMapType) {
		return nil
	}
	keyType := reflect2.TypeOfPtr((*string)(nil)).Elem()
	valueType := reflect2.TypeOfPtr((*any)(nil)).Elem()
	keyDecoder := decoderOfMapKey(ctx.append("[omapKey]"), keyType)
	elemDecoder := decoderOfType(ctx.append("[omapElem]"), valueType)
	return &orderedMapDecoder{
		keyType:     keyType,
		elemType:    valueType,
		keyDecoder:  keyDecoder,
		elemDecoder: elemDecoder,
	}
}

func encoderOfOrderedMap(ctx *ctx, typ reflect2.Type) ValEncoder {
	if !typ.AssignableTo(orderedMapType) {
		return nil
	}
	return &orderedMapEncoder{
		keyEncoder:  encoderOfMapKey(ctx.append("[omapKey]"), reflect2.TypeOfPtr((*string)(nil)).Elem()),
		elemEncoder: encoderOfType(ctx.append("[omapElem]"), reflect2.TypeOfPtr((*any)(nil)).Elem()),
	}
}

type orderedMapDecoder struct {
	keyType     reflect2.Type
	elemType    reflect2.Type
	keyDecoder  ValDecoder
	elemDecoder ValDecoder
}

func (decoder *orderedMapDecoder) Decode(ptr unsafe.Pointer, iter *Iterator) {
	keyType := decoder.keyType
	elemType := decoder.elemType

	c := iter.nextToken()
	if c == 'n' {
		iter.skipThreeBytes('u', 'l', 'l')
		*(*unsafe.Pointer)(ptr) = nil
		orderedMapType.UnsafeSet(ptr, orderedMapType.UnsafeNew())
		return
	}
	if orderedMapType.UnsafeIsNil(ptr) {
		mp := NewOrderedMap[string, any](0)
		orderedMapType.UnsafeSet(ptr, unsafe.Pointer(&mp))
	}
	if c != '{' {
		iter.ReportError("ReadOrderedMapCB", `expect { or n, but found `+fmt.Sprintf("%d", c))
		return
	}
	c = iter.nextToken()
	if c == '}' {
		iter.jitsuSkipWhitespaces()
		return
	}
	iter.unreadByte()
	key := keyType.UnsafeNew()
	decoder.keyDecoder.Decode(key, iter)
	c = iter.nextToken()
	if c != ':' {
		iter.ReportError("ReadOrderedMapCB", "expect : after object field, but found "+string([]byte{c}))
		return
	}
	elem := elemType.UnsafeNew()
	decoder.elemDecoder.Decode(elem, iter)
	orderedMap := orderedMapType.UnsafeIndirect(ptr).(*OrderedMap[string, any])
	orderedMap.Set(keyType.UnsafeIndirect(key).(string), elemType.UnsafeIndirect(elem))
	for c = iter.nextToken(); c == ','; c = iter.nextToken() {
		key := keyType.UnsafeNew()
		decoder.keyDecoder.Decode(key, iter)
		c = iter.nextToken()
		if c != ':' {
			iter.ReportError("ReadOrderedMapCB", "expect : after object field, but found "+string([]byte{c}))
			return
		}
		elem := elemType.UnsafeNew()
		decoder.elemDecoder.Decode(elem, iter)
		orderedMap.Set(keyType.UnsafeIndirect(key).(string), elemType.UnsafeIndirect(elem))
	}
	if c != '}' {
		iter.ReportError("ReadOrderedMapCB", `expect }, but found `+string([]byte{c}))
	} else {
		iter.jitsuSkipWhitespaces()
	}
}

type orderedMapEncoder struct {
	keyEncoder  ValEncoder
	elemEncoder ValEncoder
}

func (encoder *orderedMapEncoder) Encode(ptr unsafe.Pointer, stream *Stream) {
	if *(*unsafe.Pointer)(ptr) == nil {
		stream.WriteNil()
		return
	}
	stream.WriteObjectStart()
	orderedMap := orderedMapType.UnsafeIndirect(ptr).(*OrderedMap[string, any])
	orderedMap.ForEachIndexed(func(i int, key string, elem any) {
		if i != 0 {
			stream.WriteMore()
		}
		encoder.keyEncoder.Encode(unsafe.Pointer(&key), stream)
		if stream.indention > 0 {
			stream.writeTwoBytes(byte(':'), byte(' '))
		} else {
			stream.writeByte(':')
		}
		encoder.elemEncoder.Encode(unsafe.Pointer(&elem), stream)
	})
	stream.WriteObjectEnd()
}

func (encoder *orderedMapEncoder) IsEmpty(ptr unsafe.Pointer) bool {
	orderedMap := orderedMapType.UnsafeIndirect(ptr).(*OrderedMap[string, any])
	return orderedMap.Len() == 0
}
