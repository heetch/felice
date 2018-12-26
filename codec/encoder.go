package codec

import (
	"github.com/Shopify/sarama"
)

// IntEncoder creates a sarama.Encoder that encodes using the Int64 codec.
func IntEncoder(v int) sarama.Encoder {
	return &encoder{codec: Int64(), v: int64(v)}
}

// FloatEncoder creates a sarama.Encoder that encodes using the Float64 Codec.
func FloatEncoder(v float64) sarama.Encoder {
	return &encoder{codec: Float64(), v: v}
}

// JSONEncoder creates a sarama.Encoder that encodes using the JSON Codec.
func JSONEncoder(v interface{}) sarama.Encoder {
	return &encoder{codec: JSON(), v: v}
}

// encoder implements the sarama.Encoder interface.
type encoder struct {
	codec Codec
	v     interface{}
	cache []byte
	err   error
}

// Length will usually be called by Sarama before Encode.
// It calls encode and caches the result before returning the
// length of the output. If there's an error, it must postpone the error reporting
// to when Sarama calls the Encode method.
func (e *encoder) Length() int {
	if e.cache != nil {
		return len(e.cache)
	}

	e.Encode()
	return len(e.cache)
}

// Encode returns the cached result if any, otherwise it encodes
// v using the registered codec.
func (e *encoder) Encode() ([]byte, error) {
	if e.err != nil {
		return nil, e.err
	}

	if e.cache != nil {
		return e.cache, nil
	}

	e.cache, e.err = e.codec.Encode(e.v)
	return e.cache, e.err
}
