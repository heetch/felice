package codec

// Encoder is a simple interface for any type that can be encoded as an array of bytes
// in order to be sent as the key or value of a Kafka message. It matches the sarama.Encoder interface.
type Encoder interface {
	Encode() ([]byte, error)
	Length() int
}

// StringEncoder creates a Encoder that encodes using the String codec.
func StringEncoder(v string) Encoder {
	return &encoder{codec: String(), v: v}
}

// Int64Encoder creates a Encoder that encodes using the Int64 codec.
func Int64Encoder(v int) Encoder {
	return &encoder{codec: Int64(), v: int64(v)}
}

// Float64Encoder creates a Encoder that encodes using the Float64 Codec.
func Float64Encoder(v float64) Encoder {
	return &encoder{codec: Float64(), v: v}
}

// JSONEncoder creates a Encoder that encodes using the JSON Codec.
func JSONEncoder(v interface{}) Encoder {
	return &encoder{codec: JSON(), v: v}
}

// encoder implements the Encoder interface.
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
