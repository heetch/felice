package codec

// A Decoder decodes encoded data. It can be decoded
// to an arbitrary value using the Decode method.
type Decoder interface {
	Decode(interface{}) error
	Bytes() []byte
}

// Int64Decoder returns a decoder using the Int64 codec.
func Int64Decoder(data []byte) Decoder {
	return &decoder{
		data:  data,
		codec: Int64(),
	}
}

// Float64Decoder returns a decoder using the Float64 codec.
func Float64Decoder(data []byte) Decoder {
	return &decoder{
		data:  data,
		codec: Float64(),
	}
}

// StringDecoder returns a decoder using the String codec.
func StringDecoder(data []byte) Decoder {
	return &decoder{
		data:  data,
		codec: String(),
	}
}

// JSONDecoder returns a decoder using the JSON codec.
func JSONDecoder(data []byte) Decoder {
	return &decoder{
		data:  data,
		codec: JSON(),
	}
}

type decoder struct {
	data  []byte
	codec Codec
}

func (b *decoder) Decode(to interface{}) error {
	return b.codec.Decode(b.data, to)
}

func (b *decoder) Bytes() []byte {
	return b.data
}
