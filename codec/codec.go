package codec

// A Codec can encode and decode values.
type Codec interface {
	Encode(v interface{}) ([]byte, error)
	Decode(data []byte, target interface{}) error
}
