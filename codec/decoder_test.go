package codec

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Shopify/sarama"
)

func TestDecoder(t *testing.T) {
	tests := []struct {
		name     string
		enc      sarama.Encoder
		dec      func([]byte) Decoder
		expected interface{}
		actual   interface{}
	}{
		{"Int64", Int64Encoder(10), func(d []byte) Decoder { return Int64Decoder(d) }, 10, new(int64)},
		{"Float64", Float64Encoder(3.14), func(d []byte) Decoder { return Float64Decoder(d) }, 3.14, new(float64)},
		{"String", sarama.StringEncoder("hello"), func(d []byte) Decoder { return StringDecoder(d) }, "hello", new(string)},
		{"JSON", JSONEncoder(`"hello"`), func(d []byte) Decoder { return JSONDecoder(d) }, `"hello"`, new(string)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d, err := test.enc.Encode()
			require.NoError(t, err)

			err = test.dec(d).Decode(test.actual)
			require.NoError(t, err)

			require.EqualValues(t, reflect.Indirect(reflect.ValueOf(test.expected)).Interface(), reflect.Indirect(reflect.ValueOf(test.actual)).Interface())
		})
	}
}
