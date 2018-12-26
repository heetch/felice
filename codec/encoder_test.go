package codec

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncoder(t *testing.T) {
	// checks whether the encoder is working properly when calling Length first
	t.Run("Length first", func(t *testing.T) {
		enc := IntEncoder(10)
		require.Equal(t, 2, enc.Length())
		d, err := enc.Encode()
		require.NoError(t, err)
		require.EqualValues(t, "10", d)
	})

	// checks whether the encoder is working properly when calling Encode first
	t.Run("Encode first", func(t *testing.T) {
		enc := FloatEncoder(10.5)
		d, err := enc.Encode()
		require.NoError(t, err)
		require.EqualValues(t, "10.5", d)
		require.Equal(t, 4, enc.Length())
	})

	// checks whether Length is working properly when the encode method returns an error
	t.Run("Error", func(t *testing.T) {
		enc := encoder{codec: Int64(), v: "hello"}
		require.Equal(t, 0, enc.Length())
		_, err := enc.Encode()
		require.Error(t, err)
	})

}
