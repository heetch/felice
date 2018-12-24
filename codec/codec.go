package codec

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/pkg/errors"
)

// A Codec can encode and decode values.
type Codec interface {
	Encode(v interface{}) ([]byte, error)
	Decode(data []byte, target interface{}) error
}

type codecFunc struct {
	encodeFn func(v interface{}) ([]byte, error)
	decodeFn func(data []byte, target interface{}) error
}

func (c *codecFunc) Encode(v interface{}) ([]byte, error) {
	return c.encodeFn(v)
}

func (c *codecFunc) Decode(data []byte, target interface{}) error {
	return c.decodeFn(data, target)
}

// String encodes and decodes strings or byte slices into themselves.
// It is useful when passing raw data without touching it.
// The Encode method takes a byte slice, string, stringer or error and returns a byte slice.
// The Decode method turns data into v without touching it. v must be a pointer to byte slice or a pointer to string.
func String() Codec {
	return &codecFunc{
		func(v interface{}) ([]byte, error) {
			switch t := v.(type) {
			case string:
				return []byte(t), nil
			case []byte:
				return t, nil
			case fmt.Stringer:
				return []byte(t.String()), nil
			case error:
				return []byte(t.Error()), nil
			default:
				return nil, errors.Errorf("%v must be a string, a stringer, an error or a byte slice, got %t instead", v, v)
			}
		},
		func(data []byte, target interface{}) error {
			switch t := target.(type) {
			case *string:
				*t = string(data)
			case *[]byte:
				*t = data
			default:
				return errors.Errorf("%v must be a pointer to string or to a byte slice, got %t instead", target, target)
			}

			return nil
		},
	}
}

// JSON Codec handles JSON encoding.
func JSON() Codec {
	return &codecFunc{json.Marshal, json.Unmarshal}
}

// Int64 Codec handles int64 encoding.
func Int64() Codec {
	return &codecFunc{
		func(v interface{}) ([]byte, error) {
			i, ok := v.(int64)
			if !ok {
				return nil, errors.Errorf("%v must be an int64", v)
			}

			return []byte(strconv.FormatInt(i, 10)), nil
		},
		func(data []byte, target interface{}) error {
			ptr, ok := target.(*int64)
			if !ok {
				return errors.Errorf("%v must be a pointer to int64", target)
			}

			i, err := strconv.ParseInt(string(data), 10, 64)
			if err != nil {
				return err
			}

			*ptr = i
			return nil
		},
	}
}
