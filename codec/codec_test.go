package codec_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/heetch/felice/codec"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestEncoding(t *testing.T) {
	tests := []struct {
		name     string
		from     interface{}
		expected string
		codec    codec.Codec
	}{
		{"string/string", "hello", "hello", codec.String()},
		{"string/byte-slice", []byte("hello"), "hello", codec.String()},
		{"string/error", errors.New("hello"), "hello", codec.String()},
		{"string/stringer", bytes.NewBuffer([]byte("hello")), "hello", codec.String()},
		{"int64/int64", int64(10), "10", codec.Int64()},
		{"float64/float64", 3.14, "3.14", codec.Float64()},
		{"json/string", "hello", `"hello"`, codec.JSON()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := test.codec.Encode(test.from)
			require.NoError(t, err)
			require.Equal(t, test.expected, string(res))
		})
	}
}

func TestEncodingErrors(t *testing.T) {
	tests := []struct {
		name      string
		from      interface{}
		errString string
		codec     codec.Codec
	}{
		{"string/int", 10, "10 must be a string, a stringer, an error or a byte slice, got int instead", codec.String()},
		{"int64/string", "hello", "hello must be an int64, got string instead", codec.Int64()},
		{"float64/string", "hello", "hello must be a float64, got string instead", codec.Float64()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.codec.Encode(test.from)
			require.Error(t, err)
			require.EqualError(t, err, test.errString)
		})
	}
}

func TestDecoding(t *testing.T) {
	tests := []struct {
		name     string
		data     string
		to       interface{}
		expected interface{}
		codec    codec.Codec
	}{
		{"string/string", "hello", new(string), "hello", codec.String()},
		{"string/byte-slice", "hello", new([]byte), []byte("hello"), codec.String()},
		{"int64/int64", "-10", new(int64), int64(-10), codec.Int64()},
		{"float64/float64", "-3.14", new(float64), -3.14, codec.Float64()},
		{"json/string", `"hello"`, new(string), "hello", codec.JSON()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.codec.Decode([]byte(test.data), test.to)
			require.NoError(t, err)
			require.Equal(t, test.expected, reflect.Indirect(reflect.ValueOf(test.to)).Interface())
		})
	}
}

func TestDecodingErrors(t *testing.T) {
	tests := []struct {
		name      string
		data      string
		to        interface{}
		errString string
		codec     codec.Codec
	}{
		{"string/int", "hello", new(int64), "target must be a pointer to string or to a byte slice, got *int64 instead", codec.String()},
		{"int64/string", "10", new(string), "target must be a pointer to int64, got *string instead", codec.Int64()},
		{"float64/string", "3.14", new(string), "target must be a pointer to float64, got *string instead", codec.Float64()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.codec.Decode([]byte(test.data), test.to)
			require.Error(t, err)
			require.EqualError(t, err, test.errString)
		})
	}
}

func TestRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		v     interface{}
		to    interface{}
		codec codec.Codec
	}{
		{"string/string", "hello", new(string), codec.String()},
		{"string/byte-slice", []byte("hello"), new([]byte), codec.String()},
		{"int64/int64", int64(-10), new(int64), codec.Int64()},
		{"float64/float64", -3.14, new(float64), codec.Float64()},
		{"json/string", "hello", new(string), codec.JSON()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := test.codec.Encode(test.v)
			require.NoError(t, err)

			err = test.codec.Decode(data, test.to)
			require.NoError(t, err)
			require.Equal(t, reflect.Indirect(reflect.ValueOf(test.v)).Interface(), reflect.Indirect(reflect.ValueOf(test.to)).Interface())
		})
	}
}
