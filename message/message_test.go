package message_test

import (
	"testing"

	"github.com/heetch/felice/message"
	"github.com/stretchr/testify/require"
)

// message.New returns a message with a valid JSON encoded body when a nil value is passed.
func TestNewWithEmptyValue(t *testing.T) {
	msg, err := message.New("test", nil)
	require.NoError(t, err)
	require.NotNil(t, msg)

	require.NotZero(t, msg.ID)
	require.Equal(t, "test", msg.Topic)
	require.Len(t, msg.Headers, 2)

	require.Equal(t, msg.ID, msg.Headers["Message-Id"])
	require.NotZero(t, msg.Headers["Produced-At"])
	// "null" is the JSON representation of nil
	require.Equal(t, []byte("null"), msg.Body)
}

// message.New should return an error if no topic name is provided.
func TestNewWithEmptyTopic(t *testing.T) {
	_, err := message.New("", "Foo")
	require.EqualError(t, err, "messages require a non-empty topic")
}

// message.New should return an error if the value provided cannot be marshaled
func TestNewWithValueWhichCannotBeMarshaled(t *testing.T) {
	_, err := message.New("test", make(chan bool))
	require.EqualError(t, err, "failed to encode message body: json: unsupported type: chan bool")
}

// message.New returns a valid Message when provided valid parameters.
func TestNew(t *testing.T) {
	msg, err := message.New("test", "message")
	require.NoError(t, err)
	require.NotNil(t, msg)

	require.NotZero(t, msg.ID)
	require.Equal(t, "test", msg.Topic)
	require.Len(t, msg.Headers, 2)

	require.Equal(t, msg.ID, msg.Headers["Message-Id"])
	require.NotZero(t, msg.Headers["Produced-At"])
	require.Equal(t, []byte(`"message"`), msg.Body)
}

// message.New invokes all provided Options on the message it constructs.
func TestNewWithOptions(t *testing.T) {
	msg, err := message.New(
		"test", "message", message.Header("Shoe", "Wellington"), message.Key("apple"))
	require.NoError(t, err)
	require.Len(t, msg.Headers, 3)
	require.Equal(t, "Wellington", msg.Headers["Shoe"])

	require.Equal(t, "apple", msg.Key)
}
