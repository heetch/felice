package message_test

import (
	"testing"

	"github.com/heetch/felice/message"
	"github.com/stretchr/testify/require"
)

// Header returns an option function which will add a Header to a message.
func TestHeader(t *testing.T) {
	var fn message.Option
	fn = message.Header("Shoe", "Brogue")
	msg := message.Message{
		Headers: make(map[string]string),
	}
	fn(&msg)
	require.Len(t, msg.Headers, 1)
	require.Equal(t, "Brogue", msg.Headers["Shoe"])
}

// Key returns an Option function which will set the message key.
func TestKey(t *testing.T) {
	var fn message.Option
	fn = message.Key("banana")
	msg := message.Message{}
	fn(&msg)
	require.Equal(t, "banana", msg.Key)
}
