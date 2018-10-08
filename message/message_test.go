package message_test

import (
	"testing"

	"github.com/heetch/felice/message"
	"github.com/stretchr/testify/require"
)

// message.New returns an empty message on the "test" topic without error.
func TestNew(t *testing.T) {
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
