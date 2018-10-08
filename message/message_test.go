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

}
