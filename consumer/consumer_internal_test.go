package consumer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/heetch/felice/message"
)

type testHandler struct{}

func (h *testHandler) HandleMessage(m *message.Message) error {
	return nil
}

func TestHandle(t *testing.T) {
	c := &Consumer{}
	c.Handle("topic", &testHandler{})

	res, ok := c.handlers.Get("topic")
	require.True(t, ok)
	require.NotNil(t, res)
}
