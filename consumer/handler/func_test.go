package handler_test

import (
	"testing"

	"github.com/heetch/felice/consumer/handler"
	"github.com/heetch/felice/message"
	"github.com/stretchr/testify/require"
)

// HandleMessageFn can be used as a Handler
func TestHandleMessageFn(t *testing.T) {
	calls := make([]bool, 0)

	var h handler.Handler
	h = handler.HandlerFunc(func(msg *message.Message) error {
		calls = append(calls, true)
		return nil
	})
	m := &message.Message{}
	err := h.HandleMessage(m)
	require.NoError(t, err)
	require.Len(t, calls, 1)
}
