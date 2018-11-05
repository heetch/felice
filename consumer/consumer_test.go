package consumer_test

import (
	"testing"

	"github.com/heetch/felice/common"
	"github.com/heetch/felice/consumer"
	"github.com/heetch/felice/consumer/handler"
	"github.com/heetch/felice/message"
)

// Consumer.Handle emits log messages
func TestHandleLogs(t *testing.T) {
	tl := common.NewTestLogger(t)
	defer tl.TearDown()
	c := &consumer.Consumer{}
	c.Handle("foo", handler.HandlerFunc(func(m *message.Message) error {
		return nil
	}))
	tl.LogLineMatches(`Registered handler. topic="foo"`)
}
