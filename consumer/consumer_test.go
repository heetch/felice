package consumer_test

import (
	"testing"

	"github.com/heetch/felice/consumer"
)

// Consumer.Handle emits log messages
func TestHandleLogs(t *testing.T) {
	tl := consumer.NewTestLogger(t)
	c := &consumer.Consumer{Logger: tl.Logger}
	c.Handle("foo", consumer.MessageConverterV1(), consumer.HandlerFunc(func(m *consumer.Message) error {
		return nil
	}))
	tl.LogLineMatches(`Registered handler. topic="foo"`)
}
