package consumer_test

import (
	"fmt"
	"testing"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/heetch/felice/common"
	"github.com/heetch/felice/consumer"
	"github.com/heetch/felice/consumer/handler"
	"github.com/heetch/felice/message"
	"github.com/stretchr/testify/require"
)

var nilHandler = handler.HandlerFunc(func(m *message.Message) error {
	return nil
})

// Consumer.Handle emits log messages
func TestHandleLogs(t *testing.T) {
	tl := common.NewTestLogger(t)
	defer tl.TearDown()
	c := &consumer.Consumer{}
	c.Handle("foo", nilHandler)
	tl.LogLineMatches(`Registered handler. topic="foo"`)
}

// Serve emits logs when it cannot create a new consumer
func TestServeLogsErrorFromNewConsumer(t *testing.T) {
	tl := common.NewTestLogger(t)
	defer tl.TearDown()
	c := &consumer.Consumer{}
	c.NewConsumer = func(addrs []string, groupID string, topics []string, config *cluster.Config) (consumer.ClusterConsumer, error) {
		return nil, fmt.Errorf("oh noes! it doesn't work!")
	}
	c.Handle("foo", nilHandler)
	err := c.Serve("foo")
	require.Error(t, err)
	tl.SkipLogLine("registering handler")
	tl.LogLineMatches(`failed to create a consumer for topics \[foo\] in consumer group "foo-consumer-group": oh noes! it doesn't work!`)
}
