package consumer

import (
	"testing"

	cluster "github.com/bsm/sarama-cluster"
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

// PartitionConsumerMock implements the sarama's PartitionConsumer interface for testing purposes.
// The sarama library already defines a mock for it but we can't use it because it doesn't allow a partition consumer
// to be created in such a way that you can push messages to it without creating the expectation that ConsumePartition will be called.
type PartitionConsumerMock struct{}

func TestConsumerHandlePartitionsOnClosedChannel(t *testing.T) {
	c := Consumer{}
	ch := make(chan cluster.PartitionConsumer)

	close(ch)
	err := c.handlePartitions(ch)
	require.EqualError(t, err, "partition consumer channel closed")
}
