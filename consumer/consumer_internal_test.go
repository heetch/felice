package consumer

import (
	"testing"

	"github.com/Shopify/sarama"

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
type PartitionConsumerMock struct {
	MessagesCount int
	ch            chan *sarama.ConsumerMessage
}

func (pc *PartitionConsumerMock) Messages() <-chan *sarama.ConsumerMessage {
	pc.MessagesCount++
	if pc.ch == nil {
		pc.ch = make(chan *sarama.ConsumerMessage)
	}
	close(pc.ch)
	return pc.ch
}

func (pc *PartitionConsumerMock) AsyncClose() {

}

func (pc *PartitionConsumerMock) Close() error {
	return nil
}

func (pc *PartitionConsumerMock) Errors() <-chan *sarama.ConsumerError {
	return nil
}

func (pc *PartitionConsumerMock) HighWaterMarkOffset() int64 {
	return 0
}

func (pc *PartitionConsumerMock) Topic() string {
	return ""
}

func (pc *PartitionConsumerMock) Partition() int32 {
	return 0
}

func (pc *PartitionConsumerMock) InitialOffset() int64 {
	return 0
}

func (pc *PartitionConsumerMock) MarkOffset(offset int64, metadata string) {}

func (pc *PartitionConsumerMock) ResetOffset(offset int64, metadata string) {}

func TestConsumerHandlePartitionsOnClosedChannel(t *testing.T) {
	c := Consumer{}
	ch := make(chan cluster.PartitionConsumer)

	close(ch)
	err := c.handlePartitions(ch)
	require.EqualError(t, err, "partition consumer channel closed")
}

func TestConsumerHandlePartitionsWithQuit(t *testing.T) {
	c := Consumer{}
	ch := make(chan cluster.PartitionConsumer)
	c.quit = make(chan struct{}, 1)

	c.quit <- struct{}{}
	err := c.handlePartitions(ch)
	require.NoError(t, err)
}

func TestConsumerHandlePartitions(t *testing.T) {
	c := Consumer{}
	ch := make(chan cluster.PartitionConsumer, 1)

	pcm := &PartitionConsumerMock{}
	ch <- pcm
	close(ch)
	err := c.handlePartitions(ch)
	require.EqualError(t, err, "partition consumer channel closed")
	c.wg.Wait()
	require.Equal(t, 1, pcm.MessagesCount)
}
