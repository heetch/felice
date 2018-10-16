package consumer

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/stretchr/testify/require"

	"github.com/heetch/felice/message"
)

type testHandler struct {
	t         *testing.T
	testCase  func(*message.Message) (string, func(t *testing.T))
	CallCount int
}

func (h *testHandler) HandleMessage(m *message.Message) error {
	h.CallCount++
	if h.testCase != nil {
		h.t.Run(h.testCase(m))
	}
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

func TestConsumerHandleMessages(t *testing.T) {
	c := Consumer{}
	handler := &testHandler{
		t: t,
		testCase: func(m *message.Message) (string, func(t *testing.T)) {
			return "topic", func(t *testing.T) {
				require.Equal(t, "topic", m.Topic)
				require.EqualValues(t, "body", m.Body)
				require.EqualValues(t, "key", m.Key)
			}
		},
	}

	c.Handle("topic", handler)

	ch := make(chan *sarama.ConsumerMessage, 1)
	ch <- &sarama.ConsumerMessage{
		Topic: "topic",
		Key:   []byte("key"),
		Value: []byte("body"),
	}
	close(ch)

	hwm := &mockHighWaterMarker{}
	mos := &mockOffsetStash{}
	c.handleMessages(ch, mos, hwm)

	require.Equal(t, 1, handler.CallCount)
}

type metricsHook struct {
	ReportsCount int
	t            *testing.T
	testCase     func(msg message.Message, metadatas map[string]string) (string, func(*testing.T))
}

func (mmh *metricsHook) Reports(msg message.Message, metadatas map[string]string) {
	mmh.ReportsCount++
	mmh.t.Run(mmh.testCase(msg, metadatas))
}

func TestConsumerHandleMessagesMetricsReporting(t *testing.T) {
	c := Consumer{}
	mmh := &metricsHook{
		t: t,
		testCase: func(msg message.Message, meta map[string]string) (string, func(t *testing.T)) {
			return "metrics", func(t *testing.T) {
				require.Equal(t, "topic", msg.Topic)
				require.EqualValues(t, "body", msg.Body)
				require.EqualValues(t, "key", msg.Key)
			}
		},
	}
	c.SetMetricsHook(mmh)
	handler := &testHandler{}

	c.Handle("topic", handler)
	ch := make(chan *sarama.ConsumerMessage, 1)
	ch <- &sarama.ConsumerMessage{
		Topic: "topic",
		Key:   []byte("key"),
		Value: []byte("body"),
	}
	close(ch)

	hwm := &mockHighWaterMarker{}
	mos := &mockOffsetStash{}
	c.handleMessages(ch, mos, hwm)

	require.Equal(t, 1, mmh.ReportsCount)

}

func TestConvertMessage(t *testing.T) {
	c := &Consumer{}
	now := time.Now()
	sm := &sarama.ConsumerMessage{
		Topic:     "topic",
		Key:       []byte("key"),
		Value:     []byte("body"),
		Timestamp: now,
		Offset:    10,
		Partition: 10,
	}

	msg := c.convertMessage(sm)
	require.Equal(t, sm.Topic, msg.Topic)
	require.EqualValues(t, sm.Key, msg.Key)
	require.EqualValues(t, sm.Value, msg.Body)
	require.Equal(t, sm.Timestamp, msg.ProducedAt)
	require.Equal(t, sm.Offset, msg.Offset)
	require.Equal(t, sm.Partition, msg.Partition)
}

type mockOffsetStash struct {
	MarkOffsetCount int
}

func (m *mockOffsetStash) MarkOffset(msg *sarama.ConsumerMessage, metadata string) {
	m.MarkOffsetCount++
	msg.Offset++
}

type mockHighWaterMarker struct {
	HighWaterMarkOffsetCount int
}

func (m *mockHighWaterMarker) HighWaterMarkOffset() int64 {
	m.HighWaterMarkOffsetCount++
	return 0
}
