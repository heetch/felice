package consumer

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/stretchr/testify/require"

	"github.com/heetch/felice/message"
)

// Consumer.Handle registers a handler for a topic.
func TestHandle(t *testing.T) {
	c := &Consumer{}
	c.Handle("topic", &testHandler{})

	res, ok := c.handlers.Get("topic")
	require.True(t, ok)
	require.NotNil(t, res)
}

// Consumer.setup initialises important values on the consumer
func TestSetUp(t *testing.T) {
	c := &Consumer{}
	c.setup()
	require.NotNil(t, c.handlers)
	require.NotNil(t, c.quit)
	require.Equal(t, c.RetryInterval, time.Second)
}

// Consumer.handlePartitions exits when we close the channel of PartitionConsumers
func TestConsumerHandlePartitionsOnClosedChannel(t *testing.T) {
	c := Consumer{}
	ch := make(chan cluster.PartitionConsumer)

	close(ch)
	err := c.handlePartitions(ch)
	require.EqualError(t, err, "partition consumer channel closed")
}

// Consumer.handlePartitions exits when we send something on the Quit channel
func TestConsumerHandlePartitionsWithQuit(t *testing.T) {
	c := Consumer{}
	ch := make(chan cluster.PartitionConsumer)
	c.quit = make(chan struct{}, 1)

	c.quit <- struct{}{}
	err := c.handlePartitions(ch)
	require.NoError(t, err)
}

// Consumer.handlePartitions provides a channel of messages, from each
// PartitionConsumer, to the handleMessages function.
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

// Consumer.handleMessages calls the per-topic Handler for each
// message that arrives.
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

// Consumer.handleMessages will send message data, and some associated
// metadata to a metrics hook function that has been provided to
// the consumer via the Consumer.Metrics field.
func TestConsumerHandleMessagesMetricsReporting(t *testing.T) {
	c := Consumer{}
	mmh := &metricsHook{
		t: t,
		testCase: func(msg message.Message, meta map[string]string) (string, func(t *testing.T)) {
			return "metrics", func(t *testing.T) {
				require.Equal(t, "topic", msg.Topic)
				require.EqualValues(t, "body", msg.Body)
				require.EqualValues(t, "key", msg.Key)
				require.Equal(t, "1", meta["attempts"])
				require.Equal(t, "1", meta["msgOffset"])
				require.Equal(t, "0", meta["remainingOffset"])
			}
		},
	}
	c.Metrics = mmh
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

	require.Equal(t, 1, mmh.ReportCount)
}

// Consumer.convertMessage converts a sarama.ConsumerMessage into our
// own message.Message type.
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

// Consumer.newClusterConfig create a new configuration for the cluster.
func TestNewClusterConfig(t *testing.T) {
	c := newClusterConfig("test")
	require.Equal(t, "test", c.ClientID)
	require.True(t, c.Consumer.Return.Errors)
	require.Equal(t, sarama.V1_0_0_0, c.Version)
	require.Equal(t, cluster.StrategyRoundRobin, c.Group.PartitionStrategy)
	require.Equal(t, cluster.ConsumerModePartitions, c.Group.Mode)
}

// The mockOffsetStash implements the OffsetStash interface for test
// purposes.
type mockOffsetStash struct {
	MarkOffsetCount int
}

// MarkOffset will increment the offset on the message and keep count
// of how many times it has been called.
func (m *mockOffsetStash) MarkOffset(msg *sarama.ConsumerMessage, metadata string) {
	m.MarkOffsetCount++
	msg.Offset++
}

// The mockHighWaterMarker implements the highWaterMarker interface
// for testing purposes.
type mockHighWaterMarker struct {
	HighWaterMarkOffsetCount int
}

// HighWaterMarkOffset is required by the highWaterMarker interface.
// We count the number of times this function is called, and return
// that number as its result.
func (m *mockHighWaterMarker) HighWaterMarkOffset() int64 {
	m.HighWaterMarkOffsetCount++
	return int64(m.HighWaterMarkOffsetCount)
}

// PartitionConsumerMock implements the sarama's PartitionConsumer
// interface for testing purposes.  The sarama library already defines
// a mock for it but we can't use it because it doesn't allow a
// partition consumer to be created in such a way that you can push
// messages to it without creating the expectation that
// ConsumePartition will be called.
type PartitionConsumerMock struct {
	MessagesCount int
	ch            chan *sarama.ConsumerMessage
}

// Messages returns a channel of *sarama.ConsumerMessage. In our case
// we close the channel before returning it, as this allows us to test
// handlePartitions without invoking any behaviour of handleMessages
// or indeed hitting an error because of a nil channel.
func (pc *PartitionConsumerMock) Messages() <-chan *sarama.ConsumerMessage {
	pc.MessagesCount++
	if pc.ch == nil {
		pc.ch = make(chan *sarama.ConsumerMessage)
	}
	close(pc.ch)
	return pc.ch
}

// AsyncClose is required by the PartitionConsumer interface.
func (pc *PartitionConsumerMock) AsyncClose() {}

// Close is required by the PartitionConsumer interface.
func (pc *PartitionConsumerMock) Close() error {
	return nil
}

// Errors is required by the PartitionConsumer interface.
func (pc *PartitionConsumerMock) Errors() <-chan *sarama.ConsumerError {
	return nil
}

// HighWaterMarkOffset is required by the PartitionConsumer interface.
func (pc *PartitionConsumerMock) HighWaterMarkOffset() int64 {
	return 0
}

// Topic is required by the PartitionConsumer interface.
func (pc *PartitionConsumerMock) Topic() string {
	return ""
}

// Partition is required by the PartitionConsumer interface.
func (pc *PartitionConsumerMock) Partition() int32 {
	return 0
}

// InitialOffset is required by the PartitionConsumer interface.
func (pc *PartitionConsumerMock) InitialOffset() int64 {
	return 0
}

// MarkOffset is required by the PartitionConsumer interface.
func (pc *PartitionConsumerMock) MarkOffset(offset int64, metadata string) {}

// ResetOffset is required by the PartitionConsumer interface.
func (pc *PartitionConsumerMock) ResetOffset(offset int64, metadata string) {}

// metricsHook implements the MetricsHook interface for testing purposes
type metricsHook struct {
	ReportCount int
	t           *testing.T
	testCase    func(msg message.Message, metadatas map[string]string) (string, func(*testing.T))
}

// Report counts how many times it has been called, and executes any
// testCase that has been added to the metricsHook.  This allows us to
// reuse the metricsHook type every time we want to make assertions
// about the information it has been provided.
func (mmh *metricsHook) Report(msg message.Message, metadatas map[string]string) {
	mmh.ReportCount++
	mmh.t.Run(mmh.testCase(msg, metadatas))
}

// testHandler implements the handler.Handler interface for testing
// purposes. It can be used to make assertions about the message
// passed to HandleMessage.
type testHandler struct {
	t *testing.T
	// A testCase is a function that returns a name to be passed
	// as the first parameter of testing.T.Run and Curryed
	// function that forms a closure over the message.Message
	// provided and can then be passed as the 2nd parameter of
	// testing.T.Run.
	testCase  func(*message.Message) (string, func(t *testing.T))
	CallCount int
}

// HandleMessage will keep a count of how many times it is called and,
// if a testCase is set on the testHandler, it will run it with the
// message that HandleMessage recieved, allowing us to make assertions
// about the nature of that message.
func (h *testHandler) HandleMessage(m *message.Message) error {
	h.CallCount++
	if h.testCase != nil {
		// We use testing.T.Run because we can easily see that
		// the testCase has been run by looking at the verbose
		// output of go test.
		h.t.Run(h.testCase(m))
	}
	return nil
}
