package consumer

import (
	"bytes"
	"fmt"
	"log"
	"regexp"
	"testing"
	"time"

	"github.com/Shopify/sarama"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/stretchr/testify/require"

	"github.com/heetch/felice/consumer/handler"
	"github.com/heetch/felice/message"
)

const (
	logRegexPrefix = "\\[Felice\\] [0-9]*/[0-1][0-9]/[0-3][0-9] [0-2][0-9]:[0-5][0-9]:[0-5][0-9] "
)

// TestLogger grabs logs in a buffer so we can later make assertions
// about them.
type TestLogger struct {
	buf    bytes.Buffer
	Logger *log.Logger
	t      *testing.T
}

// NewTestLogger constructs a test logger we can make assertions against
func NewTestLogger(t *testing.T) *TestLogger {
	tl := &TestLogger{
		t: t,
	}
	tl.Logger = log.New(&tl.buf, "[Felice] ", log.LstdFlags)
	return tl
}

// Skip will jump over a log line we don't care about.  If there's an
// error reading from the buffer the test will fail.
func (tl *TestLogger) SkipLogLine(reason string) {
	line, err := tl.buf.ReadString('\n')
	tl.t.Logf(line)
	require.NoError(tl.t, err)
	tl.t.Logf("Skipping log line: %s", reason)
}

//
func (tl *TestLogger) LogLineMatches(match string) {
	content, err := tl.buf.ReadString('\n')
	require.NoError(tl.t, err)
	require.Regexp(tl.t, regexp.MustCompile(logRegexPrefix+match), content)
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
}

// Consumer.handlePartitions exits when we close the channel of PartitionConsumers
func TestConsumerHandlePartitionsOnClosedChannel(t *testing.T) {
	tl := NewTestLogger(t)
	c := Consumer{Logger: tl.Logger}
	ch := make(chan cluster.PartitionConsumer)

	close(ch)
	err := c.handlePartitions(ch)
	expected := "partition consumer channel closed"
	require.EqualError(t, err, expected)
}

// Consumer.handlePartitions exits when we send something on the Quit channel
func TestConsumerHandlePartitionsWithQuit(t *testing.T) {
	tl := NewTestLogger(t)
	c := Consumer{Logger: tl.Logger}
	ch := make(chan cluster.PartitionConsumer)
	c.quit = make(chan struct{}, 1)

	c.quit <- struct{}{}
	err := c.handlePartitions(ch)
	require.NoError(t, err)
	tl.LogLineMatches("partition handler terminating")
}

// Consumer.handlePartitions provides a channel of messages, from each
// PartitionConsumer, to the handleMessages function.
func TestConsumerHandlePartitions(t *testing.T) {
	tl := NewTestLogger(t)
	c := Consumer{Logger: tl.Logger}
	ch := make(chan cluster.PartitionConsumer, 1)

	pcm := &PartitionConsumerMock{}
	ch <- pcm
	close(ch)
	err := c.handlePartitions(ch)
	expected := "partition consumer channel closed"
	require.EqualError(t, err, expected)
	c.wg.Wait()
	require.Equal(t, 1, pcm.MessagesCount)
}

// Consumer.handleMessages calls the per-topic Handler for each
// message that arrives.
func TestConsumerHandleMessages(t *testing.T) {
	tl := NewTestLogger(t)

	c := Consumer{Logger: tl.Logger}
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
	tl.LogLineMatches(`Registered handler. topic="topic"`)

	ch := make(chan *sarama.ConsumerMessage, 1)
	ch <- &sarama.ConsumerMessage{
		Topic: "topic",
		Key:   []byte("key"),
		Value: []byte("body"),
	}
	close(ch)

	hwm := &mockHighWaterMarker{}
	mos := &mockOffsetStash{}
	c.handleMessages(ch, mos, hwm, "topic", 1)

	require.Equal(t, 1, handler.CallCount)

	tl.LogLineMatches(`partition messages - reading, topic="topic", partition=1`)
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
	c.handleMessages(ch, mos, hwm, "topic", 1)

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
// message that HandleMessage received, allowing us to make assertions
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

// Serve emits logs when it cannot create a new consumer
func TestServeLogsErrorFromNewConsumer(t *testing.T) {
	tl := NewTestLogger(t)
	c := &Consumer{Logger: tl.Logger}
	c.newConsumer = func(addrs []string, groupID string, topics []string, config *cluster.Config) (clusterConsumer, error) {
		return nil, fmt.Errorf("oh noes! it doesn't work!")
	}
	c.Handle("foo", handler.HandlerFunc(func(m *message.Message) error {
		return nil
	}))
	err := c.Serve(NewConfig("some-id"), "foo")
	require.Error(t, err)
}

// checks if the consumer validates the configuration correctly.
func TestValidateConfig(t *testing.T) {
	// default configuration must work with no issue and no logs should be outputted.
	t.Run("Default config", func(t *testing.T) {
		var buf bytes.Buffer
		var c Consumer
		c.Logger = log.New(&buf, "", 0)
		c.setup()

		cfg := NewConfig("some id")
		c.config = &cfg
		err := c.validateConfig()
		require.NoError(t, err)
		require.Zero(t, buf.Len())
	})

	// changing the cluster group mode should provoke a log and an override of the value
	// with the correct one.
	t.Run("Different Group mode", func(t *testing.T) {
		var buf bytes.Buffer
		var c Consumer
		c.Logger = log.New(&buf, "", 0)
		c.setup()

		cfg := NewConfig("some id")
		c.config = &cfg
		c.config.Group.Mode = cluster.ConsumerModeMultiplex
		err := c.validateConfig()
		require.NoError(t, err)
		require.NotZero(t, buf.Len())
		require.Equal(t, cluster.ConsumerModePartitions, c.config.Group.Mode)
	})
}
