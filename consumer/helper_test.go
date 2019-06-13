package consumer

import (
	"bytes"
	"context"
	"log"
	"regexp"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
)

func newConfig() *Config {
	cfg := NewConfig("some-id")
	return &cfg
}

// consumerGroupClaim implements sarama.ConsumerGroupClaim interface.
type consumerGroupClaim struct {
	ch    chan *sarama.ConsumerMessage
	topic string
}

func (c consumerGroupClaim) Topic() string {
	return c.topic
}

func (consumerGroupClaim) Partition() int32 {
	return int32(0)
}

func (consumerGroupClaim) InitialOffset() int64 {
	return int64(0)
}

func (consumerGroupClaim) HighWaterMarkOffset() int64 {
	return int64(1)
}

func (c consumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage {
	return c.ch
}

// consumerGroupSession implements sarama.ConsumerGroupSession interface.
type consumerGroupSession struct{}

func (consumerGroupSession) Claims() map[string][]int32 {
	return nil
}

func (consumerGroupSession) MemberID() string {
	return ""
}

func (consumerGroupSession) GenerationID() int32 {
	return int32(0)
}

func (consumerGroupSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
}

func (consumerGroupSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
}

func (consumerGroupSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {}

func (consumerGroupSession) Context() context.Context {
	return context.Background()
}

// TestLogger grabs logs in a buffer so we can later make assertions about them.
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

const (
	logRegexPrefix = "\\[Felice\\] [0-9]*/[0-1][0-9]/[0-3][0-9] [0-2][0-9]:[0-5][0-9]:[0-5][0-9] "
)

func (tl *TestLogger) LogLineMatches(match string) {
	content, err := tl.buf.ReadString('\n')
	require.NoError(tl.t, err)
	require.Regexp(tl.t, regexp.MustCompile(logRegexPrefix+match), content)
}

// metricsHook implements the MetricsHook interface for testing purposes
type metricsHook struct {
	ReportCount int
	t           *testing.T
	testCase    func(msg Message, metadatas *Metrics) (string, func(*testing.T))
}

// Report counts how many times it has been called, and executes any
// testCase that has been added to the metricsHook.  This allows us to
// reuse the metricsHook type every time we want to make assertions
// about the information it has been provided.
func (mmh *metricsHook) Report(msg Message, metadatas *Metrics) {
	mmh.ReportCount++
	mmh.t.Run(mmh.testCase(msg, metadatas))
}

// testHandler implements the Handler interface for testing
// purposes. It can be used to make assertions about the message
// passed to HandleMessage.
type testHandler struct {
	t *testing.T
	// testCase optionally holds a function that, given a message,
	// returns a test name and a function to run the test.
	testCase  func(*Message) (string, func(t *testing.T))
	CallCount int
}

// HandleMessage will keep a count of how many times it is called and,
// if a testCase is set on the testHandler, it will run it with the
// message that HandleMessage received, allowing us to make assertions
// about the nature of that message.
func (h *testHandler) HandleMessage(m *Message) error {
	h.CallCount++
	if h.testCase != nil {
		// We use testing.T.Run because we can easily see that
		// the testCase has been run by looking at the verbose
		// output of go test.
		h.t.Run(h.testCase(m))
	}
	return nil
}

type metricsReporterFunc func(m Message, metrics *Metrics)

func (f metricsReporterFunc) Report(m Message, metrics *Metrics) {
	f(m, metrics)
}
