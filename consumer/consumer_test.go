package consumer_test

import (
	"bytes"
	"fmt"
	"log"
	"regexp"
	"testing"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/heetch/felice/common"
	"github.com/heetch/felice/consumer"
	"github.com/heetch/felice/consumer/handler"
	"github.com/heetch/felice/message"
	"github.com/stretchr/testify/require"
)

const (
	logRegexPrefix = "\\[Felice\\] [0-9]*/[0-1][0-9]/[0-3][0-9] [0-2][0-9]:[0-5][0-9]:[0-5][0-9] "
)

type TestLogger struct {
	buf       *bytes.Buffer
	oldLogger common.StdLogger
	t         *testing.T
}

// NewTestLogger constructs a test logger we can make assertions against
func NewTestLogger(t *testing.T) *TestLogger {
	tl := &TestLogger{
		buf:       bytes.NewBuffer([]byte{}),
		oldLogger: common.Logger,
		t:         t,
	}
	l := log.New(tl.buf, "[Felice] ", log.LstdFlags)
	common.Logger = l

	return tl
}

// TearDown sets the common logger back to its previous state
func (tl *TestLogger) TearDown() {
	common.Logger = tl.oldLogger
}

// Skip will jump over a log line we don't care about.  If there's an
// error reading from the buffer the test will fail.
func (tl *TestLogger) SkipLogLine(reason string) {
	_, err := tl.buf.ReadString('\n')
	require.NoError(tl.t, err)
	tl.t.Logf("Skipping log line: %s", reason)
}

//
func (tl *TestLogger) LogLineMatches(match string) {
	content, err := tl.buf.ReadString('\n')
	require.NoError(tl.t, err)
	require.Regexp(tl.t, regexp.MustCompile(logRegexPrefix+match), content)
}

var nilHandler = handler.HandlerFunc(func(m *message.Message) error {
	return nil
})

// Consumer.Handle emits log messages
func TestHandleLogs(t *testing.T) {
	tl := NewTestLogger(t)
	defer tl.TearDown()
	c := &consumer.Consumer{}
	c.Handle("foo", nilHandler)
	tl.LogLineMatches(`Registered handler. topic="foo"`)
}

// Serve emits logs when it cannot create a new consumer
func TestServeLogsErrorFromNewConsumer(t *testing.T) {
	tl := NewTestLogger(t)
	defer tl.TearDown()
	c := &consumer.Consumer{}
	c.NewConsumer = func(addrs []string, groupID string, topics []string, config *cluster.Config) (*cluster.Consumer, error) {
		return nil, fmt.Errorf("oh noes! it doesn't work!")
	}
	c.Handle("foo", nilHandler)
	err := c.Serve("foo")
	require.Error(t, err)
	tl.SkipLogLine("registering handler")
	tl.LogLineMatches(`failed to create a consumer for topics \[foo\] in consumer group "foo-consumer-group": oh noes! it doesn't work!`)
}
