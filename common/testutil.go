package common

import (
	"bytes"
	"log"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	logRegexPrefix = "\\[Felice\\] [0-9]*/[0-1][0-9]/[0-3][0-9] [0-2][0-9]:[0-5][0-9]:[0-5][0-9] "
)

type TestLogger struct {
	buf       *bytes.Buffer
	oldLogger StdLogger
	t         *testing.T
}

// NewTestLogger constructs a test logger we can make assertions against
func NewTestLogger(t *testing.T) *TestLogger {
	tl := &TestLogger{
		buf:       bytes.NewBuffer([]byte{}),
		oldLogger: Logger,
		t:         t,
	}
	l := log.New(tl.buf, "[Felice] ", log.LstdFlags)
	Logger = l

	return tl
}

// TearDown sets the common logger back to its previous state
func (tl *TestLogger) TearDown() {
	Logger = tl.oldLogger
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
