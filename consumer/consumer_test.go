package consumer_test

import (
	"bytes"
	"log"
	"regexp"
	"testing"

	"github.com/heetch/felice/common"
	"github.com/heetch/felice/consumer"
	"github.com/heetch/felice/consumer/handler"
	"github.com/heetch/felice/message"
	"github.com/stretchr/testify/require"
)

const (
	logRegexPrefix = "\\[Felice\\] [0-9]*/[0-1][0-9]/[0-3][0-9] [0-2][0-9]:[0-5][0-9]:[0-5][0-9]"
)

// Consumer.Handle emits log messages
func TestHandleLogs(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	l := log.New(buf, "[Felice] ", log.LstdFlags)
	common.Logger = l
	c := &consumer.Consumer{}
	c.Handle("foo", handler.HandlerFunc(func(m *message.Message) error {
		return nil
	}))
	require.Regexp(t, regexp.MustCompile(
		logRegexPrefix+` Registered handler. topic="foo"`), buf.String())

}
