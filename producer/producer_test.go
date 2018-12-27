package producer_test

import (
	"fmt"
	"testing"

	"github.com/Shopify/sarama/mocks"
	"github.com/heetch/felice/producer"
	"github.com/stretchr/testify/require"
)

func TestSendMessage(t *testing.T) {
	cfg := producer.NewConfig("id", producer.MessageFormatterV1())
	msp := mocks.NewSyncProducer(t, &cfg.Config)

	p, err := producer.NewFrom(msp, cfg)
	require.NoError(t, err)

	msg := producer.NewMessage("topic", "message")

	msp.ExpectSendMessageWithCheckerFunctionAndSucceed(func(val []byte) error {
		exp := "\"message\""
		if string(val) != exp {
			return fmt.Errorf("expected: %s but got: %s", exp, val)
		}
		return nil
	})
	err = p.SendMessage(msg)
	require.NoError(t, err)

	msp.ExpectSendMessageAndFail(fmt.Errorf("cannot produce message"))
	err = p.SendMessage(msg)
	require.EqualError(t, err, "producer: failed to send message: cannot produce message")
}
