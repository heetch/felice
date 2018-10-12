package producer_test

import (
	"fmt"
	"testing"

	"github.com/Shopify/sarama/mocks"
	"github.com/heetch/felice/message"
	"github.com/heetch/felice/producer"
	"github.com/stretchr/testify/require"
)

func TestSendMessage(t *testing.T) {
	msp := mocks.NewSyncProducer(t, nil)
	p := producer.Producer{SyncProducer: msp}

	msg, err := message.New("topic", "message")
	require.NoError(t, err)

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
	require.EqualError(t, err, "failed to send message: cannot produce message")
}

func TestSend(t *testing.T) {
	msp := mocks.NewSyncProducer(t, nil)
	p := producer.Producer{SyncProducer: msp}

	msp.ExpectSendMessageAndSucceed()
	_, err := p.Send("topic", "message")
	require.NoError(t, err)

	msp.ExpectSendMessageAndFail(fmt.Errorf("cannot produce message"))
	_, err = p.Send("topic", "message")
	require.EqualError(t, err, "failed to send message: cannot produce message")
}
