package producer_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/stretchr/testify/require"

	"github.com/heetch/felice/v2/producer"
)

func TestSendMessage(t *testing.T) {
	msgs := []*producer.Message{
		producer.NewMessage("topic", "message"),
		&producer.Message{Topic: "topic", Body: "message"},
	}

	for _, msg := range msgs {
		cfg := producer.NewConfig("id", producer.MessageConverterV1())
		msp := mocks.NewSyncProducer(t, &cfg.Config)

		p, err := producer.NewFrom(msp, cfg)
		require.NoError(t, err)

		msp.ExpectSendMessageWithCheckerFunctionAndSucceed(func(val []byte) error {
			exp := "\"message\""
			if string(val) != exp {
				return fmt.Errorf("expected: %s but got: %s", exp, val)
			}
			return nil
		})
		err = p.SendMessage(context.Background(), msg)
		require.NoError(t, err)

		msp.ExpectSendMessageAndFail(fmt.Errorf("cannot produce message"))
		err = p.SendMessage(context.Background(), msg)
		require.EqualError(t, err, "producer: failed to send message: cannot produce message")
	}
}

func TestSendMessages(t *testing.T) {
	msgs := []*producer.Message{
		producer.NewMessage("topic1", "message1"),
		&producer.Message{Topic: "topic2", Body: "message2"},
		&producer.Message{Topic: "topic2", Body: "message3"},
	}
	cfg := producer.NewConfig("id", producer.MessageConverterV1())
	msp := mocks.NewSyncProducer(t, &cfg.Config)
	p, err := producer.NewFrom(msp, cfg)
	require.NoError(t, err)

	for i, msg := range msgs {
		i, msg := i, msg
		msp.ExpectSendMessageWithCheckerFunctionAndSucceed(func(val []byte) error {
			if got, want := string(val), fmt.Sprintf("%q", msg.Body); got != want {
				return fmt.Errorf("unexpected message %d; got %q want %q", i, got, want)
			}
			return nil
		})
	}
	err = p.SendMessages(context.Background(), msgs)
	require.NoError(t, err)

	// Unfortunately the Sarama mock doesn't fill out of any of the Offset, Partition
	// or Timestamp fields when SendMessages is called, so we can't test
	// that functionality here.
}

func TestSendMessagesError(t *testing.T) {
	msgs := []*producer.Message{
		producer.NewMessage("topic1", "message1"),
		&producer.Message{Topic: "topic2", Body: "message2"},
		&producer.Message{Topic: "topic2", Body: "message3"},
	}
	perr1 := fmt.Errorf("first error")
	perr2 := fmt.Errorf("second error")
	cfg := producer.NewConfig("id", producer.MessageConverterV1())
	sendMessages := func(smsgs []*sarama.ProducerMessage) error {
		return sarama.ProducerErrors{{
			Msg: smsgs[0],
			Err: perr1,
		}, {
			Msg: smsgs[2],
			Err: perr2,
		}}
	}
	p, err := producer.NewFrom(sendMessagesFunc{f: sendMessages}, cfg)
	require.NoError(t, err)
	err = p.SendMessages(context.Background(), msgs)
	sendErrs, ok := err.(producer.SendMessagesErrors)
	require.True(t, ok)
	require.Len(t, sendErrs, 2)

	if sendErrs[0].Msg != msgs[0] {
		t.Errorf("unexpected message at 0: %#v", sendErrs[0].Msg)
	}
	if sendErrs[0].Err != perr1 {
		t.Errorf("unexpected error at 0: %#v", sendErrs[0].Err)
	}
	if sendErrs[1].Msg != msgs[2] {
		t.Errorf("unexpected message at 0: %#v", sendErrs[0].Msg)
	}
	if sendErrs[1].Err != perr2 {
		t.Errorf("unexpected error at 0: %#v", sendErrs[0].Err)
	}
}

func TestSend(t *testing.T) {
	msp := mocks.NewSyncProducer(t, nil)
	cfg := producer.NewConfig("id", producer.MessageConverterV1())
	p, err := producer.NewFrom(msp, cfg)
	require.NoError(t, err)

	msp.ExpectSendMessageAndSucceed()
	msg, err := p.Send(context.Background(), "topic", "message", producer.Int64Key(10), producer.Header("k", "v"))
	require.NoError(t, err)
	key, err := msg.Key.Encode()
	require.NoError(t, err)

	require.EqualValues(t, "10", key)
	require.Equal(t, "v", msg.Headers["k"])

	msp.ExpectSendMessageAndFail(fmt.Errorf("cannot produce message"))
	_, err = p.Send(context.Background(), "topic", "message")
	require.EqualError(t, err, "producer: failed to send message: cannot produce message")
}

type sendMessagesFunc struct {
	sarama.SyncProducer
	f func(msgs []*sarama.ProducerMessage) error
}

func (f sendMessagesFunc) SendMessages(msgs []*sarama.ProducerMessage) error {
	return f.f(msgs)
}
