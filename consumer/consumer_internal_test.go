package consumer

import (
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"

	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		c, err := New(newConfig())
		require.NoError(t, err)

		require.NotNil(t, c.Logger)
		require.NotNil(t, c.config)
		require.NotNil(t, c.handlers)
		require.NotNil(t, c.quit)
	})

	t.Run("NOK - nil Config", func(t *testing.T) {
		_, err := New(nil)
		require.Error(t, err)
		require.Equal(t, "configuration must be not nil", err.Error())
	})
}

// Consumer.Handle registers a handler for a topic.
func TestHandle(t *testing.T) {
	c, _ := New(newConfig())
	c.Handle("topic", MessageConverterV1(NewConfig("")), &testHandler{})

	res, ok := c.handlers.Get("topic")
	require.True(t, ok)
	require.NotNil(t, res)
}

// ConsumerClaim calls the per-topic Handler for each message that arrives.
func TestConsumerClaim(t *testing.T) {
	tl := NewTestLogger(t)
	topic := "topic"

	sess := consumerGroupSession{}
	claim := consumerGroupClaim{
		ch:    make(chan *sarama.ConsumerMessage, 1),
		topic: topic,
	}

	c, _ := New(newConfig())
	c.Logger = tl.Logger

	handler := &testHandler{
		t: t,
		testCase: func(m *Message) (string, func(t *testing.T)) {
			return topic, func(t *testing.T) {
				require.Equal(t, topic, m.Topic)
				require.EqualValues(t, `"body"`, m.Body.Bytes())
				require.EqualValues(t, "key", m.Key.Bytes())
			}
		},
	}

	c.Handle(topic, MessageConverterV1(NewConfig("")), handler)
	tl.LogLineMatches(`Registered handler. topic="topic"`)

	claim.ch <- &sarama.ConsumerMessage{
		Topic: topic,
		Key:   []byte("key"),
		Value: []byte(`"body"`),
	}
	close(claim.ch)

	groupHandler := consumerGroupHandler{
		consumer: c,
	}
	groupHandler.ConsumeClaim(sess, claim)

	require.Equal(t, 1, handler.CallCount)

	tl.LogLineMatches(`partition reading, topic="topic", partition=0`)
}

// ConsumerClaim will send message data, and some associated
// metadata to a metrics hook function that has been provided to
// the consumer via the Consumer.Metrics field.
func TestConsumerClaimMetricsReporting(t *testing.T) {
	topic := "topic"

	sess := consumerGroupSession{}
	claim := consumerGroupClaim{
		ch:    make(chan *sarama.ConsumerMessage, 1),
		topic: topic,
	}

	mmh := &metricsHook{
		t: t,
		testCase: func(msg Message, meta *Metrics) (string, func(t *testing.T)) {
			return "metrics", func(t *testing.T) {
				require.Equal(t, topic, msg.Topic)
				require.EqualValues(t, `"body"`, msg.Body.Bytes())
				require.EqualValues(t, "key", msg.Key.Bytes())
				require.EqualValues(t, 1, msg.Offset)
				require.EqualValues(t, 1, meta.Attempts)
				require.EqualValues(t, 0, meta.RemainingOffset)
			}
		},
	}
	c, _ := New(newConfig())
	c.Metrics = mmh

	handler := &testHandler{}

	c.Handle(topic, MessageConverterV1(NewConfig("")), handler)

	claim.ch <- &sarama.ConsumerMessage{
		Topic:  topic,
		Key:    []byte("key"),
		Value:  []byte(`"body"`),
		Offset: 1,
	}
	close(claim.ch)

	groupHandler := consumerGroupHandler{
		consumer: c,
	}
	groupHandler.ConsumeClaim(sess, claim)

	require.Equal(t, 1, mmh.ReportCount)
}

func TestConsumerClaimRetryOnError(t *testing.T) {
	topic := "topic"

	sess := consumerGroupSession{}
	claim := consumerGroupClaim{
		ch:    make(chan *sarama.ConsumerMessage, 1),
		topic: topic,
	}

	metricsCh := make(chan *Metrics)
	reportMetric := func(m Message, metrics *Metrics) {
		metricsCh <- metrics
	}

	c, _ := New(newConfig())
	c.Metrics = metricsReporterFunc(reportMetric)

	type msgHandleReq struct {
		m     *Message
		reply chan error
	}
	msgHandleCh := make(chan msgHandleReq)
	handle := func(m *Message) error {
		reply := make(chan error)
		msgHandleCh <- msgHandleReq{m, reply}
		return <-reply
	}
	c.Handle(topic, MessageConverterV1(NewConfig("")), HandlerFunc(handle))

	handleMessagesDone := make(chan struct{})

	groupHandler := consumerGroupHandler{
		consumer: c,
	}
	go func() {
		defer close(handleMessagesDone)
		groupHandler.ConsumeClaim(sess, claim)
	}()

	// Send a message to the channel; we should exponentially
	// backoff until the message is handled OK. As the backoff is jittered
	// (and controlled by the specific retry strategy chosen) we don't
	// check the specific delays.
	claim.ch <- &sarama.ConsumerMessage{
		Topic:  topic,
		Key:    []byte("key"),
		Value:  []byte(`"body0"`),
		Offset: 1,
	}
	for i := 0; i < 5; i++ {
		select {
		case req := <-msgHandleCh:
			require.Equal(t, []byte("key"), req.m.Key.Bytes())
			require.Equal(t, []byte(`"body0"`), req.m.Body.Bytes())
			req.reply <- fmt.Errorf("some error")
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for handle request")
		}
	}
	// Allow the handler to succeed.
	select {
	case req := <-msgHandleCh:
		require.Equal(t, []byte("key"), req.m.Key.Bytes())
		require.Equal(t, []byte(`"body0"`), req.m.Body.Bytes())
		req.reply <- nil
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for handle request")
	}

	// We should receive a metrics report that
	// the message was sent correctly.
	select {
	case m := <-metricsCh:
		require.Equal(t, m.Attempts, 6)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for handle request")
	}

	// When the channel is closed, the handler should terminate.
	close(claim.ch)
	select {
	case <-handleMessagesDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for handler loop to finish")
	}
}

// Consumer.convertMessage converts a sarama.ConsumerMessage into our
// own Message type.
func TestMessageConverterV1(t *testing.T) {
	now := time.Now()
	sm := sarama.ConsumerMessage{
		Topic:     "topic",
		Key:       []byte("key"),
		Value:     []byte("body"),
		Timestamp: now,
		Offset:    10,
		Partition: 10,
		Headers: []*sarama.RecordHeader{
			&sarama.RecordHeader{Key: []byte("k"), Value: []byte("v")},
			&sarama.RecordHeader{Key: []byte("Message-Id"), Value: []byte("some-id")},
		},
	}

	msg, err := MessageConverterV1(NewConfig("some-id")).FromKafka(&sm)
	require.NoError(t, err)
	require.Equal(t, sm.Topic, msg.Topic)
	require.EqualValues(t, sm.Key, msg.Key.Bytes())
	require.EqualValues(t, sm.Value, msg.Body.Bytes())
	require.Equal(t, sm.Timestamp, msg.ProducedAt)
	require.Equal(t, sm.Offset, msg.Offset)
	require.Equal(t, sm.Partition, msg.Partition)
	require.Equal(t, "some-id", msg.ID)
	require.Equal(t, "v", msg.Headers["k"])
}

// Test that Consumer.Handle emits log messages
func TestHandleLogs(t *testing.T) {
	c, _ := New(newConfig())

	tl := NewTestLogger(t)
	c.Logger = tl.Logger

	c.Handle("foo", MessageConverterV1(NewConfig("")), HandlerFunc(func(m *Message) error {
		return nil
	}))
	tl.LogLineMatches(`Registered handler. topic="foo"`)
}
