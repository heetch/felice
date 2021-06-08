package consumer_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	qt "github.com/frankban/quicktest"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/heetch/felice/v2/codec"
	"github.com/heetch/felice/v2/consumer"
)

func TestSimpleHandler(t *testing.T) {
	c := qt.New(t)

	c.Parallel()

	k := newTestKafka(c)
	topic := k.NewTopic("testtopic")

	t0 := time.Now()
	err := k.Produce(&sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.StringEncoder("a"),
		Value:     sarama.StringEncoder(`{"x":1}`),
		Timestamp: t0,
		Headers: []sarama.RecordHeader{{
			Key:   []byte("header1"),
			Value: []byte("value1"),
		}, {
			Key:   []byte("header2"),
			Value: []byte("value2"),
		}},
	})
	c.Assert(err, qt.IsNil)

	cs, err := k.NewConsumer()
	c.Assert(err, qt.Equals, nil)
	defer func() {
		// On success, this will be the second time cs.Close is called,
		// but that should be fine because there's a specific check for that.
		c.Check(cs.Close(), qt.Equals, nil)
	}()
	hc := make(chan handleReq)
	cs.Handle(topic, consumer.MessageConverterV1(nil), handlerFunc(func(ctx context.Context, m *consumer.Message) error {
		rc := make(chan error)
		hc <- handleReq{m, rc}
		return <-rc
	}))
	serveDone := make(chan error)
	go func() {
		err := cs.Serve(context.Background())
		c.Logf("consumer_test Serve finished (error %v)", err)
		serveDone <- err
	}()
	select {
	case req := <-hc:
		c.Assert(req.m, deepEquals, &consumer.Message{
			Topic:      topic,
			Body:       codec.JSONDecoder([]byte(`{"x":1}`)),
			Key:        codec.StringDecoder([]byte("a")),
			ProducedAt: t0,
			Partition:  0,
			Offset:     0,
			Headers: map[string]string{
				"header1": "value1",
				"header2": "value2",
			},
		})
		c.Check(req.m.HighWaterMarkOffset(), qt.Equals, int64(1))
		req.reply <- nil
	case <-time.After(15 * time.Second):
		// For some reason, it's not uncommon for the first message
		// to be delivered 10 seconds after starting (the JoinGroup
		// call can be very slow), hence the long wait time.
		c.Errorf("timed out waiting for HandleMessage call")
	}
	c.Check(cs.Close(), qt.Equals, nil)
	select {
	case err := <-serveDone:
		c.Assert(err, qt.Equals, nil)
	case <-time.After(5 * time.Second):
		c.Fatalf("timed out waiting for Serve to return")
	}
}

func TestCloseBeforeServe(t *testing.T) {
	c := qt.New(t)

	c.Parallel()
	cfg := consumer.NewConfig("clientid", "0.1.2.3:1234")
	cs, err := consumer.New(cfg)
	c.Assert(err, qt.Equals, nil)
	err = cs.Close()
	c.Assert(err, qt.Equals, nil)
}

func TestDiscardedCalledOnHandlerError(t *testing.T) {
	c := qt.New(t)

	c.Parallel()

	k := newTestKafka(c)
	c.Run("Mark as committed", func(c *qt.C) {
		topic := k.NewTopic("testtopic")

		t0 := time.Now()
		err := k.Produce(&sarama.ProducerMessage{
			Topic:     topic,
			Key:       sarama.StringEncoder("a"),
			Value:     sarama.StringEncoder(`1`),
			Timestamp: t0,
		})
		c.Assert(err, qt.IsNil)

		err = k.Produce(&sarama.ProducerMessage{
			Topic:     topic,
			Key:       sarama.StringEncoder("a"),
			Value:     sarama.StringEncoder(`2`),
			Timestamp: t0,
		})
		c.Assert(err, qt.IsNil)

		discarded := make(chan discardedCall)
		cfg := consumer.NewConfig("testclient", k.kt.Addrs()...)
		cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
		cfg.Discarded = func(ctx context.Context, m *sarama.ConsumerMessage, err error) bool {
			discarded <- discardedCall{ctx: ctx, msg: m, err: err}
			return true
		}
		notDiscarded := make(chan string)

		cs, err := consumer.New(cfg)
		c.Assert(err, qt.Equals, nil)
		cs.Handle(topic, consumer.MessageConverterV1(nil), handlerFunc(func(ctx context.Context, m *consumer.Message) error {
			val := string(m.Body.Bytes())
			if val == "1" {
				// When we return this error, the Discarded method should be called
				// and the message should be marked as consumed.
				return fmt.Errorf("some handler error")
			}
			select {
			case notDiscarded <- val:
			case <-ctx.Done():
				c.Errorf("error trying to send on notDiscarded: %v", ctx.Err())
			}
			return nil
		}))
		serveDone := make(chan error)
		go func() {
			serveDone <- cs.Serve(context.Background())
		}()
		defer func() {
			c.Check(cs.Close(), qt.Equals, nil)
			c.Check(<-serveDone, qt.Equals, nil)
		}()
		// The first message should be sent by the Discarded function.
		select {
		case call := <-discarded:
			c.Check(call.ctx, qt.Not(qt.IsNil))
			c.Check(string(call.msg.Value), qt.Equals, "1")
			c.Check(call.msg.Offset, qt.Equals, int64(0))
			c.Check(call.err, qt.ErrorMatches, "some handler error")
		case v := <-notDiscarded:
			c.Fatalf("unexpected non-error message %q (expecting call to Discarded)", v)
		case <-time.After(15 * time.Second):
			c.Fatalf("timed out waiting for first Discarded call")
		}
		// The second message should be handled normally.
		select {
		case call := <-discarded:
			c.Errorf("unexpected call to discarded %q (message received twice?)", call.msg.Value)
		case v := <-notDiscarded:
			c.Check(v, qt.Equals, "2")
		case <-time.After(15 * time.Second):
			c.Fatalf("timed out waiting for second HandleMessage call")
		}
	})

	c.Run("Mark as not committed", func(c *qt.C) {
		topic := k.NewTopic("testtopic1")
		err := k.Produce(&sarama.ProducerMessage{
			Topic:     topic,
			Key:       sarama.StringEncoder("msgkey"),
			Value:     sarama.StringEncoder("value"),
			Timestamp: time.Now(),
		})
		c.Assert(err, qt.IsNil)

		discarded := make(chan discardedCall)

		cfg := consumer.NewConfig("testclient1", k.kt.Addrs()...)
		cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
		cfg.Discarded = func(ctx context.Context, m *sarama.ConsumerMessage, err error) bool {
			discarded <- discardedCall{ctx: ctx, msg: m, err: err}
			// Don't mark as committed
			return false
		}

		cs, err := consumer.New(cfg)
		c.Assert(err, qt.IsNil)
		cs.Handle(topic, consumer.MessageConverterV1(nil), handlerFunc(func(ctx context.Context, m *consumer.Message) error {
			c.Check(string(m.Body.Bytes()), qt.Equals, "value")
			c.Check(string(m.Key.Bytes()), qt.Equals, "msgkey")
			return errors.New("here I am")
		}))

		serveDone := make(chan error)
		go func() {
			serveDone <- cs.Serve(context.Background())
		}()

		select {
		case call := <-discarded:
			c.Check(call.ctx, qt.Not(qt.IsNil))
			c.Check(string(call.msg.Value), qt.Equals, "value")
			c.Check(call.msg.Offset, qt.Equals, int64(0))
			c.Check(call.err, qt.ErrorMatches, "here I am")
		case <-time.After(15 * time.Second):
			c.Fatalf("timed out waiting for first Discarded call")
		}

		c.Assert(cs.Close(), qt.IsNil)
		c.Assert(<-serveDone, qt.IsNil)

		// Check no offset were marked
		adminClient, err := sarama.NewClusterAdmin(k.kt.Addrs(), cfg.Config)
		c.Assert(err, qt.IsNil)

		resp, err := adminClient.ListConsumerGroupOffsets("testclient1", map[string][]int32{
			topic: []int32{0},
		})
		c.Assert(err, qt.IsNil)

		c.Assert(resp.GetBlock(topic, 0).Offset, qt.Equals, sarama.OffsetNewest,
			qt.Commentf("message should not be marked"))

	})
}

func TestServeReturnsOnClose(t *testing.T) {
	c := qt.New(t)

	c.Parallel()

	k := newTestKafka(c)
	topic := k.NewTopic("testtopic")

	cs, err := k.NewConsumer()
	c.Assert(err, qt.Equals, nil)
	defer func() {
		// On success, this will be the second time cs.Close is called,
		// but that should be fine because there's a specific check for that.
		c.Check(cs.Close(), qt.Equals, nil)
	}()
	cs.Handle(topic, consumer.MessageConverterV1(nil), handlerFunc(func(ctx context.Context, m *consumer.Message) error {
		return nil
	}))
	serveDone := make(chan error)
	go func() {
		serveDone <- cs.Serve(context.Background())
	}()
	// Wait for Serve to be called.
	time.Sleep(100 * time.Millisecond)
	c.Check(cs.Close(), qt.Equals, nil)
	select {
	case err := <-serveDone:
		c.Assert(err, qt.Equals, nil)
	case <-time.After(5 * time.Second):
		c.Fatalf("timed out waiting for Serve to return")
	}
}

func TestHandlerCanceledOnClose(t *testing.T) {
	c := qt.New(t)

	c.Parallel()

	k := newTestKafka(c)
	topic := k.NewTopic("testtopic")

	t0 := time.Now()
	err := k.Produce(&sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.StringEncoder("a"),
		Value:     sarama.StringEncoder(`{"x":1}`),
		Timestamp: t0,
	})
	c.Assert(err, qt.IsNil)

	cs, err := k.NewConsumer()
	c.Assert(err, qt.Equals, nil)
	defer func() {
		// On success, this will be the second time cs.Close is called,
		// but that should be fine because there's a specific check for that.
		c.Check(cs.Close(), qt.Equals, nil)
	}()
	handlerProgress := make(chan struct{}, 1)
	cs.Handle(topic, consumer.MessageConverterV1(nil), handlerFunc(func(ctx context.Context, m *consumer.Message) error {
		handlerProgress <- struct{}{}
		<-ctx.Done()
		handlerProgress <- struct{}{}
		return nil
	}))
	serveDone := make(chan error)
	go func() {
		serveDone <- cs.Serve(context.Background())
	}()
	select {
	case <-handlerProgress:
	case <-time.After(15 * time.Second):
		// For some reason, it's not uncommon for the first message
		// to be delivered 10 seconds after starting (the JoinGroup
		// call can be very slow), hence the long wait time.
		c.Errorf("timed out waiting for HandleMessage call")
	}

	// Closing the consumer should cause the handler context to be
	// cancelled and thus the handler to complete.
	c.Check(cs.Close(), qt.Equals, nil)
	select {
	case err := <-serveDone:
		c.Assert(err, qt.Equals, nil)
	case <-time.After(5 * time.Second):
		c.Fatalf("timed out waiting for Serve to return")
	}
	select {
	case <-handlerProgress:
	case <-time.After(1 * time.Second):
		c.Errorf("timed out waiting for handler to complete call")
	}
}

type handleReq struct {
	m     *consumer.Message
	reply chan error
}

type discardedCall struct {
	ctx context.Context
	msg *sarama.ConsumerMessage
	err error
}

// handlerFunc implements Handle by calling the underlying function.
type handlerFunc func(context.Context, *consumer.Message) error

// HandleMessage implements Handler.HandleMessage by calling h.
func (f handlerFunc) HandleMessage(ctx context.Context, m *consumer.Message) error {
	return f(ctx, m)
}

// deepEquals is a comparer that's suitable for comparison
// of consumer.Message
var deepEquals = qt.CmpEquals(
	cmpopts.IgnoreUnexported(consumer.Message{}),
	equateApproxTime(time.Second),
	cmp.Comparer(decoderEqual),
	// We'd like to do a bit better than this, but it's a little bit
	// awkward.
	cmpopts.IgnoreFields(consumer.Message{}, "ID"),
)

// equateApproxTime returns a Comparer options that
// determine two time.Time values to be equal if they
// are within the given time interval of one another.
//
// The zero time is treated specially: it is only considered
// equal to another zero time value.
func equateApproxTime(margin time.Duration) cmp.Option {
	return cmp.Comparer(func(t0, t1 time.Time) bool {
		if t0.IsZero() && t1.IsZero() {
			return true
		}
		if t0.IsZero() || t1.IsZero() {
			return false
		}
		diff := t0.Sub(t1)
		if diff < 0 {
			diff = -diff
		}
		return diff <= margin
	})
}

func decoderEqual(a, b codec.Decoder) (eq bool) {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return reflect.TypeOf(a) == reflect.TypeOf(b) && bytes.Equal(a.Bytes(), b.Bytes())
}
