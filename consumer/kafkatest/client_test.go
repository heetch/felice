package kafkatest_test

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"
	qt "github.com/frankban/quicktest"

	"github.com/heetch/felice/consumer/kafkatest"
)

func TestNew(t *testing.T) {
	c := qt.New(t)
	k, err := kafkatest.New()
	c.Assert(err, qt.Equals, nil)

	// Produce a message to a new topic.
	cfg := k.Config()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

	producer, err := sarama.NewSyncProducer(k.Addrs(), cfg)
	c.Assert(err, qt.Equals, nil)
	defer producer.Close()
	topic := k.NewTopic()

	// Check that the topic has actually been created.
	admin, err := sarama.NewClusterAdmin(k.Addrs(), cfg)
	c.Assert(err, qt.Equals, nil)
	defer admin.Close()
	topics, err := admin.ListTopics()
	c.Assert(err, qt.Equals, nil)
	_, ok := topics[topic]
	c.Assert(ok, qt.Equals, true)

	// Produce a message to the topic.
	_, offset, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder("key"),
		Value: sarama.StringEncoder("value"),
	})
	c.Assert(err, qt.Equals, nil)
	c.Assert(offset, qt.Equals, int64(0))

	// Check that we can consume the message we just produced.
	consumer, err := sarama.NewConsumer(k.Addrs(), cfg)
	c.Assert(err, qt.Equals, nil)
	defer consumer.Close()
	pconsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	c.Assert(err, qt.Equals, nil)
	defer pconsumer.Close()
	select {
	case m := <-pconsumer.Messages():
		c.Check(string(m.Key), qt.Equals, "key")
		c.Check(string(m.Value), qt.Equals, "value")
	case <-time.After(10 * time.Second):
		c.Fatal("timed out waiting for message")
	}

	// Close the Kafka instance and check that the
	// new topic has been removed.
	err = k.Close()
	c.Assert(err, qt.Equals, nil)
	topics, err = admin.ListTopics()
	c.Assert(err, qt.Equals, nil)
	_, ok = topics[topic]
	c.Assert(ok, qt.Equals, false)

	// Check we can call Close again.
	err = k.Close()
	c.Assert(err, qt.Equals, nil)
}

func TestDisabled(t *testing.T) {
	c := qt.New(t)
	c.Setenv("KAFKA_DISABLE", "1")
	k, err := kafkatest.New()
	c.Assert(err, qt.Equals, kafkatest.ErrDisabled)
	c.Assert(k, qt.IsNil)

	c.Setenv("KAFKA_DISABLE", "bad")
	k, err = kafkatest.New()
	c.Assert(err, qt.ErrorMatches, `bad value for \$KAFKA_DISABLE: invalid boolean value "bad" \(possible values are: 1, t, T, TRUE, true, True, 0, f, F, FALSE\)`)
	c.Assert(k, qt.IsNil)
}
