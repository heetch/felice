package consumer_test

import (
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/Shopify/sarama"
	qt "github.com/frankban/quicktest"
	"github.com/heetch/kafkatest"

	"github.com/heetch/felice/v2/consumer"
)

type testKafka struct {
	client   sarama.Client
	producer sarama.SyncProducer
	kt       *kafkatest.Kafka
}

func newTestKafka(c *qt.C) *testKafka {
	kt, err := kafkatest.New()
	if errors.Is(err, kafkatest.ErrDisabled) {
		c.Skipf("skipping integration tests")
	}
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(kt.Close(), qt.IsNil)
	})

	cfg := kt.Config()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.ClientID = randomName("clientid-")
	client, err := sarama.NewClient(kt.Addrs(), cfg)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(client.Close(), qt.IsNil) })

	producer, err := sarama.NewSyncProducerFromClient(client)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(producer.Close(), qt.IsNil) })
	k := &testKafka{
		kt:       kt,
		client:   client,
		producer: producer,
	}
	return k
}

// NewConsumer implements testKafka.NewConsumer by returning a consumer
// that uses a real Kafka instance.
func (k *testKafka) NewConsumer() (*consumer.Consumer, error) {
	// Note: if we use the same consumer group name
	// for all consumers, we see sporadic timeout issues,
	// even though that technically shouldn't happen
	// with unrelated topics. *sigh*
	// cfg := consumer.NewConfig("testclient", k.addr)
	cfg := consumer.NewConfig(randomName("testclient"), k.kt.Addrs()...)
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	return consumer.New(cfg)
}

func (k *testKafka) NewTopic(prefix string) string {
	return k.kt.NewTopic()
}

func (k *testKafka) Produce(m *sarama.ProducerMessage) error {
	_, _, err := k.producer.SendMessage(m)
	return err
}

func randomName(prefix string) string {
	buf := make([]byte, 6)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%s-%x", prefix, buf)
}
