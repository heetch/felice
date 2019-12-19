package consumer_test

import (
	"crypto/rand"
	"fmt"

	"github.com/Shopify/sarama"
	qt "github.com/frankban/quicktest"
	"github.com/heetch/kafkatest"

	"github.com/heetch/felice/consumer"
)

type testKafka struct {
	client   sarama.Client
	producer sarama.SyncProducer
	kt       *kafkatest.Kafka
}

func newTestKafka(c *qt.C) *testKafka {
	kt, err := kafkatest.New()
	c.Assert(err, qt.Equals, nil)
	c.Defer(func() {
		c.Check(kt.Close(), qt.Equals, nil)
	})
	cfg := kt.Config()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.ClientID = randomName("clientid-")
	client, err := sarama.NewClient(kt.Addrs(), cfg)
	c.Assert(err, qt.Equals, nil)
	c.Defer(func() { client.Close() })
	producer, err := sarama.NewSyncProducerFromClient(client)
	c.Assert(err, qt.Equals, nil)
	c.Defer(func() { producer.Close() })
	return &testKafka{
		kt:       kt,
		client:   client,
		producer: producer,
	}
}

// NewConsumer implements testKafka.NewConsumer by returning a consumer
// that uses a real Kafka instance.
func (k *testKafka) NewConsumer() (*consumer.Consumer, error) {
	// Note: if we use the same consumer group name
	// for all consumers, we see sporadic timeout issues,
	// even though that technically shouldn't happen
	// with unrelated topics. *sigh*
	// cfg := consumer.NewConfig("testclient", k.kt.Addrs())
	cfg := consumer.NewConfig(randomName("testclient"), k.kt.Addrs()...)
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	return consumer.New(cfg)
}

func (k *testKafka) NewTopic() string {
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
