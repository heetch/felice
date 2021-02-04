package consumer_test

import (
	"crypto/rand"
	"fmt"
	"os"

	"github.com/Shopify/sarama"
	qt "github.com/frankban/quicktest"

	"github.com/heetch/felice/consumer"
)

type testKafka struct {
	addr       string
	client     sarama.Client
	controller *sarama.Broker
	producer   sarama.SyncProducer
	topics     []string
}

func newTestKafka(c *qt.C, addr string) *testKafka {
	if os.Getenv("KAFKA_DISABLE") != "" {
		c.Skipf("skipping integration tests")
	}

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V1_0_0_0
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.ClientID = randomName("clientid-")
	client, err := sarama.NewClient([]string{addr}, cfg)
	c.Assert(err, qt.Equals, nil)
	controller, err := client.Controller()
	c.Assert(err, qt.Equals, nil)
	producer, err := sarama.NewSyncProducerFromClient(client)
	c.Assert(err, qt.Equals, nil)
	k := &testKafka{
		addr:       addr,
		client:     client,
		controller: controller,
		producer:   producer,
	}
	c.Defer(k.Close)
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
	cfg := consumer.NewConfig(randomName("testclient"), k.addr)
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	return consumer.New(cfg)
}

func (k *testKafka) NewTopic(prefix string) (string, error) {
	topic := randomName(prefix)
	if _, err := k.controller.CreateTopics(&sarama.CreateTopicsRequest{
		Version: 0,
		TopicDetails: map[string]*sarama.TopicDetail{
			topic: {
				NumPartitions:     1,
				ReplicationFactor: 0,
				ReplicaAssignment: map[int32][]int32{},
			},
		},
	}); err != nil {
		return "", fmt.Errorf("cannot create topic %q: %v", topic, err)
	}
	k.topics = append(k.topics, topic)
	return topic, nil
}

func (k *testKafka) Close() {
	_, err := k.controller.DeleteTopics(&sarama.DeleteTopicsRequest{
		Version: 0,
		Topics:  k.topics,
	})
	if err != nil {
		panic(fmt.Errorf("cannot delete topic: %v", err))
	}
	k.controller.Close()
	k.producer.Close()
	k.client.Close()
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
