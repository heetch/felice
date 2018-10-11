package producer

import (
	"github.com/Shopify/sarama"
	"github.com/heetch/felice/message"
	"github.com/pkg/errors"
)

// Producer sends messages to Kafka.
// It embeds the sarama.SyncProducer type and shadows the Send method to
// use our message.Message type.
type Producer struct {
	sarama.SyncProducer
}

// New creates a configured Producer.
// This Producer is synchronous, it means that it will wait for all the replicas to
// acknowledge the message.
func New(clientID string, maxRetry int, addrs ...string) (*Producer, error) {
	config := newSaramaConfiguration(clientID, maxRetry)
	p, err := sarama.NewSyncProducer(addrs, config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create a producer")
	}

	return &Producer{SyncProducer: p}, nil
}

func newSaramaConfiguration(clientID string, maxRetry int) *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0
	config.ClientID = clientID
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = maxRetry             // Retry up to maxRetry times to produce the message
	// required for the SyncProducer, see https://godoc.org/github.com/Shopify/sarama#SyncProducer
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	return config
}

// Send creates and sends a message to Kafka synchronously.
func (p *Producer) Send(topic string, value interface{}, opts ...message.Option) error {
	msg, err := message.New(topic, value, opts...)
	if err != nil {
		return errors.Wrap(err, "producer: failed to create a message")
	}

	return p.SendMessage(msg)
}

// SendMessage sends the given message to Kafka synchronously.
func (p *Producer) SendMessage(msg *message.Message) error {
	pmsg := &sarama.ProducerMessage{
		Topic: msg.Topic,
		Value: sarama.ByteEncoder(msg.Body),
	}

	if msg.Key != "" {
		pmsg.Key = sarama.StringEncoder(msg.Key)
	}

	var err error
	msg.Partition, msg.Offset, err = p.SyncProducer.SendMessage(pmsg)

	return errors.Wrap(err, "failed to send message")
}
