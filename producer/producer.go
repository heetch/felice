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

	config Config
}

// New creates a Producer.
// This Producer is synchronous, this means that it will wait for all the replicas to
// acknowledge the message.
func New(config Config, addrs ...string) (*Producer, error) {
	p, err := sarama.NewSyncProducer(addrs, &config.Config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create a producer")
	}

	return NewFrom(p, config), nil
}

// NewFrom creates a producer using the given SyncProducer. Useful when
// wanting to create multiple producers with different configurations but sharing the same underlying connection.
func NewFrom(producer sarama.SyncProducer, config Config) *Producer {
	return &Producer{SyncProducer: producer, config: config}
}

// Send creates and sends a message to Kafka synchronously.
// It returns the message.Message sent to the brokers.
func (p *Producer) Send(topic string, value interface{}, opts ...message.Option) (*message.Message, error) {
	msg, err := message.New(topic, value, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "producer: failed to create a message")
	}

	err = p.SendMessage(msg)
	return msg, err
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
