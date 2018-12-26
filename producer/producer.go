package producer

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/heetch/felice/codec"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

// Producer sends messages to Kafka.
// It embeds the sarama.SyncProducer type and adds a Send method to
// use Felice Message type.
type Producer struct {
	sarama.SyncProducer

	config Config
}

// New creates a Producer.
// This Producer is synchronous, this means that it will wait for all the replicas to
// acknowledge the message.
func New(config Config, addrs ...string) (*Producer, error) {
	err := verifyConfig(&config)
	if err != nil {
		return nil, err
	}

	p, err := sarama.NewSyncProducer(addrs, &config.Config)
	if err != nil {
		return nil, errors.Wrap(err, "producer: failed to create a producer")
	}

	return NewFrom(p, config)
}

func verifyConfig(cfg *Config) error {
	if cfg.Formatter == nil {
		return errors.New("producer: missing Formatter in config")
	}

	return nil
}

// NewFrom creates a producer using the given SyncProducer. Useful when
// wanting to create multiple producers with different configurations but sharing the same underlying connection.
func NewFrom(producer sarama.SyncProducer, config Config) (*Producer, error) {
	err := verifyConfig(&config)
	if err != nil {
		return nil, err
	}

	return &Producer{SyncProducer: producer, config: config}, nil
}

// SendMessage sends the given message to Kafka synchronously.
func (p *Producer) SendMessage(msg *Message) error {
	if p.config.Formatter == nil {
		return errors.New("producer: missing Formatter in config")
	}

	pmsg, err := p.config.Formatter.Format(msg)
	if err != nil {
		return err
	}

	msg.Partition, msg.Offset, err = p.SyncProducer.SendMessage(pmsg)
	msg.ProducedAt = pmsg.Timestamp

	return errors.Wrap(err, "producer: failed to send message")
}

// Message represents a message to be sent via Kafka.
// Before sending it, the producer will transform this structure into a
// sarama.ProducerMessage using the registered Formatter.
type Message struct {
	// The Kafka topic this Message applies to.
	Topic string

	// If specified, messages with the same key will be sent to the same Kafka partition.
	Key sarama.Encoder

	// Body of the Kafka message.
	Body interface{}

	// The time at which this Message was produced.
	ProducedAt time.Time

	// Partition where this publication was stored.
	Partition int32

	// Offset where this publication was stored.
	Offset int64

	// Headers of the message.
	Headers map[string]string

	// Unique ID of the message.
	ID string
}

// NewMessage creates a configured message with a generated unique ID.
func NewMessage(topic string, body interface{}) *Message {
	return &Message{
		Topic:   topic,
		Body:    body,
		Headers: make(map[string]string),
		ID:      uuid.Must(uuid.NewV4()).String(),
	}
}

// A MessageFormatter transforms a Message into a sarama.ProducerMessage.
// The role of the formatter is to decouple the conventions defined by users from
// the producer.
// Each formatter defines a set of convention regarding how the message is
// formatted in Kafka. A formatter can add metadata, use an enveloppe to store every information
// in the body or even use Kafka headers.
type MessageFormatter interface {
	Format(*Message) (*sarama.ProducerMessage, error)
}

// MessageFormatterV1 is the first version of the default formatter.
// The headers are sent using Kafka headers and the body is encoded into JSON.
// A Message-Id and Produced-At headers are automatically added containing respectively
// the message ID it not empty and the current time in UTC format.
func MessageFormatterV1() MessageFormatter {
	return new(messageFormatterV1)
}

type messageFormatterV1 struct{}

// Format the message using Kafka headers and JSON body.
func (f *messageFormatterV1) Format(msg *Message) (*sarama.ProducerMessage, error) {
	// if the Message-Id key is not already filled, override it with the msg.ID
	if _, ok := msg.Headers["Message-Id"]; !ok && msg.ID != "" {
		msg.Headers["Message-Id"] = msg.ID
	}

	// if the Produced-At key is not already filled, override it with the current time
	if _, ok := msg.Headers["Produced-At"]; !ok {
		msg.Headers["Produced-At"] = time.Now().UTC().Format(time.RFC3339)
	}

	pmsg := sarama.ProducerMessage{
		Topic: msg.Topic,
		Key:   msg.Key,
		Value: codec.JSONEncoder(msg.Body), // encode the body as a JSON object
	}

	pmsg.Headers = make([]sarama.RecordHeader, 0, len(msg.Headers))
	for k, v := range msg.Headers {
		pmsg.Headers = append(pmsg.Headers, sarama.RecordHeader{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}

	return &pmsg, nil
}
