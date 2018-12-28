package producer

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	"github.com/heetch/felice/codec"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

// Producer sends messages to Kafka.
// It embeds the sarama.SyncProducer type and shadows the SendMessage method to
// use the Message type.
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
	if cfg.Converter == nil {
		return errors.New("producer: missing Converter in config")
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
func (p *Producer) SendMessage(ctx context.Context, msg *Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if p.config.Converter == nil {
		return errors.New("producer: missing Converter in config")
	}

	msg.prepare()

	pmsg, err := p.config.Converter.ToKafka(ctx, p.config, msg)
	if err != nil {
		return err
	}

	msg.Partition, msg.Offset, err = p.SyncProducer.SendMessage(pmsg)
	msg.ProducedAt = pmsg.Timestamp

	return errors.Wrap(err, "producer: failed to send message")
}

// Message represents a message to be sent via Kafka.
// Before sending it, the producer will transform this structure into a
// sarama.ProducerMessage using the registered Converter.
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

	// Unique ID of the message. Defaults to an uuid.
	ID string
}

// prepare makes sure the message contains a unique ID and
// the Headers map memory is allocated.
func (m *Message) prepare() {
	if m.ID == "" {
		m.ID = uuid.Must(uuid.NewV4()).String()
	}

	if m.Headers == nil {
		m.Headers = make(map[string]string)
	}
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

// A MessageConverter transforms a Message into a sarama.ProducerMessage.
// The role of the converter is to decouple the conventions defined by users from
// the producer.
// Each converter defines a set of convention regarding how the message is
// formatted in Kafka. A converter can add metadata, use an enveloppe to store every information
// in the body or even use Kafka headers.
type MessageConverter interface {
	ToKafka(context.Context, Config, *Message) (*sarama.ProducerMessage, error)
}

// MessageConverterV1 is the first version of the default converter.
// The headers are sent using Kafka headers and the body is encoded into JSON.
// A Message-Id and Produced-At headers are automatically added containing respectively
// the message ID it not empty and the current time in UTC format.
func MessageConverterV1() MessageConverter {
	return new(messageConverterV1)
}

type messageConverterV1 struct{}

// ToKafka converts the message to Sarama ProducerMessage using Kafka headers and JSON body.
func (f *messageConverterV1) ToKafka(ctx context.Context, cfg Config, msg *Message) (*sarama.ProducerMessage, error) {
	// if the Message-Id key is not already filled, override it with the msg.ID
	if _, ok := msg.Headers["Message-Id"]; !ok && msg.ID != "" {
		msg.Headers["Message-Id"] = msg.ID
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
