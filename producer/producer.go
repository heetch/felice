package producer

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"

	"github.com/heetch/felice/v2/codec"
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

// Send creates and sends a message to Kafka synchronously.
// It returns the message.Message sent to the brokers.
func (p *Producer) Send(ctx context.Context, topic string, body interface{}, opts ...Option) (*Message, error) {
	msg := NewMessage(topic, body)
	for _, opt := range opts {
		opt(msg)
	}

	err := p.SendMessage(ctx, msg)
	return msg, err
}

// SendMessagesErrors is the error type returned if SendMessages
// fails to send to Kafka.
type SendMessagesErrors []*SendMessagesError

func (e SendMessagesErrors) Error() string {
	return fmt.Sprintf("kafka: failed to deliver %d messages", len(e))
}

// SendMessagesError describes why one message
// failed to be sent.
type SendMessagesError struct {
	Msg *Message
	Err error
}

// SendMessages sends all the given messages in order.
// If it fails to send the messages to Kafka, it will return a
// SendMessagesErrors error describing which messages failed.
func (p *Producer) SendMessages(ctx context.Context, msgs []*Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if p.config.Converter == nil {
		return errors.New("producer: missing Converter in config")
	}
	pmsgs := make([]*sarama.ProducerMessage, len(msgs))
	for i, m := range msgs {
		m.prepare()
		pmsg, err := p.config.Converter.ToKafka(ctx, m)
		if err != nil {
			return err
		}
		pmsg.Metadata = m
		pmsgs[i] = pmsg
	}
	err := p.SyncProducer.SendMessages(pmsgs)
	// Some messages may still have been sent, so copy all the
	// send information across even when there are errors.
	for i, m := range msgs {
		p := pmsgs[i]
		m.Partition, m.Offset, m.ProducedAt = p.Partition, p.Offset, p.Timestamp
	}
	if producerErrs, ok := err.(sarama.ProducerErrors); ok {
		sendErrs := make(SendMessagesErrors, len(producerErrs))
		for i, e := range producerErrs {
			sendErrs[i] = &SendMessagesError{
				Msg: e.Msg.Metadata.(*Message),
				Err: e.Err,
			}
		}
		err = sendErrs
	}
	return err
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

	pmsg, err := p.config.Converter.ToKafka(ctx, msg)
	if err != nil {
		return err
	}

	msg.Partition, msg.Offset, err = p.SyncProducer.SendMessage(pmsg)
	msg.ProducedAt = pmsg.Timestamp

	return errors.Wrap(err, "producer: failed to send message")
}

// A MessageConverter transforms a Message into a sarama.ProducerMessage.
// The role of the converter is to decouple the conventions defined by users from
// the producer.
// Each converter defines a set of convention regarding how the message is
// formatted in Kafka. A converter can add metadata, use an enveloppe to store every information
// in the body or even use Kafka headers.
type MessageConverter interface {
	ToKafka(context.Context, *Message) (*sarama.ProducerMessage, error)
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
func (f *messageConverterV1) ToKafka(ctx context.Context, msg *Message) (*sarama.ProducerMessage, error) {
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
