package producer

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/heetch/felice/codec"
	uuid "github.com/satori/go.uuid"
)

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

// Option is a function type that receives a pointer to a Message and
// modifies it in place. Options are intended to customize a message
// before sending it. You can do this either by passing them as
// parameters to the New function, or by calling them directly against
// a Message.
type Option func(*Message)

// Header is an Option that adds a custom header to the message. You
// may pass as many Header options to New as you wish. If multiple
// Header's are defined for the same key, the value of the last one
// past to New will be the value that appears on the Message.
func Header(k, v string) Option {
	return func(m *Message) {
		m.Headers[k] = v
	}
}

// Key is an Option that specifies a key for the message. You should
// only pass this once to the New function, but if you pass it multiple
// times, the value set by the final one you pass will be what is set
// on the Message when it is returned by New.
func Key(key codec.Encoder) Option {
	return func(m *Message) {
		m.Key = key
	}
}
