package message

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

// Message represents a message to be sent via Kafka, or received from
// it.  When using Felice's Consumer, any Handlers that you register
// will receive Messages as they're arguments.  When using the Felice
// Producer, you will be sending Messages.  When making a Message to
// be sent it essential that you use the New function to do so.
type Message struct {
	// The Kafka topic this Message applies to.
	Topic string

	// If specified, messages with the same key will be sent to the same Kafka partition.
	Key string

	// Body of the Kafka message. For now this will always be a
	// JSON marshaled form of whatever was passed to New.
	Body []byte

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

// New creates a new Message, correctly configured for use with the
// felice Producer.  You should not attempt to create Message types by
// hand (unless you really know what you're doing! - I'm just some
// documentation, not the police).
func New(topic string, value interface{}, opts ...Option) (*Message, error) {
	if topic == "" {
		return nil, fmt.Errorf("messages require a non-empty topic")
	}

	uuid, err := uuid.NewV4()
	if err != nil {
		return nil, errors.Wrap(err, "failed to generate an UUID")
	}

	id := uuid.String()

	m := Message{
		ID:    id,
		Topic: topic,
		Headers: map[string]string{
			"Message-Id":  id,
			"Produced-At": time.Now().UTC().String(),
		},
	}

	for _, o := range opts {
		o(&m)
	}

	body, err := json.Marshal(value)
	if err != nil {
		return nil, errors.Wrap(err, "failed to encode message body")
	}

	m.Body = body

	return &m, nil
}
