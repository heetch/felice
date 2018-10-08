package message

import (
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

// Message contains informations about the message to be sent to Kafka.
type Message struct {
	// Kafka topic.
	Topic string

	// If specified, messages with the same key will be sent to the same Kafka partition.
	Key string

	// Body of the Kafka message.
	Body []byte

	ProducedAt time.Time

	// Partition where this publication was stored.
	Partition int32

	// Offset where this publication was stored.
	Offset int64

	// Headers of the message.
	Headers map[string]string

	// Unique id of the message.
	ID string
}

// New creates a new configured message.
func New(topic string, value interface{}, opts ...Option) (*Message, error) {
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
