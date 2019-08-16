package consumer

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
)

// Handler is the interface for handling consumed messages.
// See HandlerFunc for a function-based implementation.
// Note that felice Handlers receive felice Message types.
type Handler interface {
	// HandleMessage handles the given message. It is responsible
	// for any retries necessary, and should abort as soon as possible
	// when the context is done.
	// The message will be marked as handled even if an error is returned.
	HandleMessage(context.Context, *Message) error
}

// A handlers slice stores a set of topics and their associated
// handlers.
type handlers []*topicHandler

// topicHandler describes a handler for a topic and
// its associated converter.
type topicHandler struct {
	topic     string
	handler   Handler
	converter MessageConverter
}

func (h *topicHandler) handleMessage(
	ctx context.Context,
	sm *sarama.ConsumerMessage,
	claim sarama.ConsumerGroupClaim,
) error {
	m, err := h.converter.FromKafka(sm)
	if err != nil {
		return fmt.Errorf("message conversion error: %v", err)
	}
	m.claim = claim
	return h.handler.HandleMessage(ctx, m)
}

// get returns the topicHandler associated with a given topic,
// or nil if none was found.
func (hs handlers) get(topic string) *topicHandler {
	for _, h := range hs {
		if h.topic == topic {
			return h
		}
	}
	return nil
}

// Topics returns a slice of all the topic names for which the
// collection contains a HandlerConfig. It is safe to use Topics from
// concurrent code.
func (hs handlers) topics() []string {
	topics := make([]string, len(hs))
	for i, h := range hs {
		topics[i] = h.topic
	}
	return topics
}

// set adds a handler to hs. If there is already a handler
// for h.topic, it will be overridden.
func (hs *handlers) add(h *topicHandler) {
	if old := hs.get(h.topic); old != nil {
		*old = *h
	} else {
		*hs = append(*hs, h)
	}
}
