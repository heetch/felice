package consumer

import (
	"sync"
)

// Handler is the interface for handling consumed messages. You can
// either add a compliant HandleMessage function to some type by hand,
// or make use of the HandlerFunc. Note that felice Handlers receive
// felice Message types.
type Handler interface {
	// HandleMessage functions will receive a *Message
	// type, and may do with it what they wish.  However, some
	// caution should be used when returning an error from a
	// HandleMessage function.  Errors are considered an
	// indication that the message has not been handled.  After
	// waiting a respectable amount of time, the HandleMessage
	// function will be called again with the same message.  This
	// will continue until on of the following conditions is met:
	//
	// - 1. HandleMessage returns nil, instead of an error.
	// - 2. A system administrator intervenes.
	// - 3. The Earth is consumed by the sun.
	HandleMessage(*Message) error
}

// A HandlerFunc is a function that supports the Handler interface.
// Calls to Handler.HandleMessage made against any function cast to
// this type will result in the function itself being called.
type HandlerFunc func(*Message) error

// HandleMessage implements the Handler interface by calling the
// HandlerFunc to which it is associated.
func (h HandlerFunc) HandleMessage(msg *Message) error {
	return h(msg)
}

// A collection is set of HandlerConfig types, keyed by topic. Only one
// handler config may exists in a collection per topic, but any number of
// topics may be contained in the collection. The handler config for a topic
// can be added via Set, and retrieved via Get. It is not currently
// possible to remove a topic. Additionally the set of all topics with
// handlers can be returned via the Topics function. These actions are
// safe to use in concurrent code.
type collection struct {
	sync.RWMutex
	handlers map[string]HandlerConfig
}

// Get returns the HandlerConfig associated with a given topic, and a
// Boolean value indicating if the topic was found at all. It is safe
// to use Get from concurrent code.
func (h *collection) Get(topic string) (handlerCfg HandlerConfig, ok bool) {
	h.RLock()
	handlerCfg, ok = h.handlers[topic]
	h.RUnlock()
	return
}

// Topics returns a slice of all the topic names for which the
// collection contains a HandlerConfig. It is safe to use Topics from
// concurrent code.
func (h *collection) Topics() []string {
	h.RLock()
	i := 0
	count := len(h.handlers)
	topics := make([]string, count)
	for t := range h.handlers {
		topics[i] = t
		i++
	}
	h.RUnlock()
	return topics
}

// Set associates the given HandlerConfig to the given Topic within the
// collection. If a Handler was already associated with the Topic,
// then that association will be lost and replaced by the new one. It
// is safe to use Set from concurrent code.
func (h *collection) Set(topic string, handlerCfg HandlerConfig) {
	h.Lock()
	if h.handlers == nil {
		h.handlers = make(map[string]HandlerConfig)
	}
	h.handlers[topic] = handlerCfg
	h.Unlock()
}

// HandlerConfig describes a handler and its configuration.
// It is currently used to store the related message unformatter.
type HandlerConfig struct {
	Handler     Handler
	Unformatter MessageUnformatter
}
