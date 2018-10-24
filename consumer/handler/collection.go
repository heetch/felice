package handler

import (
	"log"
	"sync"
)

// A Collection is set of Handler types, keyed by topic. Only one
// handler may exists in a Collection per topic, but any number of
// topics may be contained in the Collection. The handler for a topic
// can be added via Set, and retrieved via Get. It is not currently
// possible to remove a topic. Additionally the set of all topics with
// handlers can be returned via the Topics function. These actions are
// safe to use in concurrent code.
type Collection struct {
	sync.RWMutex
	handlers map[string]Handler
	Logger   *log.Logger
}

// Get returns the Handler associated with a given topic, and a
// Boolean value indicating if the topic was found at all. It is safe
// to use Get from concurrent code.
func (h *Collection) Get(topic string) (handler Handler, ok bool) {
	h.RLock()
	handler, ok = h.handlers[topic]
	h.RUnlock()
	return
}

// Topics returns a slice of all the topic names for which the
// Collection contains a Handler. It is safe to use Topics from
// concurrent code.
func (h *Collection) Topics() []string {
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

// Set associates the given Handler to the given Topic within the
// collection. If a Handler was already associated with the Topic,
// then that association will be lost and replaced by the new one. It
// is safe to use Set from concurrent code.
func (h *Collection) Set(topic string, handler Handler) {
	h.Lock()
	if h.handlers == nil {
		h.handlers = make(map[string]Handler)
	}
	if h.Logger != nil {
		h.Logger.Printf("Registered handler. topic=%q\n", topic)
	}
	h.handlers[topic] = handler
	h.Unlock()
}
