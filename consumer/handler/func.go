package handler

import (
	"github.com/heetch/felice/message"
)

// A HandleMessageFn is a function type that can be passed to
// HandlerFunc.  Its signature mimics the interface of the
// HandleMessage function in the Handler interface.
type HandleMessageFn func(*message.Message) error

func (h *HandleMessageFn) HandleMessage(msg *message.Message) error {
	return (*h)(msg)
}

// HandlerFunc wraps a HandleMessagFn in such a way that it complies
// with the Handler interface.
func HandlerFunc(fn HandleMessageFn) Handler {
	return &fn
}
