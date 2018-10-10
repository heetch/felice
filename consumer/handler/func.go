package handler

import (
	"github.com/heetch/felice/message"
)

// A HandlerFunc is a function type that mimics the interface of the
// HandleMessage function in the Handler interface.  If you cast a
// function to this type, it will comply with the Handler interface.
type HandlerFunc func(*message.Message) error

func (h HandlerFunc) HandleMessage(msg *message.Message) error {
	return h(msg)
}
