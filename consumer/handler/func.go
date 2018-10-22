package handler

import (
	"github.com/heetch/felice/message"
)

// A HandlerFunc is a function that supports the Handler interface.
// Calls to Handler.HandleMessage made against any function cast to
// this type will result in the function itself being called.
type HandlerFunc func(*message.Message) error

// HandleMessage implements the Handler interface by calling the
// HandlerFunc to which it is associated.
func (h HandlerFunc) HandleMessage(msg *message.Message) error {
	return h(msg)
}
