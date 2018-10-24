package handler

import "github.com/heetch/felice/message"

// Handler is the interface for handling consumed messages.  You can
// either add a compliant HandleMessage function to some type by hand,
// or make use of the HandlerFunc.  Note that felice Handlers receive
// felice Message types.
type Handler interface {
	HandleMessage(*message.Message) error
}
