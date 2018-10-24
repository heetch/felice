package handler

import "github.com/heetch/felice/message"

// Handler is the interface for handling consumed messages. You can
// either add a compliant HandleMessage function to some type by hand,
// or make use of the HandlerFunc. Note that felice Handlers receive
// felice Message types.
type Handler interface {
	// HandleMessage functions will receive a *message.Message
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
	HandleMessage(*message.Message) error
}
