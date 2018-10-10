package handler

import "github.com/heetch/felice/message"

// A Handler handles a Message.
type Handler interface {
	HandleMessage(*message.Message) error
}
