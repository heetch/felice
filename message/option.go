package message

// Option is a function type that receives a pointer to a Message and
// modifies it in place. Options are intended to customize a message
// before sending it. You can do this either by passing them as
// parameters to the New function, or by calling them directly against
// a Message.
type Option func(*Message)

// Header is an Option that adds a custom header to the message. You
// may pass as many Header options to New as you wish. If multiple
// Header's are defined for the same key, the value of the last one
// past to New will be the value that appears on the Message.
func Header(k, v string) Option {
	return func(m *Message) {
		m.Headers[k] = v
	}
}

// Key is an Option that specifies a key for the message. You should
// only pass ths once to the New function, but if you pass it multiple
// times, the value set by the final one you pass will be what is set
// on the Message when it is returned by New.
func Key(key string) Option {
	return func(m *Message) {
		m.Key = key
	}
}
