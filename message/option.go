package message

// Option is used to customize a message before sending it.
type Option func(*Message)

// Header option adds a header to the message.
func Header(k, v string) Option {
	return func(m *Message) {
		m.Headers[k] = v
	}
}

// Key option specifies a key for the message.
func Key(key string) Option {
	return func(m *Message) {
		m.Key = key
	}
}
