package producer

import (
	"github.com/Shopify/sarama"
	"github.com/heetch/felice/codec"
)

// Config is used to configure the Producer.
type Config struct {
	sarama.Config

	// Codec used to encode the body of the message. Is required.
	Codec codec.Codec

	// Codec used to encode the message key. Defaults to codec.String.
	KeyCodec codec.Codec

	// Formatter used to translate Felice messages to Sarama ones.
	Formatter MessageFormatter
}

// NewConfig creates a config with sane defaults. Parameter clientID is directly copied set in Sarama.Config.ClientID
// and the defaultCodec will be used to encode every message with no specified codec.
func NewConfig(clientID string, defaultCodec codec.Codec) Config {
	var c Config

	// Sarama configuration
	c.Config = *sarama.NewConfig()
	c.Config.Version = sarama.V1_0_0_0
	c.Config.ClientID = clientID
	c.Config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	c.Config.Producer.Retry.Max = 3                    // Retry up to 3 times to produce the message
	// required for the SyncProducer, see https://godoc.org/github.com/Shopify/sarama#SyncProducer
	c.Config.Producer.Return.Successes = true
	c.Config.Producer.Return.Errors = true

	// Felice configuration
	c.Codec = defaultCodec
	c.KeyCodec = codec.String() // defaults to String
	return c
}
