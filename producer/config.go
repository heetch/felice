package producer

import (
	"github.com/Shopify/sarama"
)

// Config is used to configure the Producer.
type Config struct {
	sarama.Config

	// Converter used to translate Felice messages to Sarama ones.
	Converter MessageConverter
}

// NewConfig creates a config with sane defaults. Parameter clientID is directly copied in Sarama.Config.ClientID.
func NewConfig(clientID string, converter MessageConverter) Config {
	var c Config

	// Sarama configuration
	c.Config = *sarama.NewConfig()
	c.Config.Version = sarama.V1_0_0_0
	c.Config.ClientID = clientID
	c.Config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	c.Config.Producer.Retry.Max = 3                    // Retry up to 3 times to produce the message
	c.Config.Producer.Partitioner = NewJVMCompatiblePartitioner
	// required for the SyncProducer, see https://godoc.org/github.com/Shopify/sarama#SyncProducer
	c.Config.Producer.Return.Successes = true
	c.Config.Producer.Return.Errors = true

	c.Converter = converter
	return c
}
