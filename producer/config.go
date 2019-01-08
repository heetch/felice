package producer

import "github.com/Shopify/sarama"

// Config is used to configure the Producer.
type Config struct {
	sarama.Config
	// here we will add Felice specific configuration.
}

// NewConfig creates a config with sane defaults.
func NewConfig(clientID string) Config {
	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0
	config.ClientID = clientID
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 3                    // Retry up to 3 times to produce the message
	// required for the SyncProducer, see https://godoc.org/github.com/Shopify/sarama#SyncProducer
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	return Config{*config}
}
