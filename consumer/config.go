package consumer

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/heetch/felice/codec"
)

// Config is used to configure the Consumer.
type Config struct {
	*sarama.Config

	// MaxRetryInterval controls the maximum length of time that
	// the Felice consumer will wait before trying to
	// consume a message from Kafka that failed the first time around.
	// The default value is 5 seconds.
	MaxRetryInterval time.Duration

	// Codec used to decode the message key. Defaults to codec.String.
	KeyCodec codec.Codec
}

// NewConfig creates a config with sane defaults.
// The Sarama Cluster group mode will always be overwritten by the consumer
// and thus cannot be changed, as the consumer is designed to use the ConsumerModePartitions mode.
func NewConfig(clientID string) Config {
	var c Config

	c.Config = sarama.NewConfig()
	c.ClientID = clientID
	c.Consumer.Return.Errors = true
	// Specify that we are using at least Kafka v1.0
	c.Version = sarama.V1_0_0_0

	// Distribute load across instances using round robin strategy
	c.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	// Felice consumer configuration
	c.MaxRetryInterval = 5 * time.Second
	c.KeyCodec = codec.String() // defaults to String
	return c
}
