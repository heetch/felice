package consumer

import (
	"errors"
	"time"

	"github.com/Shopify/sarama"
	"github.com/heetch/felice/codec"
	"gopkg.in/retry.v1"
)

// Config is used to configure the Consumer.
type Config struct {
	*sarama.Config

	// KafkaAddrs holds kafka brokers addresses. There must be at least
	// one entry in the slice.
	// The default value is "localhost:9092".
	KafkaAddrs []string

	// MaxRetryInterval controls the maximum length of time that
	// the Felice consumer will wait before trying to
	// consume a message from Kafka that failed the first time around.
	// The default value is 5 seconds.
	MaxRetryInterval time.Duration
	// Codec used to decode the message key.
	// The default value is codec.String.
	KeyCodec codec.Codec

	retryStrategy retry.Strategy
}

// NewConfig creates a config with sane defaults.
// Broker addresses defaults to localhost:9092.
func NewConfig(clientID string, addrs ...string) Config {
	var c Config

	c.Config = sarama.NewConfig()
	c.ClientID = clientID
	c.Consumer.Return.Errors = true
	// Specify that we are using at least Kafka v1.0
	c.Version = sarama.V1_0_0_0
	// Distribute load across instances using round robin strategy
	c.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	// Felice consumer configuration
	c.KafkaAddrs = addrs
	if len(c.KafkaAddrs) == 0 {
		c.KafkaAddrs = []string{"localhost:9092"}
	}

	c.MaxRetryInterval = 5 * time.Second
	c.KeyCodec = codec.String() // defaults to String

	// Note: the logic in handleMsg assumes that
	// this does not terminate; be aware of that when changing
	// this strategy.
	c.retryStrategy = retry.Exponential{
		Initial:  time.Millisecond,
		Factor:   2,
		MaxDelay: c.MaxRetryInterval,
		Jitter:   true,
	}

	return c
}

// Validate validates the configuration.
func (c Config) Validate() error {
	if len(c.KafkaAddrs) == 0 {
		return errors.New("felice: invalid configuration (broker addresses must not be empty)")
	}

	return c.Config.Validate()
}
