package consumer

import (
	"context"
	"errors"

	"github.com/Shopify/sarama"
)

// Config is used to configure the Consumer.
// It should be created with NewConfig before any custom
// configuration settings are applied.
type Config struct {
	// Config holds the configuration used to create the consumer
	// group instance. It must be non-nil.
	*sarama.Config

	// KafkaAddrs holds the kafka broker addresses in host:port
	// format. There must be at least one entry in the slice.
	KafkaAddrs []string

	// If non-nil, Discarded is called when a message handler
	// returns an error. It receives the ConsumerGroupSession context as first parameter.
	// It returns if the message should be marked as committed.
	// If Discarded is not set, then the message will be marked as committed.
	Discarded func(ctx context.Context, m *sarama.ConsumerMessage, err error) (mark bool)
}

// NewConfig returns a configuration filled in with default values.
//
// If addrs is empty, KafkaAddrs will default to localhost:9092.
//
// The clientID is used to form the consumer group name
// (clientID + "-consumer-group").
func NewConfig(clientID string, addrs ...string) Config {
	c := sarama.NewConfig()
	c.ClientID = clientID

	// Specify that we are using at least Kafka v1.0
	c.Version = sarama.V1_0_0_0

	// Distribute load across partitions using round robin strategy
	c.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	// Note: we could set c.Consumer.Return.Errors to  true
	// and then read on the errors channel to log consumer errors.

	if len(addrs) == 0 {
		addrs = []string{"localhost:9092"}
	}
	return Config{
		Config:     c,
		KafkaAddrs: addrs,
	}
}

// Validate validates the configuration.
func (c Config) Validate() error {
	if len(c.KafkaAddrs) == 0 {
		return errors.New("felice: invalid configuration (broker addresses must not be empty)")
	}

	return c.Config.Validate()
}
