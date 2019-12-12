package consumer

import (
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/heetch/felice/codec"
)

// Config is used to configure the Consumer.
type Config struct {
	cluster.Config

	// MaxRetryInterval controls the maximum length of time that
	// the Felice consumer will wait before trying to
	// consume a message from Kafka that failed the first time around.
	// The default value is 5 seconds.
	MaxRetryInterval time.Duration

	// Codec used to decode the message key. Defaults to codec.String.
	KeyCodec codec.Codec
}

const defaultRetryInterval = 5 * time.Second

// NewConfig creates a config with sane defaults.
// The Sarama Cluster group mode will always be overwritten by the consumer
// and thus cannot be changed, as the consumer is designed to use the ConsumerModePartitions mode.
func NewConfig(clientID string) Config {
	var c Config

	// Sarama Cluster configuration
	c.Config = *cluster.NewConfig()
	c.ClientID = clientID
	c.Consumer.Return.Errors = true
	// Specify that we are using at least Kafka v1.0
	c.Version = sarama.V1_0_0_0
	// Distribute load across instances using round robin strategy
	c.Group.PartitionStrategy = cluster.StrategyRoundRobin
	// One chan per partition instead of default multiplexing behaviour.
	c.Group.Mode = cluster.ConsumerModePartitions

	// Felice consumer configuration
	c.KeyCodec = codec.String() // defaults to String
	return c
}
