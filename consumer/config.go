package consumer

import (
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

// Config is used to configure the Consumer.
type Config struct {
	cluster.Config
	// here we will add Felice specific configuration.
}

// NewConfig creates a config with sane defaults.
// The Sarama Cluster group mode will always be overwritten by the consumer
// and thus cannot be changed, as the consumer is designed to use the ConsumerModePartitions mode.
func NewConfig(clientID string) Config {
	c := cluster.NewConfig()
	c.ClientID = clientID
	c.Consumer.Return.Errors = true
	// Specify that we are using at least Kafka v1.0
	c.Version = sarama.V1_0_0_0
	// Distribute load across instances using round robin strategy
	c.Group.PartitionStrategy = cluster.StrategyRoundRobin
	// One chan per partition instead of default multiplexing behaviour.
	c.Group.Mode = cluster.ConsumerModePartitions

	return Config{Config: *c}
}
