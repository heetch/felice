package consumer

import (
	"testing"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/stretchr/testify/require"
)

// checks if NewConfig returns the right defaults.
func TestNewConfig(t *testing.T) {
	c := NewConfig("test")
	require.Equal(t, "test", c.ClientID)
	require.True(t, c.Consumer.Return.Errors)
	require.Equal(t, sarama.V1_0_0_0, c.Version)
	require.Equal(t, cluster.StrategyRoundRobin, c.Group.PartitionStrategy)
	require.Equal(t, cluster.ConsumerModePartitions, c.Group.Mode)
}
