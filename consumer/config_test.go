package consumer

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
)

func TestNewConfig(t *testing.T) {
	c := NewConfig("test")
	require.Equal(t, "test", c.ClientID)
	require.True(t, c.Consumer.Return.Errors)
	require.Equal(t, sarama.V1_0_0_0, c.Version)
	require.Equal(t, sarama.BalanceStrategyRoundRobin, c.Consumer.Group.Rebalance.Strategy)

	require.NotNil(t, c.KeyCodec)
}
