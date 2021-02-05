package consumer_test

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"

	"github.com/heetch/felice/v2/consumer"
)

func TestConfig(t *testing.T) {
	t.Run("Default", func(t *testing.T) {
		c := consumer.NewConfig("client-id")
		require.Equal(t, "client-id", c.ClientID)
		require.False(t, c.Consumer.Return.Errors)
		require.Equal(t, sarama.V1_0_0_0, c.Version)
		require.Equal(t, sarama.BalanceStrategyRoundRobin, c.Consumer.Group.Rebalance.Strategy)

		require.Equal(t, []string{"localhost:9092"}, c.KafkaAddrs)

		require.NoError(t, c.Validate())
	})

	t.Run("With broker addrs", func(t *testing.T) {
		c := consumer.NewConfig("client-id", "1.2.3.4:9092", "5.6.7.8:9092")
		require.Equal(t, "client-id", c.ClientID)
		require.Equal(t, []string{"1.2.3.4:9092", "5.6.7.8:9092"}, c.KafkaAddrs)

		require.NoError(t, c.Validate())
	})

	t.Run("Without broker addrs", func(t *testing.T) {
		c := consumer.Config{
			Config: sarama.NewConfig(),
		}

		err := c.Validate()
		require.Error(t, err)
		require.Equal(t, "felice: invalid configuration (broker addresses must not be empty)", err.Error())
	})
}
