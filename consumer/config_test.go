package consumer

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
	"gopkg.in/retry.v1"
)

func TestConfig(t *testing.T) {
	t.Run("Default", func(t *testing.T) {
		c := NewConfig("client-id")
		require.Equal(t, "client-id", c.ClientID)
		require.True(t, c.Consumer.Return.Errors)
		require.Equal(t, sarama.V1_0_0_0, c.Version)
		require.Equal(t, sarama.BalanceStrategyRoundRobin, c.Consumer.Group.Rebalance.Strategy)

		require.Equal(t, 5*time.Second, c.MaxRetryInterval)
		require.NotNil(t, c.KeyCodec)

		require.Equal(t, []string{"localhost:9092"}, c.KafkaAddrs)
		require.Equal(t, retry.Exponential{
			Initial:  time.Millisecond,
			Factor:   2,
			MaxDelay: c.MaxRetryInterval,
			Jitter:   true,
		}, c.retryStrategy)

		require.NoError(t, c.Validate())
	})

	t.Run("With broker addrs", func(t *testing.T) {
		c := NewConfig("client-id", "1.2.3.4:9092", "5.6.7.8:9092")
		require.Equal(t, "client-id", c.ClientID)
		require.Equal(t, []string{"1.2.3.4:9092", "5.6.7.8:9092"}, c.KafkaAddrs)

		require.NoError(t, c.Validate())
	})

	t.Run("Without broker addrs", func(t *testing.T) {
		c := Config{
			Config: sarama.NewConfig(),
		}

		err := c.Validate()
		require.Error(t, err)
		require.Equal(t, "felice: invalid configuration (broker addresses must not be empty)", err.Error())
	})
}
