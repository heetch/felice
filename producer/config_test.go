package producer

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
)

// checks if NewConfig returns the right defaults.
func TestNewConfig(t *testing.T) {
	f := MessageConverterV1()
	c := NewConfig("test", f)
	require.Equal(t, "test", c.ClientID)
	require.Equal(t, sarama.V1_0_0_0, c.Version)
	require.Equal(t, sarama.WaitForAll, c.Config.Producer.RequiredAcks)
	require.Equal(t, 3, c.Config.Producer.Retry.Max)
	require.True(t, c.Config.Producer.Return.Successes)
	require.True(t, c.Config.Producer.Return.Errors)

	require.Equal(t, f, c.Converter)
}
