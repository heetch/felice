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

	t.Run("JVM Partitioner", func(t *testing.T) {
		subtests := []struct {
			In  string
			Out int32
		}{
			// https://github.com/aappleby/smhasher/blob/61a0530f28277f2e850bfc39600ce61d02b518de/src/main.cpp#L73
			{
				In:  "Murmur2B",
				Out: 4,
			},
			{
				In:  "Murmur2C",
				Out: 5,
			},
			// https://github.com/burdiyan/kafkautil/blob/master/partitioner_test.go#L20
			{
				In:  "foobar",
				Out: 6,
			},
			{
				In:  "88d7a76c-48d4-4515-9547-8be944be4594",
				Out: 8,
			},
		}

		partitioner := c.Config.Producer.Partitioner("test-topic")
		for _, st := range subtests {
			partitionNumber, err := partitioner.Partition(
				&sarama.ProducerMessage{Key: sarama.StringEncoder(st.In)},
				12)
			require.NoError(t, err)
			require.Equal(t, st.Out, partitionNumber, "expected to use Murmur2 partitioner as default")
		}
	})
}
