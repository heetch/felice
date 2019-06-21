package consumer

import (
	"io/ioutil"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/heetch/felice/codec"
	"gopkg.in/retry.v1"
)

// MetricsReporter is an interface that can be passed to set metrics hook to receive metrics
// from the consumer as it handles messages.
type MetricsReporter interface {
	Report(Message, *Metrics)
}

// Config is used to configure the Consumer.
type Config struct {
	*sarama.Config

	// KafkaAddrs holds kafka brokers addresses. There must be at least
	// one entry in the slice.
	// Default to localhost:9092.
	KafkaAddrs []string

	// Metrics stores a type that implements the MetricsReporter interface.
	// If you provide an implementation, then its Report function will be called every time
	// a message is successfully handled.  The Report function will
	// receive a copy of the message that was handled, along with
	// a map[string]string containing metrics about the handling of the
	// message.  Currently we pass the following metrics: "attempts" (the
	// number of attempts it took before the message was handled);
	// "msgOffset" (the marked offset for the message on the topic); and
	// "remainingOffset" (the difference between the high water mark and
	// the current offset).
	Metrics MetricsReporter
	// If you wish to provide a different value for the Logger, you must do this prior to calling Consumer.Serve.
	Logger *log.Logger
	// MaxRetryInterval controls the maximum length of time that
	// the Felice consumer will wait before trying to
	// consume a message from Kafka that failed the first time around.
	// Default to 5 seconds.
	MaxRetryInterval time.Duration
	// Codec used to decode the message key. Defaults to codec.String.
	KeyCodec codec.Codec

	retryStrategy retry.Strategy
}

// NewConfig creates a config with sane defaults.
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
	if c.KafkaAddrs == nil {
		c.KafkaAddrs = []string{"localhost:9092"}
	}

	c.MaxRetryInterval = 5 * time.Second
	c.KeyCodec = codec.String() // defaults to String

	c.Logger = log.New(ioutil.Discard, "[Felice] ", log.LstdFlags)

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
