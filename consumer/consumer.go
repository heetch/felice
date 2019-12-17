package consumer

import (
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/heetch/felice/codec"
	"github.com/pkg/errors"
	"gopkg.in/retry.v1"
)

// clusterConsumer is an interface that makes it possible to fake the cluster.Consumer for testing purposes.
type clusterConsumer interface {
	MarkOffset(*sarama.ConsumerMessage, string)
	Partitions() <-chan cluster.PartitionConsumer
	Close() error
}

// Consumer is the structure used to consume messages from Kafka.
// Having constructed a Consumer you should use its Handle method to
// register per-topic handlers and, finally, call it's Serve method to
// begin consuming messages.
type Consumer struct {
	// Metrics stores a type that implements the felice.MetricsReporter interface.
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
	// If you wish to provide a different value for the Logger, you must do this prior to calling Serve.
	Logger        *log.Logger
	newConsumer   func(addrs []string, groupID string, topics []string, config *cluster.Config) (clusterConsumer, error)
	consumer      clusterConsumer
	retryStrategy retry.Strategy
	config        *Config
	handlers      *collection
	wg            sync.WaitGroup
	quit          chan struct{}
}

// Handle registers the handler for the given topic.
// Handle must be called before Serve.
func (c *Consumer) Handle(topic string, converter MessageConverter, h Handler) {
	c.setup()

	c.handlers.Set(topic, HandlerConfig{
		Handler:   h,
		Converter: converter,
	})

	c.Logger.Printf("Registered handler. topic=%q\n", topic)
}

func (c *Consumer) setup() {
	if c.Logger == nil {
		c.Logger = log.New(ioutil.Discard, "[Felice] ", log.LstdFlags)
	}

	if c.handlers == nil {
		c.handlers = new(collection)
	}

	if c.quit == nil {
		c.quit = make(chan struct{})
	}

	if c.newConsumer == nil {
		c.newConsumer = func(addrs []string, groupID string, topics []string, config *cluster.Config) (clusterConsumer, error) {
			cons, err := cluster.NewConsumer(addrs, groupID, topics, config)
			return clusterConsumer(cons), err
		}
	}

	if c.config != nil && c.retryStrategy == nil {
		// Note: the logic in handleMsg assumes that
		// this does not terminate; be aware of that when changing
		// this strategy.
		if c.config.MaxRetryInterval <= 0 {
			c.config.MaxRetryInterval = defaultRetryInterval
		}
		c.retryStrategy = retry.Exponential{
			Initial:  time.Millisecond,
			Factor:   2,
			MaxDelay: c.config.MaxRetryInterval,
			Jitter:   true,
		}
	}
}

// Serve runs the consumer and listens for new messages on the given
// topics.  You must provide it with unique clientID and the address
// of one or more Kafka brokers.  Serve will block until it is
// instructed to stop, which you can achieve by calling Consumer.Stop.
// When Serve terminates it will return an Error or nil to indicate
// that it excited without error.
func (c *Consumer) Serve(config Config, addrs ...string) error {
	c.config = &config
	err := c.validateConfig()
	if err != nil {
		return err
	}

	c.setup()

	topics := c.handlers.Topics()

	consumerGroup := fmt.Sprintf("%s-consumer-group", c.config.ClientID)
	c.consumer, err = c.newConsumer(
		addrs,
		consumerGroup,
		topics,
		&c.config.Config,
	)
	if err != nil {
		// Note: this kind of error comparison is weird, but
		// it's possible because sarama defines the KError
		// type as an int16 that supports the error interface.
		if kerr, ok := err.(sarama.KError); ok && kerr == sarama.ErrConsumerCoordinatorNotAvailable {
			// We'll add some additional context
			// information.  This issue comes from the
			// RefreshCoordinator call inside sarama, but
			// it's not reporting the root cause, sadly.
			//
			// Even with this information it's rather an
			// annoying little issue.
			err = errors.Wrap(err, "__consumer_offsets topic doesn't yet exist, either because no client has yet requested an offset, or because this consumer group is not yet functioning at startup or after rebalancing.")
		}
		err = errors.Wrap(err, fmt.Sprintf("failed to create a consumer for topics %+v in consumer group %q", topics, consumerGroup))
		return err
	}

	return c.handlePartitions(c.consumer.Partitions())
}

func (c *Consumer) validateConfig() error {
	// Always make sure we are using the right group mode: One chan per partition instead of default multiplexing behaviour.
	if c.config.Group.Mode != cluster.ConsumerModePartitions {
		c.Logger.Println("warning: config.Group.Mode cannot be changed. Value replaced by cluster.ConsumerModePartitions.")
		c.config.Group.Mode = cluster.ConsumerModePartitions
	}

	return nil
}

// Stop the consumer.
func (c *Consumer) Stop() error {
	close(c.quit)
	// TODO(rog) It seems like closing the quit channel probably
	// won't end up causing handleMessages to terminate
	// so this Wait will probably deadlock. Perhaps the consumer should
	// be closed first, or handleMessages could select on quit too.
	defer c.wg.Wait()

	err := c.consumer.Close()
	return errors.Wrap(err, "failed to close the consumer properly")
}

func (c *Consumer) handlePartitions(ch <-chan cluster.PartitionConsumer) error {
	for {
		select {
		case part, ok := <-ch:
			if !ok {
				return fmt.Errorf("partition consumer channel closed")
			}

			c.wg.Add(1)
			go func() {
				defer c.wg.Done()
				c.handleMessages(part.Messages(), c.consumer, part, part.Topic(), part.Partition())
			}()
		case <-c.quit:
			c.Logger.Println("partition handler terminating")
			return nil
		}
	}
}

func (c *Consumer) handleMessages(ch <-chan *sarama.ConsumerMessage, offset offsetStash, max highWaterMarker, topic string, partition int32) {
	logSuffix := fmt.Sprintf(", topic=%q, partition=%d", topic, partition)
	c.Logger.Println("partition messages - reading" + logSuffix)

	for msg := range ch {
		m, attempts := c.handleMsg(msg)
		if m == nil {
			c.Logger.Println("partition messages - closing" + logSuffix)
			break
		}
		// We succeeded in sending the message.
		offset.MarkOffset(msg, "")
		if c.Metrics != nil {
			c.Metrics.Report(*m, &Metrics{
				Attempts:        attempts,
				RemainingOffset: max.HighWaterMarkOffset() - msg.Offset,
			})
		}
	}
}

// handleMsg attempts to handle the message. It retries until the
// message can be handled OK or the consumer is stopped.
// It returns the message that was handled and the
// number of attempts that it took, or
// nil if the consumer was stopped.
func (c *Consumer) handleMsg(msg *sarama.ConsumerMessage) (*Message, int) {
	attempts := 0
	for a := retry.StartWithCancel(c.retryStrategy, nil, c.quit); a.Next(); {
		if !a.More() {
			// Note: we're assuming here that the retry
			// strategy does not terminate except when
			// explicitly cancelled.
			return nil, 0
		}
		attempts++

		// Note: The second returned value is not checked
		// because we will never receive messages for a topic
		// that it does not have a handler for.
		h, _ := c.handlers.Get(msg.Topic)

		// If an error occurs during an unformat call, it
		// usually means the chosen converter was the wrong one.
		// The consumption must block and an error must be
		// reported.
		m, err := h.Converter.FromKafka(msg)
		if err != nil {
			c.Logger.Printf("an error occured while converting a message: %v; retrying", err)
			continue
		}
		if err := h.Handler.HandleMessage(m); err != nil {
			c.Logger.Printf("an error occured while handling a message: %v; retrying", err)
			continue
		}
		return m, attempts
	}
	return nil, attempts

}

// MetricsReporter is an interface that can be passed to set metrics hook to receive metrics
// from the consumer as it handles messages.
type MetricsReporter interface {
	Report(Message, *Metrics)
}

// Metrics contains information about message consumption for a given partition.
type Metrics struct {
	// Number of times the consumer tried to consume this message.
	Attempts int
	// Best effort information about the remaining unconsumed messages in the partition
	RemainingOffset int64
}

type offsetStash interface {
	MarkOffset(msg *sarama.ConsumerMessage, metadata string)
}

type highWaterMarker interface {
	HighWaterMarkOffset() int64
}

// Message represents a message received from Kafka.
// When using Felice's Consumer, any Handlers that you register
// will receive Messages as their arguments.
type Message struct {
	// The Kafka topic this message applies to.
	Topic string

	// Key on which this message was sent to.
	Key codec.Decoder

	// Body of the Kafka message.
	Body codec.Decoder

	// The time at which this Message was produced.
	ProducedAt time.Time

	// Partition where this publication was stored.
	Partition int32

	// Offset where this publication was stored.
	Offset int64

	// Headers of the message.
	Headers map[string]string

	// Unique ID of the message.
	ID string
}

// A MessageConverter transforms a sarama.ConsumerMessage into a Message.
// The role of the converter is to decouple the conventions defined by users from
// the consumer.
// Each converter defines the way it wants to decode metadata, headers and body from the message received from Kafka
// and returns a format agnostic Message structure.
type MessageConverter interface {
	FromKafka(*sarama.ConsumerMessage) (*Message, error)
}

// MessageConverterV1 is the first version of the default converter.
// It converts messages formatted using the producer.MessageConverterV1.
// The headers are extracted from Kafka headers and the body is decoded from JSON.
// If the Message-Id and Produced-At headers are found, they will automatically be added to
// the ID and ProducedAt fields.
func MessageConverterV1(config Config) MessageConverter {
	return &messageConverterV1{config: config}
}

type messageConverterV1 struct {
	config Config
}

// FromKafka converts the message from Kafka headers and JSON body.
func (f *messageConverterV1) FromKafka(cm *sarama.ConsumerMessage) (*Message, error) {
	msg := Message{
		Topic:      cm.Topic,
		Headers:    make(map[string]string),
		Key:        codec.NewDecoder(f.config.KeyCodec, cm.Key),
		Body:       codec.JSONDecoder(cm.Value),
		Partition:  cm.Partition,
		Offset:     cm.Offset,
		ProducedAt: cm.Timestamp,
	}

	for _, pair := range cm.Headers {
		msg.Headers[string(pair.Key)] = string(pair.Value)
	}

	msg.ID = msg.Headers["Message-Id"]

	return &msg, nil
}
