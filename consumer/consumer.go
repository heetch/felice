package consumer

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/heetch/felice/codec"
	"github.com/heetch/felice/consumer/handler"
	"github.com/pkg/errors"
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
	Logger      *log.Logger
	newConsumer func(addrs []string, groupID string, topics []string, config *cluster.Config) (clusterConsumer, error)
	consumer    clusterConsumer
	config      *Config
	handlers    *handler.Collection
	wg          sync.WaitGroup
	quit        chan struct{}
}

// Handle registers the handler for the given topic.
// Handle must be called before Serve.
func (c *Consumer) Handle(topic string, h handler.Handler) {
	c.setup()

	c.handlers.Set(topic, h)
}

func (c *Consumer) setup() {
	if c.Logger == nil {
		c.Logger = log.New(ioutil.Discard, "[Felice] ", log.LstdFlags)
	}
	if c.handlers == nil {
		c.handlers = &handler.Collection{Logger: c.Logger}
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
}

// Serve runs the consumer and listens for new messages on the given
// topics.  You must provide it with unique clientID and the address
// of one or more Kafka brokers.  Serve will block until it is
// instructed to stop, which you can achieve by calling Consumer.Stop.
// When Serve terminates it will return an Error or nil to indicate
// that it excited without error.
func (c *Consumer) Serve(config Config, addrs ...string) error {
	c.setup()

	c.config = &config
	err := c.validateConfig()
	if err != nil {
		return err
	}

	topics := c.handlers.Topics()

	consumerGroup := fmt.Sprintf("%s-consumer-group", c.config.ClientID)
	c.consumer, err = c.newConsumer(
		addrs,
		consumerGroup,
		topics,
		&c.config.Config)
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

	err = c.handlePartitions(c.consumer.Partitions())
	return err
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
			go func(pc cluster.PartitionConsumer) {
				defer c.wg.Done()
				c.handleMessages(pc.Messages(), c.consumer, pc, pc.Topic(), pc.Partition())
			}(part)
		case <-c.quit:
			c.Logger.Println("partition handler terminating")
			return nil

		}
	}
}

func (c *Consumer) handleMessages(ch <-chan *sarama.ConsumerMessage, offset offsetStash, max highWaterMarker, topic string, partition int32) {
	logSuffix := fmt.Sprintf(", topic=%q, partition=%d\n", topic, partition)
	c.Logger.Println("partition messages - reading" + logSuffix)

	for msg := range ch {
		var attempts int

		for {
			attempts++

			m := c.convertMessage(msg)
			// Note: The second returned value is not checked because we will never receive messages for a topic
			// that it does not have a handler for.
			h, _ := c.handlers.Get(msg.Topic)
			// err := h.(handler.Handler).HandleMessage(m)
			var err error
			if err == nil {
				offset.MarkOffset(msg, "")
				if c.Metrics != nil {
					c.Metrics.Report(*m, map[string]string{
						"attempts":        strconv.Itoa(attempts),
						"msgOffset":       strconv.FormatInt(msg.Offset, 10),
						"remainingOffset": strconv.FormatInt(max.HighWaterMarkOffset()-msg.Offset, 10),
					})
				}
				break
			}

			select {
			case <-time.After(c.config.RetryInterval):
			case <-c.quit:
				c.Logger.Println("partition messages - closing" + logSuffix)
				return
			}
		}
	}
}

func (c *Consumer) convertMessage(cm *sarama.ConsumerMessage) *Message {
	var msg Message
	if cm.Key != nil {
		msg.Key = string(cm.Key)
	}

	msg.Topic = cm.Topic
	msg.ProducedAt = cm.Timestamp
	msg.Offset = cm.Offset
	msg.Partition = cm.Partition
	// msg.Body = cm.Value

	return &msg
}

// MetricsReporter is a interface that can be passed to set metrics hook to receive metrics
// from the consumer as it handles messages.
type MetricsReporter interface {
	Report(Message, map[string]string)
}

type offsetStash interface {
	MarkOffset(msg *sarama.ConsumerMessage, metadata string)
}

type highWaterMarker interface {
	HighWaterMarkOffset() int64
}

// Message represents a message received from Kafka.
// When using Felice's Consumer, any Handlers that you register
// will receive Messages as they're arguments.
type Message struct {
	// The Kafka topic this Message applies to.
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

// A MessageUnformatter transforms a sarama.ProducerMessage into a Message.
// The role of the unformatter is to decouple the conventions defined by users from
// the consumer.
// Each unformatter defines the way it wants to decode metadata, headers and body from the message received from Kafka
// and returns a format agnostic Message structure.
type MessageUnformatter interface {
	Unformat(Config, *sarama.ConsumerMessage) (*Message, error)
}

// MessageUnformatterV1 is the first version of the default unformatter.
// It unformats messages formatted using the producer.MessageFormatterv1.
// The headers are extracted from Kafka headers and the body is decoded from JSON.
// If the Message-Id and Produced-At headers are found, they will automatically be added to
// the ID and ProducedAt fields.
func MessageUnformatterV1() MessageUnformatter {
	return new(messageUnformatterV1)
}

type messageUnformatterV1 struct{}

// Unformat the message from Kafka headers and JSON body.
func (f *messageUnformatterV1) Unformat(config Config, cm *sarama.ConsumerMessage) (*Message, error) {
	msg := Message{
		Topic:     cm.Topic,
		Headers:   make(map[string]string),
		Key:       codec.NewDecoder(config.KeyCodec, cm.Key),
		Body:      codec.JSONDecoder(cm.Value),
		Partition: cm.Partition,
		Offset:    cm.Offset,
	}

	for _, pair := range cm.Headers {
		msg.Headers[string(pair.Key)] = string(pair.Value)
	}

	msg.ID = msg.Headers["Message-Id"]

	if msg.Headers["Produced-At"] != "" {
		// if the Produced-At timestamp is malformed, we simply skip it.
		// the user can check if the field is empty or not.
		msg.ProducedAt, _ = time.Parse(time.RFC3339, msg.Headers["Produced-At"])
	}

	return &msg, nil
}
