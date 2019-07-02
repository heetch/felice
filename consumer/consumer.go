package consumer

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/heetch/felice/codec"
	"github.com/pkg/errors"
	"gopkg.in/retry.v1"
)

// Consumer is the structure used to consume messages from Kafka.
// Having constructed a Consumer with New, you should use its Handle method to
// register per-topic handlers and, finally, call it's Serve method to
// begin consuming messages.
type Consumer struct {
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

	// Logger output defaults to ioutil.Discard.
	// If you wish to provide a different value for the Logger, you must do this prior to calling Consumer.Serve.
	Logger *log.Logger

	consumer sarama.ConsumerGroup
	config   *Config
	handlers *collection
	quit     chan struct{}
}

// New returns a ready to use Consumer configured with sane defaults.
// The passed Config shouldn't be nil otherwise a non nil error will be returned.
func New(cfg *Config) (*Consumer, error) {
	if cfg == nil {
		return nil, errors.New("configuration must be not nil")
	}

	err := cfg.Config.Validate()
	if err != nil {
		return nil, err
	}

	return &Consumer{
		Logger: log.New(ioutil.Discard, "[Felice] ", log.LstdFlags),

		config:   cfg,
		handlers: new(collection),
		quit:     make(chan struct{}),
	}, nil
}

// Handle registers the handler for the given topic.
// Handle must be called before Serve.
func (c *Consumer) Handle(topic string, converter MessageConverter, h Handler) {
	c.handlers.Set(topic, HandlerConfig{
		Handler:   h,
		Converter: converter,
	})

	c.Logger.Printf("Registered handler. topic=%q\n", topic)
}

// Serve runs the consumer and listens for new messages on the given topics.
// Serve will block until it is instructed to stop, which you can achieve by calling Consumer.Stop.
// When Serve terminates it will return an Error or nil to indicate that it exited without error.
func (c *Consumer) Serve() error {
	var err error

	groupID := fmt.Sprintf("%s-consumer-group", c.config.ClientID)
	c.consumer, err = sarama.NewConsumerGroup(c.config.KafkaAddrs, groupID, c.config.Config)
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
		err = errors.Wrap(err, fmt.Sprintf("failed to create consumer group %q", groupID))
		return err
	}

	cgh := consumerGroupHandler{
		consumer: c,
	}

	err = c.consumer.Consume(context.Background(), c.handlers.Topics(), cgh)
	c.Logger.Println("consumer closing")
	return err
}

// consumerGroupHandler implements sarama.ConsumerGroupHandler interface.
// ConsumeClaim will be run in a separate goroutine for each topic/partition.
type consumerGroupHandler struct {
	consumer *Consumer
}

func (consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (c consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	logSuffix := fmt.Sprintf(", topic=%q, partition=%d", claim.Topic(), claim.Partition())
	c.consumer.Logger.Println("partition reading" + logSuffix)

	h, _ := c.consumer.handlers.Get(claim.Topic())

	for msg := range claim.Messages() {
		m, attempts := c.handleMsg(msg, h)
		if m == nil {
			break
		}
		// We succeeded in sending the message.
		sess.MarkMessage(msg, "")
		if c.consumer.Metrics != nil {
			c.consumer.Metrics.Report(*m, &Metrics{
				Attempts:        attempts,
				RemainingOffset: claim.HighWaterMarkOffset() - msg.Offset,
			})
		}
	}

	return nil
}

// Stop the consumer.
func (c *Consumer) Stop() error {
	close(c.quit)

	err := c.consumer.Close()
	return errors.Wrap(err, "failed to close the consumer properly")
}

// handleMsg attempts to handle the message. It retries until the
// message can be handled OK or the consumer is stopped.
// It returns the message that was handled and the
// number of attempts that it took, or nil if the consumer was stopped.
func (c consumerGroupHandler) handleMsg(msg *sarama.ConsumerMessage, h HandlerConfig) (*Message, int) {
	attempts := 0
	for a := retry.StartWithCancel(c.consumer.config.retryStrategy, nil, c.consumer.quit); a.Next(); {
		if !a.More() {
			// Note: we're assuming here that the retry
			// strategy does not terminate except when
			// explicitly cancelled.
			return nil, 0
		}
		attempts++

		// If an error occurs during an unformat call, it
		// usually means the chosen converter was the wrong one.
		// The consumption must block and an error must be
		// reported.
		m, err := h.Converter.FromKafka(msg)
		if err != nil {
			c.consumer.Logger.Printf("an error occured while converting a message: %v; retrying", err)
			continue
		}
		if err := h.Handler.HandleMessage(m); err != nil {
			c.consumer.Logger.Printf("an error occured while handling a message: %v; retrying", err)
			continue
		}
		return m, attempts
	}
	return nil, attempts
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

// MetricsReporter is an interface that can be passed to set metrics hook to receive metrics
// from the consumer as it handles messages.
type MetricsReporter interface {
	Report(Message, *Metrics)
}

// Metrics contains information about message consumption for a given partition.
type Metrics struct {
	// Number of times the consumer tried to consume this message.
	Attempts int
	// Best effort information about the remaining unconsumed messages in the partition.
	RemainingOffset int64
}
