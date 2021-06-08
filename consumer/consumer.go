package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"

	"github.com/heetch/felice/v2/codec"
)

// Consumer represents a Kafka message consumer.
// See the package documentation and New for
// information on how to use it.
type Consumer struct {
	config Config

	// mu guards the fields below.
	mu sync.Mutex

	// quit is closed when the consumer is closed.
	quit chan struct{}

	// consumerGroup holds the consumerGroup started by Serve.
	consumerGroup sarama.ConsumerGroup

	handlers handlers
}

// New returns a new consumer with the given configuration.
// Use the Handle method to register handlers for all subscribed
// topics, then call Serve to begin consuming messages.
//
// The Consumer must be closed with the Close method after use.
func New(cfg Config) (*Consumer, error) {
	err := cfg.Validate()
	if err != nil {
		return nil, err
	}
	return &Consumer{
		config: cfg,
		quit:   make(chan struct{}),
	}, nil
}

// Handle registers the handler for the given topic.
// It will panic if it is called after Serve or the topic
// is empty.
func (c *Consumer) Handle(topic string, converter MessageConverter, h Handler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.consumerGroup != nil {
		panic("Consumer.Handle called after Consumer.Serve")
	}
	c.handlers.add(&topicHandler{
		topic:     topic,
		handler:   h,
		converter: converter,
	})
}

// Serve runs the consumer and listens for new messages on the given
// topics. Serve will block until it is instructed to stop, which you
// can achieve by calling Close.
//
// Serve will return an error if it is called more than once,
// if the Consumer has been closed, or if no handlers have
// been registered, or if the consumer failed with an error.
//
// It will not return an error if Close was called and the consumer shut
// down cleanly.
func (c *Consumer) Serve(ctx context.Context) error {
	// Note that from here, c.handlers can't change because
	// Consumer.Handle will panic, so there's no need to
	// access it within the mutex.
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		<-c.quit
		cancel()
	}()
	for {
		select {
		case <-c.quit:
			return nil
		default:
		}
		if err := c.createConsumerGroup(); err != nil {
			return err
		}
		err := c.consumerGroup.Consume(ctx, c.handlers.topics(), consumerGroupHandler{
			consumer: c,
		})
		if err != nil {
			if err == sarama.ErrClosedConsumerGroup {
				// This can happen when the Consumer has been closed.
				err = nil
			}
			return err
		}
		c.mu.Lock()
		c.consumerGroup = nil
		c.mu.Unlock()
	}
}

// createConsumerGroup sets c.consumerGroup to a newly created
// sarama.ConsumerGroup instance.
func (c *Consumer) createConsumerGroup() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.consumerGroup != nil {
		return errors.New("Consumer.Serve called twice")
	}
	groupID := c.config.ClientID + "-consumer-group"
	cg, err := sarama.NewConsumerGroup(c.config.KafkaAddrs, groupID, c.config.Config)
	if err != nil {
		if err == sarama.ErrConsumerCoordinatorNotAvailable {
			// We'll add some additional context
			// information. This issue comes from the
			// RefreshCoordinator call inside sarama, but
			// it's not reporting the root cause, sadly.
			//
			// Even with this information it's rather an
			// annoying little issue.
			err = errors.Wrap(err, "__consumer_offsets topic doesn't yet exist, either because no client has yet requested an offset, or because this consumer group is not yet functioning at startup or after rebalancing.")
		}
		return errors.Wrap(err, fmt.Sprintf("failed to create consumer group %q", groupID))
	}
	c.consumerGroup = cg
	return nil
}

// consumerGroupHandler implements sarama.ConsumerGroupHandler
// interface. ConsumeClaim will be run in a separate goroutine for each
// topic/partition.
type consumerGroupHandler struct {
	consumer *Consumer
}

func (consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := sess.Context()
	// Note that the get should always return a non-nil topicHandler
	// because the consumer group won't call ConsumeClaim with
	// a topic that wasn't explicitly handed to NewConsumerGroup
	// and it's not possible to remove a topic from the handler list.
	h := c.consumer.handlers.get(claim.Topic())
	for msg := range claim.Messages() {
		mark := true
		if err := h.handleMessage(ctx, msg, claim); err != nil {
			if c.consumer.config.Discarded != nil {
				mark = c.consumer.config.Discarded(ctx, msg, err)
			}
		}
		if ctx.Err() != nil {
			// The ConsumerGroupHandler contract requires us
			// to consume all the messages, so do that
			// without invoking the handler.
			for range claim.Messages() {
			}
			break
		}
		if mark {
			sess.MarkMessage(msg, "")
		}
	}
	return nil
}

// Close closes the consumer and synchronously shuts down
// the running Serve call if any, which will return with no error.
func (c *Consumer) Close() error {
	c.mu.Lock()
	select {
	case <-c.quit:
		// Close has already been called.
		return nil
	default:
	}
	close(c.quit)
	cg := c.consumerGroup
	c.mu.Unlock()
	if cg != nil {
		if err := cg.Close(); err != nil {
			return errors.Wrap(err, "error closing consumer group")
		}
	}
	return nil
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

	claim sarama.ConsumerGroupClaim
}

// HighWaterMarkOffset returns the high water mark offset of the partition,
// i.e. the offset that will be used for the next message that will be produced.
// You can use this to determine how far behind the processing is.
func (m *Message) HighWaterMarkOffset() int64 {
	return m.claim.HighWaterMarkOffset()
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
//
// The keyCodec parameter is used to decode partition keys.
// If it's nil, codec.String() will be used.
func MessageConverterV1(keyCodec codec.Codec) MessageConverter {
	if keyCodec == nil {
		keyCodec = codec.String()
	}
	return &messageConverterV1{
		keyCodec: keyCodec,
	}
}

type messageConverterV1 struct {
	keyCodec codec.Codec
}

// FromKafka converts the message from Kafka headers and JSON body.
func (f *messageConverterV1) FromKafka(cm *sarama.ConsumerMessage) (*Message, error) {
	headers := make(map[string]string)
	for _, pair := range cm.Headers {
		headers[string(pair.Key)] = string(pair.Value)
	}
	return &Message{
		Topic:      cm.Topic,
		ID:         headers["Message-Id"],
		Headers:    headers,
		Key:        codec.NewDecoder(f.keyCodec, cm.Key),
		Body:       codec.JSONDecoder(cm.Value),
		Partition:  cm.Partition,
		Offset:     cm.Offset,
		ProducedAt: cm.Timestamp,
	}, nil
}
