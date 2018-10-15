package consumer

import (
	"sync"
	"time"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/heetch/felice/consumer/handler"
)

type Consumer struct {
	consumer      *cluster.Consumer
	config        *cluster.Config
	handlers      *handler.Collection
	wg            sync.WaitGroup
	quit          chan struct{}
	RetryInterval time.Duration
}

// Handle registers the handler for the given topic.
// Handle must be called before Serve.
func (c *Consumer) Handle(topic string, h handler.Handler) {
	c.setup()

	c.handlers.Set(topic, h)
}

func (c *Consumer) setup() {
	if c.handlers == nil {
		c.handlers = &handler.Collection{}
	}

	if c.quit == nil {
		c.quit = make(chan struct{})
	}

	if c.RetryInterval == 0 {
		c.RetryInterval = time.Second
	}
}
