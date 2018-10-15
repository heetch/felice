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
