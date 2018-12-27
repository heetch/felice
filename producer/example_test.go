package producer_test

import (
	"github.com/Shopify/sarama"
	"github.com/heetch/felice/codec"
	"github.com/heetch/felice/producer"
)

var endpoints []string

func Example() {
	config := producer.NewConfig("some-id", producer.MessageConverterV1())

	p, err := producer.New(config, endpoints...)
	if err != nil {
		panic(err)
	}
	defer p.Close()

	err = p.SendMessage(&producer.Message{
		Topic: "some topic",
		Key:   codec.StringEncoder("some key"),
		Body:  "some body",
	})
	if err != nil {
		panic(err)
	}
}

type customConverter struct{}

func (customConverter) Convert(*producer.Message) (*sarama.ProducerMessage, error) {
	return nil, nil
}

func ExampleNewFrom() {
	config := producer.NewConfig("some-id", producer.MessageConverterV1())

	p1, err := producer.New(config, endpoints...)
	if err != nil {
		panic(err)
	}
	defer p1.Close()

	config = producer.NewConfig("some-id", new(customConverter))
	p2, err := producer.NewFrom(p1.SyncProducer, config)
	if err != nil {
		panic(err)
	}

	err = p2.SendMessage(&producer.Message{
		Topic: "some topic",
		Key:   codec.StringEncoder("some key"),
		Body:  "some body",
	})
	if err != nil {
		panic(err)
	}
}
