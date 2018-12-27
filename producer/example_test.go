package producer_test

import (
	"github.com/heetch/felice/codec"
	"github.com/heetch/felice/producer"
)

var endpoints []string

func Example() {
	config := producer.NewConfig("some-id", producer.MessageFormatterV1())

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
