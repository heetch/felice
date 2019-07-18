package felice

import (
	"context"
	"fmt"
	"testing"
	
	"github.com/heetch/felice/producer"
	"github.com/heetch/felice/codec"

)


func BenchmarkSendMessage(b *testing.B) {
	config := producer.NewConfig("benchmark", producer.MessageConverterV1())
	prod, err := producer.New(config, "localhost:9092")
	if err != nil {
		panic(err)
	}
	defer prod.Close()
	errors := make([]error, b.N, b.N)
	for n := 0; n < b.N; n++ {
		err = prod.SendMessage(context.Background(),
			&producer.Message{
				Topic: "benchmark.test",
				Key: codec.StringEncoder("some key"),
				Body: "some body",
			})
		if err != nil {
			errors[n] = err
		}
	}
	errCount := make(map[string]int)
	var count int
	var msg string
	for _, e := range errors {
		if e != nil {
			msg = e.Error()
			count = errCount[msg]
			count++
			errCount[msg] = count
		}

	}
	output := "\n"
	for m, c := range errCount {
		output += fmt.Sprintf("|| %d\t\t: \"%s\"\n", c, m)
	}
	b.Log(output)
}
