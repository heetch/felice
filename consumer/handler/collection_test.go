package handler_test

import (
	"fmt"
	"testing"

	"github.com/heetch/felice/consumer/handler"
	"github.com/heetch/felice/message"
	"github.com/stretchr/testify/require"
)

type testHandler struct {
	ID int
}

func (th *testHandler) HandleMessage(m *message.Message) error {
	return fmt.Errorf("%d", th.ID)
}

// The Get and Set functions on the Collection type manage a uniqe set
// of associations between Topics and Handlers.
func TestCollectionGetSet(t *testing.T) {
	type testCase struct {
		Topic   string
		Handler handler.Handler
		OK      bool
	}

	th1 := &testHandler{ID: 1}

	setups := []struct {
		Name     string
		Handlers map[string]handler.Handler
		Tests    []testCase
	}{
		{
			Name:     "Get from an empty Collection",
			Handlers: make(map[string]handler.Handler),
			Tests:    []testCase{{Topic: "Shoe", Handler: nil, OK: false}},
		},
		{
			Name:     "Set and Get the same Topic's handler",
			Handlers: map[string]handler.Handler{"Shoe": th1},
			Tests:    []testCase{{Topic: "Shoe", Handler: th1, OK: true}},
		},
	}

	for _, s := range setups {
		t.Run(s.Name, func(t *testing.T) {
			c := &handler.Collection{}
			for k, v := range s.Handlers {
				c.Set(k, v)
			}
			for _, tc := range s.Tests {
				h, ok := c.Get(tc.Topic)
				if tc.OK {
					require.True(t, ok)
					msg := &message.Message{}
					require.Equal(t, tc.Handler.HandleMessage(msg), h.HandleMessage(msg))
				} else {
					require.False(t, tc.OK)
				}
			}

		})
	}

}
