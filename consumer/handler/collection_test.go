package handler_test

import (
	"testing"

	"github.com/heetch/felice/consumer/handler"
	"github.com/stretchr/testify/require"
)

// The Get and Set functions on the Collection type manage a uniqe set
// of associations between Topics and Handlers.
func TestCollectionGetSet(t *testing.T) {
	type testcase struct {
		Topic   string
		Handler handler.Handler
		OK      bool
	}
	setups := []struct {
		Name     string
		Handlers map[string]handler.Handler
		Tests    []testcase
	}{
		{
			Name:     "Get from an empty Collection",
			Handlers: make(map[string]handler.Handler),
			Tests:    []testcase{{Topic: "Shoe", Handler: nil, OK: false}},
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
					require.Equal(t, tc.Handler, h)
				} else {
					require.False(t, tc.OK)
				}
			}

		})
	}

}
