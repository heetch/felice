package consumer_test

import (
	"fmt"
	"testing"

	"github.com/heetch/felice/message"
	"github.com/stretchr/testify/require"
)

// HandleMessageFn can be used as a Handler
func TestHandleMessageFn(t *testing.T) {
	var calls []bool

	var h Handler
	h = HandlerFunc(func(msg *message.Message) error {
		calls = append(calls, true)
		return nil
	})
	m := &message.Message{}
	err := h.HandleMessage(m)
	require.NoError(t, err)
	require.Len(t, calls, 1)
}

type testHandler struct {
	ID int
}

func (th *testHandler) HandleMessage(m *message.Message) error {
	return fmt.Errorf("%d", th.ID)
}

// The Get and Set functions on the Collection type manage a uniqe set
// of associations between Topics and Handlers.
func TestCollectionGetSet(t *testing.T) {

	th1 := &testHandler{ID: 1}
	th2 := &testHandler{ID: 2}

	testCases := []struct {
		Name            string
		Handlers        []map[string]Handler
		TestTopic       string
		ExpectedHandler Handler
		ExpectedOK      bool
	}{
		{
			Name:            "Get from an empty Collection",
			Handlers:        nil,
			TestTopic:       "Shoe",
			ExpectedHandler: nil,
			ExpectedOK:      false,
		},
		{
			Name:            "Set and Get the same Topic's handler",
			Handlers:        []map[string]Handler{{"Shoe": th1}},
			TestTopic:       "Shoe",
			ExpectedHandler: th1,
			ExpectedOK:      true,
		},
		{
			Name: "Overwite topic association",
			Handlers: []map[string]Handler{
				{"Shoe": th1},
				// The association with th2 should
				// overwrite the association with th1.
				{"Shoe": th2},
			},
			TestTopic:       "Shoe",
			ExpectedHandler: th2,
			ExpectedOK:      true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			c := &handler.Collection{}
			// We apply iterations of Topic⇒Handler associations in order
			for _, hi := range tc.Handlers {
				for k, v := range hi {
					c.Set(k, v)
				}
			}
			h, ok := c.Get(tc.TestTopic)
			if tc.ExpectedOK {
				require.True(t, ok)
				msg := &message.Message{}
				require.Equal(t, tc.ExpectedHandler.HandleMessage(msg), h.HandleMessage(msg))
			} else {
				require.False(t, ok)
			}
		})
	}
}

// The Collection.Topics function returns the set of all topics with
// handlers in the Collection.
func TestCollectionTopics(t *testing.T) {
	testCases := []struct {
		Name        string
		Topics      []string
		Expectation []string
	}{
		{
			Name:        "No topics registered",
			Topics:      nil,
			Expectation: []string{},
		},
		{
			Name:        "One topic",
			Topics:      []string{"Shoe"},
			Expectation: []string{"Shoe"},
		},
		{
			Name:        "Multiple topics",
			Topics:      []string{"Shoe", "Fruit"},
			Expectation: []string{"Shoe", "Fruit"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			c := &handler.Collection{}
			for _, t := range tc.Topics {
				c.Set(t, &testHandler{})
			}
			result := c.Topics()
			require.ElementsMatch(t, result, tc.Expectation)
		})
	}
}
