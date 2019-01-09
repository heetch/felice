package consumer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// HandleMessageFn can be used as a Handler
func TestHandleMessageFn(t *testing.T) {
	var calls []bool

	var h Handler
	h = HandlerFunc(func(msg *Message) error {
		calls = append(calls, true)
		return nil
	})
	m := &Message{}
	err := h.HandleMessage(m)
	require.NoError(t, err)
	require.Len(t, calls, 1)
}

type collectionTestHandler struct {
	ID int
}

func (th *collectionTestHandler) HandleMessage(m *Message) error {
	return fmt.Errorf("%d", th.ID)
}

// The Get and Set functions on the Collection type manage a unique set
// of associations between Topics and Handlers.
func TestCollectionGetSet(t *testing.T) {

	th1 := &collectionTestHandler{ID: 1}
	th2 := &collectionTestHandler{ID: 2}

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
			c := &collection{}
			// We apply iterations of Topic⇒Handler associations in order
			for _, hi := range tc.Handlers {
				for k, v := range hi {
					c.Set(k, HandlerConfig{v, MessageConverterV1(NewConfig(""))})
				}
			}
			h, ok := c.Get(tc.TestTopic)
			if tc.ExpectedOK {
				require.True(t, ok)
				msg := &Message{}
				require.Equal(t, tc.ExpectedHandler.HandleMessage(msg), h.Handler.HandleMessage(msg))
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
			var c collection
			for _, t := range tc.Topics {
				c.Set(t, HandlerConfig{&collectionTestHandler{}, MessageConverterV1(NewConfig(""))})
			}
			result := c.Topics()
			require.ElementsMatch(t, result, tc.Expectation)
		})
	}
}
