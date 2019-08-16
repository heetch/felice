/*
Package consumer is Felice's primary entrance point for receiving messages
from a Kafka cluster.

The best way to create a Consumer is using the New function.  You have
to pass a non nil Config otherwise New will return an error.

Note: NewConfig returns a Config with sane defaults.

   c, err := consumer.New(consumer.NewConfig("client-id"))

Once you've constructed a consumer you must add message handlers to it.
This is done by calling the Consumer.Handle method.  Each time you call
Handle you'll pass a topic name, a message converter and a type that
implements the Handler interface.  There can only ever be one handler
associated with a topic so, if you call Handle multiple times with the
same topic, only the final handler will be registered.

The Handler interface defines the signature for felice message handlers.

	type myHandler struct {}

	// HandleMessage implements consumer.MessageHandler.
	func (h myHandler) HandleMessage(ctx context.Context, m *consumer.Message) error {
		// Do something of your choice here!
		return nil // .. or return an actual error.
	}

	c.Handle("testmsg", consumer.MessageConverterV1(nil),  myHandler{})

Once you've registered all your handlers you should call Consumer.Serve.

Serve will start consuming messages and pass them to their per-topic
handlers. Serve itself will block until Consumer.Close is called.
When Serve terminates it will return an error, which will be nil under
normal circumstances.

Note that any calls to Consumer.Handle after Consumer.Serve has been
called will panic.

Handlers are responsible for doing any appropriate retries (for example because
of a temporary network outage). When doing this, a handler should return as soon
as possible if the context is cancelled. This will happen when the Consumer is shutting down.

For example:

	type FooHandler struct { }

	func (FooHandler) HandleMessage(ctx context.Context, m *Message) error {
		tryCount := 0
		for {
			tryCount++
			err := tryAction(ctx, m)
			if err == nil || tryCount > 10 {
				return err
			}
			// Wait a while before trying again.
			select{
			case <-time.After(time.Second):
			case <-ctx.Done():
				// The context has been cancelled.
				return ctx.Err()
			}
		}
	}
*/
package consumer
