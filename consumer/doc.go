// Consumer is Felice's primary entrance point for receiving messages
// from a Kafka cluster.
//
// There is no special construction function for the Consumer
// structure as all of its public members are optional, and we shall
// discuss them below.  Thus you construct a Consumer by the normal Go
// means:
//
//    c := felice.Consumer{}
//
// Once you've constructed a consumer you must add message handlers to
// it.  This is done by calling the Consumer.Handle method.  Each time
// you call Handle you'll pass a topic name and a type that implements
// the handler.Handler interface.  Their can only ever be one handler
// associated with a topic so, if you call Handle multiple times with
// the same topic, they will update the handler registered for the
// topic, and only the final one will count.  A typical call to Handle
// looks like this:
//
//    c.Handle("testmsg", handler.HandlerFunc(func(m *message.Message) error {
//        // Do something of your choice here!
//        return nil // .. or return an actual error.
//    }))
//
// Once you've registered all your handlers you may call
// Consumer.Serve. Serve requires a client ID and a slice of strings,
// each of which is the address of a Kafka broker to attempt to
// communicate with. Serve will start a go routine for each partition
// to consume messages and pass them to their per-topic
// handlers. Serve itself will block until Consumer.Stop is called.
// When Serve terminates it will return an error, which will be nil
// under normal circumstances.
// 
// Note that any calls to Consumer.Handle after
// Consumer.Serve has been called will have no effect.
//
// Tweaking the consumer
// --------------------------
// The first public member is the RetryInterval, a time.Duration that
// controls how long the Felice consumer will wait before trying to
// consume a message from Kafka that failed the first time around.
// The default value if 1 second.
//
// The second public member is Metrics.  Metrics stores a type that
// implements the felice.MetricsReporter interface.  If you provide an
// implementation, then it's Report function will be called every time
// a message is successfully handled.  The Report function will
// receive a copy of the message.Message that was handled, along with
// a map[string]string containing metrics about the handling of the
// message.  Currently we pass the following metrics: "attempts" (the
// number of attempts it took before the message was handled);
// "msgOffset" (the marked offset for the message on the topic); and
// "remainingOffset" (the difference between the high water mark and
// the current offset).
package consumer
