// Package consumer is Felice's primary entrance point for receiving messages
// from a Kafka cluster.
//
// There is no special construction function for the Consumer
// structure as all of its public members are optional, and we shall
// discuss them below.  Thus you construct a Consumer by the normal Go
// means:
//
//    var c consumer.Consumer
//
// Once you've constructed a consumer you must add message handlers to
// it.  This is done by calling the Consumer.Handle method.  Each time
// you call Handle you'll pass a topic name, a message unformatter and a type that implements
// the Handler interface.  There can only ever be one handler
// associated with a topic so, if you call Handle multiple times with
// the same topic, they will update the handler registered for the
// topic, and only the final one will count.  A typical call to Handle
// looks like this:
//
//    c.Handle("testmsg", consumer.MessageUnformatterV1(), HandlerFunc(func(m *consumer.Message) error {
//        // Do something of your choice here!
//        return nil // .. or return an actual error.
//    }))
//
// Once you've registered all your handlers you may call
// Consumer.Serve. Serve requires a configuration and a slice of strings,
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
// The Handler interface defines the signature for all felice
// Handlers. There are two common ways to comply with this interface.
// The first is simply to create a type with the HandleMessage
// function:
//
//    type MyFooHandler struct { }
//
//    func (mfh MyFooHandler) HandleMessage(msg *consumer.Message) error {
//        fmt.Printf("%+v", *msg)
//    }
//
// The second approach is to cast a function to the HandlerFunc type
// defined in this package:
//
//    h := handler.HandlerFunc(func(msg *consumer.Message) error {
//        fmt.Printf("%+v", *msg)
//    })
//
// Handlers can also be managed in Collections. The Collection struct
// will allow exactly one handler to be associated with each topic,
// and will, on demand return a list of all topics for which a handler
// has been registered. For example:
//
//    col := handler.Collection{}
//
//    col.Set("my-topic", handler.HandlerFunc(func(msg *message.Message) error) {
//        fmt.Printf("Got message on my-topic: %+v", *msg)
//        return nil
//    })
//
//    col.Set("your-topic", handler.HandlerFunc(func(msg *message.Message) error) {
//        fmt.Printf("Got message on your-topic: %+v", msg)
//        return nil
//    })
//
//    yourHandler, ok := col.Get("your-topic")
//    if !ok {
//        fmt.Println("Couldn't find a handler for your-topic")
//    }
//
//    for _, t := range col.Topics {
//        fmt.Printf("We have a handler for: %s", t)
//    }
//
package consumer
