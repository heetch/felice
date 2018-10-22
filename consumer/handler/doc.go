// The handler package defines the mechanisms by which a recieved
// message can be handled, and how handlers themselves can be managed.
// The most obvious way to use the handler package is via the consumer
// package, but this isn't an absolute requirement.  We do however
// require that the messages handled are felice's message.Message
// type.
//
// The Handler interface defines the signature for all felice
// Handlers.  There are two common ways to comply with this interface.
// The first is simply to create a type with the HandleMessage
// function:
//
//    type MyFooHandler struct { }
//
//    func (mfh MyFooHandler) HandleMessage(msg *message.Message) error {
//        fmt.Printf("%+v", *msg)
//    }
//
// This approach has the advantage of not actually requiring you to
// import the handler package when defining handlers for use with the
// conusmer.Consumer.
//
// The second approach is to cast a function to the HandlerFunc type
// defined in this package:
//
//    h := handler.HandlerFunc(func(msg *message.Message) error {
//        fmt.Printf("%+v", *msg)
//    })
//
// Handlers can also be managed in Collections.  The Collection struct
// will allow exactly one handler to be associated with each topic,
// and will, on demand return a list of all topics for which a handler
// has been registered.  For example:
//
//    col := handler.Collection{}
//
//    col.Set("my-topic", handler.HandlerFunc(func(msg *message.Message)) {
//        fmt.Printf("Got message on my-topic: %+v", *msg)
//    })
//
//    col.Set("your-topic", handler.HandlerFunc(func(msg *message.Message)) {
//        fmt.Printf("Got message on your-topic: %+v", msg)
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
package handler
