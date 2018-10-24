// The message package contains the Message type. When using
// the felice Producer, Message will be the type you send.
// When using the felice Consumer, you will register handlers that
// receive the Message type.
//
// You can create a new Message by calling New:
//
//    msg := New("my-topic", "simple string message")
//
// The value passed as the 2nd argument can be any Go type that can be
// marshalled with encoding/json.  Two default headers are added to
// all Messages:
//
// - Message-Id : a universally unique ID for the message
// - Produced-At : the current time in the UTC timezone.
//
// New can also be passed zero, one or many additional Options. An
// Option is a function that receives a pointer to the Message and can
// modify it directly prior to it being returned by New. Two
// predefined Options exists in felice: Header and Key.
//
// For example, if you want to create a Message with a custom header
// field you could request it as follows:
//
//    msg := New("my-topic", "cold potatoes ain't hot!", Header("subject", "potatoes"))
//
package message
