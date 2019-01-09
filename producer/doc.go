// Package producer provides types for producing messages to Kafka.
// Felice provides a generic way of defining messages that is not
// tied to the way this message is sent to Kafka.
// In Felice, a message contains a body and may contain a key and headers.
// The way these this information is sent to Kafka is function of the MessageConverter
// used.
// Some MessageConverter could decide to send headers using the Kafka headers feature
// and encode the body using JSON, while others might want to wrap everything, headers and body, into
// an Avro message and send everything as the Kafka value.
// This allows to decouple business logic from the convention used to format messages
// so it is easy to change the format without changing too much code.
//
// Producers require a valid configuration to be able to run properly.
// The Config type allows to define the client id and converter by also to customize
// Sarama's behaviour.
package producer
