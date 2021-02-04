// Package producer provides types for producing messages to Kafka.
// Felice provides a generic way of defining messages that is not
// tied to the way this message is sent to Kafka.
// In Felice, a message contains a body and may contain a key and headers.
// The way this information is sent to Kafka is a function of the MessageConverter
// used.
// Some MessageConverter could decide to send headers using the Kafka headers feature
// and encode the body using JSON, whilst another might want to wrap the headers and body, into
// an Avro message and send everything as the Kafka value.
// This allows decoupling of business logic from the convention used to format messages
// so it is easy to change the format without changing too much code.
//
// Producers require a valid configuration to be able to run properly.
// The Config type allows to define the client id and converter by also to customize
// Sarama's behaviour.
//
// Default configuration:
// * Uses murmur2 partitioner to be compatible with JVM ecosystem, specially KStreams
// * Max retry 3 attemps
// * Wait for all in-sync replicas to ack the message
package producer
