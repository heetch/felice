![](https://raw.githubusercontent.com/heetch/felice/master/felice.png)

# Felice
[![Test](https://github.com/heetch/felice/workflows/Test/badge.svg)](https://github.com/heetch/felice/actions)
[![Documentation](https://godoc.org/github.com/heetch/felice?status.svg)](http://godoc.org/github.com/heetch/felice) 

## What is Felice?
Felice is a nascent, opinionated Kafka library for Go, in Go.

Currently, you can use Felice to send and consume messages via Kafka topics.

## Why "Felice"?
Felice Bauer was, at one time, Franz Kafka's fiance.  He wrote her many messages, which she faithfuly kept, and later published.

## Where should I start?
If you wish to send messages via Kafka, you should start by reading
the documentation for the `producer` package.  If you wish to consume
messages from Kafka, you should start by reading the documentation for
the `consumer` package.  The `message` package contains the `Message` type that is
shared by both `consumer` and `producer` code.

## Default configuration

Felice uses the following default configuration:

### Producer

* It uses `murmur2` partitioner to be compatible with JVM ecosystem, specially KStreams
* A maximum of 3 attempts are made to produce a message before an error is returned
* It waits for all in-sync replicas to ack the message

### Consumer

* It distributes load across partitions using round robin strategy
