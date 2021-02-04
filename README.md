![](https://raw.githubusercontent.com/heetch/felice/master/felice.png)

# Felice
![Test](https://github.com/heetch/felice/workflows/Test/badge.svg)
[![Documentation](https://godoc.org/github.com/heetch/felice?status.svg)](http://godoc.org/github.com/heetch/felice) 
## What is Felice?
Felice is a nascent, opinionated Kafka library for Go, in Go.

Currently you can use Felice to send and consume messages via Kafka topics.  We intend to add more advanced features shortly.

## Can I use Felice?
Felice is very much a work in progress.  As of 06th Demember 2018 Felice's most basic message sending and consuming functions are ready for use, but we are not yet ready to rule out future changes to the public interfaces of the code. proceed with caution.

## Why "Felice"?
Felice Bauer was, at one time, Franz Kafka's fiance.  He wrote her many messages, which she faithfuly kept, and later published.

## Where should I start?
If you wish to send messages via Kafka, you should start by reading
the documentation for the `producer` package.  If you wish to consume
messages from Kafka, you should start by reading the documentation for
the `consumer` package.  The `message` package contains the `Message` type that is
shared by both `consumer` and `producer` code.

