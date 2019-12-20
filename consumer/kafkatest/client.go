// Package kafkatest provides a package intended for running tests
// that require a Kafka backend.
package kafkatest

import (
	"crypto/rand"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"gopkg.in/retry.v1"
)

var ErrDisabled = fmt.Errorf("kafka tests are disabled")

// New connects to a Kafka instance and returns a Kafka
// instance that uses it.
//
// The following environment variables can be used to
// configure the connection parameters:
//
//	- $KAFKA_DISABLE
//		A boolean as parsed by strconv.ParseBool. If this is true,
//		New will return ErrDisabled.
//	- $KAFKA_ADDRS
//		A comma-separate list of Kafka broker addresses in host:port
//		form. If this is empty, localhost:9092 will be used.
//		The list of address can be discovered by calling Client.Addrs.
//	- $KAFKA_USERNAME, $KAFKA_PASWORD
//		The username and password to use for SASL authentication.
//		When $KAFKA_USERNAME is non-empty, SASL will be
//		enabled.
//	- $KAFKA_USE_TLS
//		A boolean as parsed by strconv.ParseBool. If this
//		is true, a secure TLS connection will be used.
//
//	- $KAFKA_TIMEOUT
//		The maximum duration to wait when trying to connect
//		to Kakfa. Defaults to "30s".
//
// The returned Kafka instance must be closed after use.
func New() (*Kafka, error) {
	disabled, err := boolVar("KAFKA_DISABLE")
	if err != nil {
		return nil, fmt.Errorf("bad value for $KAFKA_DISABLE: %v", err)
	}
	if disabled {
		return nil, ErrDisabled
	}
	addrsStr := os.Getenv("KAFKA_ADDRS")
	if addrsStr == "" {
		addrsStr = "localhost:9092"
	}
	addrs := strings.Split(addrsStr, ",")
	useTLS, err := boolVar("KAFKA_USE_TLS")
	if err != nil {
		return nil, fmt.Errorf("bad value for KAFKA_USE_TLS: %v", err)
	}
	client := &Kafka{
		addrs:        addrs,
		useTLS:       useTLS,
		saslUser:     os.Getenv("KAFKA_USERNAME"),
		saslPassword: os.Getenv("KAFKA_PASSWORD"),
	}
	// The cluster might not be available immediately, so try
	// for a while before giving up.
	retryLimit := 30 * time.Second
	if limit := os.Getenv("KAFKA_TIMEOUT"); limit != "" {
		retryLimit, err = time.ParseDuration(limit)
		if err != nil {
			return nil, fmt.Errorf("bad value for KAFKA_TIMEOUT: %v", err)
		}
	}
	retryStrategy := retry.LimitTime(retryLimit, retry.Exponential{
		Initial:  time.Millisecond,
		MaxDelay: time.Second,
	})
	t0 := time.Now()
	for a := retry.Start(retryStrategy, nil); a.Next(); {
		admin, err := sarama.NewClusterAdmin(addrs, client.Config())
		if err == nil {
			log.Printf("successfully connected to admin after %v", time.Since(t0))
			client.admin = admin
			break
		}
		if !a.Next() {
			return nil, fmt.Errorf("cannot connect to Kafka cluster at %q after %v: %v", addrs, retryLimit, err)
		}
	}
	return client, nil
}

// Kafka represents a connection to a Kafka cluster.
type Kafka struct {
	addrs        []string
	useTLS       bool
	saslUser     string
	saslPassword string
	admin        sarama.ClusterAdmin
	topics       []string
}

// Config returns a sarama configuration that will
// use connection parameters defined in the environment
// variables described in New.
func (k *Kafka) Config() *sarama.Config {
	cfg := sarama.NewConfig()
	k.InitConfig(cfg)
	return cfg
}

// InitConfig is similar to Config, except that instead of
// returning a new configuration, it configures an existing
// one.
func (k *Kafka) InitConfig(cfg *sarama.Config) {
	if cfg.Version == sarama.MinVersion {
		// R
		cfg.Version = sarama.V1_0_0_0
	}
	cfg.Net.TLS.Enable = k.useTLS
	if k.saslUser != "" {
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.User = k.saslUser
		cfg.Net.SASL.Password = k.saslPassword
	}
}

// Addrs returns the configured Kakfa broker addresses.
func (k *Kafka) Addrs() []string {
	return k.addrs
}

// NewTopic creates a new Kafka topic with a random name and
// single partition. It returns the topic's name. The topic will be deleted
// when c.Close is called.
//
// NewTopic panics if the topic cannot be created.
func (k *Kafka) NewTopic() string {
	if k.admin == nil {
		panic("cannot create topic with closed kafkatest.Kafka instance")
	}
	topic := randomName("kafkatest-")
	if err := k.admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false); err != nil {
		panic(fmt.Errorf("cannot create topic %q: %v", topic, err))
	}
	k.topics = append(k.topics, topic)
	return topic
}

// Close closes the client connection and removes any topics
// created by Topic. This method may be called more than once.
func (k *Kafka) Close() error {
	if k.admin == nil {
		return nil
	}
	for ; len(k.topics) != 0; k.topics = k.topics[1:] {
		if err := k.admin.DeleteTopic(k.topics[0]); err != nil {
			return fmt.Errorf("cannot delete topic %q: %v", k.topics[0], err)
		}
	}
	k.admin.Close()
	k.admin = nil
	return nil
}

func boolVar(envVar string) (bool, error) {
	s := os.Getenv(envVar)
	if s == "" {
		return false, nil
	}
	b, err := strconv.ParseBool(s)
	if err != nil {
		return false, fmt.Errorf("invalid boolean value %q (possible values are: 1, t, T, TRUE, true, True, 0, f, F, FALSE)", s)
	}
	return b, nil
}

func randomName(prefix string) string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%s%x", prefix, buf)
}
