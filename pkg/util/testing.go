package util

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// TestZKAddr returns a zookeeper address for unit testing purposes.
func TestZKAddr() string {
	// Inside docker-compose (i.e., in CI), we need to use a different
	// address
	testZkAddr, ok := os.LookupEnv("KAFKA_TOPICS_TEST_ZK_ADDR")
	if !ok {
		return "localhost:2181"
	}

	return testZkAddr
}

// TestKafkaAddr returns a kafka bootstrap address for unit testing purposes.
func TestKafkaAddr() string {
	// Inside docker-compose (i.e., in CI), we need to use a different
	// address
	testKafkaAddr, ok := os.LookupEnv("KAFKA_TOPICS_TEST_KAFKA_ADDR")
	if !ok {
		return "169.254.123.123:9092"
	}

	return testKafkaAddr
}

// TestKafkaConn returns a kafka-go connection for unit testing purposes.
func TestKafkaConn(ctx context.Context, t *testing.T) *kafka.Conn {
	conn, err := kafka.DefaultDialer.DialContext(ctx, "tcp", TestKafkaAddr())
	require.NoError(t, err)
	return conn
}

// TestKafkaContollerConn returns a kafka-go connection to the cluster controller
// for unit testing purposes.
func TestKafkaContollerConn(ctx context.Context, t *testing.T) *kafka.Conn {
	conn := TestKafkaConn(ctx, t)
	defer conn.Close()

	broker, err := conn.Controller()
	require.NoError(t, err)

	controllerConn, err := kafka.DefaultDialer.DialContext(
		ctx,
		"tcp",
		fmt.Sprintf("%s:%d", broker.Host, broker.Port),
	)

	require.NoError(t, err)
	return controllerConn
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// RandomString returns a random string with the argument length.
//
// Adapted from the example in
// https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-go.
func RandomString(prefix string, length int) string {
	b := make([]rune, length)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return fmt.Sprintf("%s-%s", prefix, string(b))
}

func RetryUntil(t *testing.T, timeout time.Duration, f func() error) {
	sleepTime := 100 * time.Millisecond
	end := time.Now().Add(timeout)
	var err error

	for time.Now().Before(end) {
		time.Sleep(sleepTime)
		sleepTime = sleepTime * 2

		err = f()
		if err == nil {
			return
		}
	}

	require.NoError(t, err)
}
