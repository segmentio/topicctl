package util

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

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
		//return "169.254.123.123:9092"
		return "localhost:9093"
	}

	return testKafkaAddr
}

// CanTestBrokerAdmin returns whether we can test the broker-only admin client.
func CanTestBrokerAdmin() bool {
	value, ok := os.LookupEnv("KAFKA_TOPICS_TEST_BROKER_ADMIN")
	if ok && value != "" {
		return true
	}

	return false
}

// CanTestBrokerAdminSecurity returns whether we can test the broker-only admin client security features.
func CanTestBrokerAdminSecurity() bool {
	value, ok := os.LookupEnv("KAFKA_TOPICS_TEST_BROKER_ADMIN_SECURITY")
	if ok && value != "" {
		return true
	}

	return false
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

// RetryUntil is a helper that will re-run the argument function multiple times (up to a
// duration limit) until it no longer produces an error.
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
