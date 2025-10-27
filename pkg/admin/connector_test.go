package admin

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/createtopics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConnectorDefaultTimeout(t *testing.T) {
	originalTimeout := kafka.DefaultDialer.Timeout
	t.Cleanup(func() { kafka.DefaultDialer.Timeout = originalTimeout })

	connector, err := NewConnector(
		ConnectorConfig{
			BrokerAddr: "localhost:9092",
		},
	)
	require.NoError(t, err)

	assert.Equal(t, 10*time.Second, connector.Dialer.Timeout)
	transport, ok := connector.KafkaClient.Transport.(*kafka.Transport)
	require.True(t, ok)
	assert.Equal(t, 10*time.Second, transport.DialTimeout)
	assert.Equal(t, 10*time.Second, connector.KafkaClient.Timeout)
}

func TestNewConnectorCustomTimeout(t *testing.T) {
	customTimeout := 3 * time.Second

	connector, err := NewConnector(
		ConnectorConfig{
			BrokerAddr:  "localhost:9092",
			ConnTimeout: customTimeout,
			TLS: TLSConfig{
				Enabled:    true,
				SkipVerify: true,
			},
		},
	)
	require.NoError(t, err)

	assert.Equal(t, customTimeout, connector.Dialer.Timeout)
	assert.NotNil(t, connector.Dialer.TLS)
	transport, ok := connector.KafkaClient.Transport.(*kafka.Transport)
	require.True(t, ok)
	assert.Equal(t, customTimeout, transport.DialTimeout)
	assert.Equal(t, customTimeout, connector.KafkaClient.Timeout)
}

func TestConnectorDialerTimeoutHappyPath(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	acceptErrCh := make(chan error)
	go func() {
		defer close(acceptErrCh)
		conn, err := listener.Accept()
		if err != nil {
			acceptErrCh <- err
			return
		}
		if err := conn.Close(); err != nil {
			acceptErrCh <- err
		}
	}()

	connector, err := NewConnector(
		ConnectorConfig{
			BrokerAddr:  listener.Addr().String(),
			ConnTimeout: 100 * time.Millisecond,
		},
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	conn, err := connector.Dialer.DialContext(ctx, "tcp", listener.Addr().String())
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	select {
	case err, ok := <-acceptErrCh:
		if ok {
			require.NoError(t, err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for listener accept")
	}
}

func TestConnectorDialerTimeoutUnhappyPath(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	connector, err := NewConnector(
		ConnectorConfig{
			BrokerAddr:  listener.Addr().String(),
			ConnTimeout: time.Nanosecond,
		},
	)
	require.NoError(t, err)

	_, err = connector.Dialer.DialContext(t.Context(), "tcp", listener.Addr().String())
	require.Error(t, err)

	var netErr net.Error
	if errors.As(err, &netErr) {
		require.True(t, netErr.Timeout(), "expected timeout error, got: %v", err)
		return
	}

	require.True(t, errors.Is(err, context.DeadlineExceeded), "expected deadline exceeded, got: %v", err)
}

type captureTransport struct {
	t                 *testing.T
	expectedTimeoutMs int32
}

func (c *captureTransport) RoundTrip(_ context.Context, _ net.Addr, req protocol.Message) (protocol.Message, error) {
	createReq, ok := req.(*createtopics.Request)
	require.True(c.t, ok)
	require.Equal(c.t, c.expectedTimeoutMs, createReq.TimeoutMs)

	return &createtopics.Response{}, nil
}
