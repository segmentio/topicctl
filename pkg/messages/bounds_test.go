package messages

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetAllPartitionBounds(t *testing.T) {
	ctx := context.Background()
	controllerConn := util.TestKafkaContollerConn(ctx, t)
	defer controllerConn.Close()

	topicName := util.RandomString("topic-bounds-", 6)

	err := controllerConn.CreateTopics(
		kafka.TopicConfig{
			Topic:             topicName,
			NumPartitions:     4,
			ReplicationFactor: 1,
		},
	)
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	writer := kafka.NewWriter(
		kafka.WriterConfig{
			Brokers:  []string{util.TestKafkaAddr()},
			Topic:    topicName,
			Balancer: &kafka.RoundRobin{},
		},
	)
	defer writer.Close()

	messages := []kafka.Message{}

	for i := 0; i < 10; i++ {
		messages = append(
			messages,
			kafka.Message{
				Key:   []byte(fmt.Sprintf("key%d", i)),
				Value: []byte(fmt.Sprintf("value%d", i)),
			},
		)
	}

	err = writer.WriteMessages(ctx, messages...)
	require.NoError(t, err)

	brokerConnector, err := admin.NewBrokerConnector(
		admin.BrokerConnectorConfig{
			BrokerAddr: util.TestKafkaAddr(),
		},
	)
	require.NoError(t, err)

	bounds, err := GetAllPartitionBounds(ctx, brokerConnector, topicName, nil)
	assert.Nil(t, err)

	// The first partition gets 3 messages
	assert.Equal(t, 4, len(bounds))
	assert.Equal(t, 0, bounds[0].Partition)
	assert.Equal(t, int64(0), bounds[0].FirstOffset)
	assert.Equal(t, int64(2), bounds[0].LastOffset)

	// The last partition gets only 2 messages
	assert.Equal(t, 3, bounds[3].Partition)
	assert.Equal(t, int64(0), bounds[3].FirstOffset)
	assert.Equal(t, int64(1), bounds[3].LastOffset)

	boundsWithOffsets, err := GetAllPartitionBounds(
		ctx,
		brokerConnector,
		topicName,
		map[int]int64{
			0: 1,
		},
	)
	assert.Nil(t, err)

	assert.Equal(t, 4, len(boundsWithOffsets))

	// Start of first partition is moved forward
	assert.Equal(t, 0, boundsWithOffsets[0].Partition)
	assert.Equal(t, int64(1), boundsWithOffsets[0].FirstOffset)
	assert.Equal(t, int64(2), boundsWithOffsets[0].LastOffset)

	// Other partition bounds are unchanged
	assert.Equal(t, 3, boundsWithOffsets[3].Partition)
	assert.Equal(t, int64(0), boundsWithOffsets[3].FirstOffset)
	assert.Equal(t, int64(1), boundsWithOffsets[3].LastOffset)
}
