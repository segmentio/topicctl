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
	connector, err := admin.NewConnector(admin.ConnectorConfig{
		BrokerAddr: util.TestKafkaAddr(),
	})
	require.NoError(t, err)

	topicName := util.RandomString("topic-bounds-", 6)
	_, err = connector.KafkaClient.CreateTopics(
		ctx,
		&kafka.CreateTopicsRequest{
			Topics: []kafka.TopicConfig{
				{
					Topic:             topicName,
					NumPartitions:     4,
					ReplicationFactor: 1,
				},
			},
		},
	)
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	writer := kafka.NewWriter(
		kafka.WriterConfig{
			Brokers:  []string{connector.Config.BrokerAddr},
			Dialer:   connector.Dialer,
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

	bounds, err := GetAllPartitionBounds(ctx, connector, topicName, nil)
	assert.NoError(t, err)

	// The first partition gets 3 messages. (i.e) earliest/first offset is 0 and latest/last is 3
	assert.Equal(t, 4, len(bounds))
	assert.Equal(t, 0, bounds[0].Partition)
	assert.Equal(t, int64(0), bounds[0].FirstOffset)
	assert.Equal(t, int64(3), bounds[0].LastOffset)

	// The last partition gets only 2 messages. (i.e) earliest/first offset is 0 and latest/last is 2
	assert.Equal(t, 3, bounds[3].Partition)
	assert.Equal(t, int64(0), bounds[3].FirstOffset)
	assert.Equal(t, int64(2), bounds[3].LastOffset)

	boundsWithOffsets, err := GetAllPartitionBounds(
		ctx,
		connector,
		topicName,
		map[int]int64{
			0: 1,
		},
	)
	assert.NoError(t, err)

	assert.Equal(t, 4, len(boundsWithOffsets))

	// Start of first partition is moved forward. First partition has earliest offset is 0 and latest is 3
	assert.Equal(t, 0, boundsWithOffsets[0].Partition)
	assert.Equal(t, int64(1), boundsWithOffsets[0].FirstOffset)
	assert.Equal(t, int64(3), boundsWithOffsets[0].LastOffset)

	// Other partition bounds are unchanged. Last partition has earliest offset is 0 and latest is 2
	assert.Equal(t, 3, boundsWithOffsets[3].Partition)
	assert.Equal(t, int64(0), boundsWithOffsets[3].FirstOffset)
	assert.Equal(t, int64(2), boundsWithOffsets[3].LastOffset)
}
