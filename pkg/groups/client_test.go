package groups

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetGroups(t *testing.T) {
	ctx := context.Background()
	topicName := createTestTopic(t, ctx)
	groupID := fmt.Sprintf("test-group-%s", topicName)

	reader := kafka.NewReader(
		kafka.ReaderConfig{
			Brokers:  []string{util.TestKafkaAddr()},
			GroupID:  groupID,
			Topic:    topicName,
			MinBytes: 50,
			MaxBytes: 10000,
		},
	)

	readerCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	for i := 0; i < 8; i++ {
		_, err := reader.ReadMessage(readerCtx)
		require.Nil(t, err)
	}

	client := NewClient(util.TestKafkaAddr())
	groups, err := client.GetGroups(ctx)
	require.Nil(t, err)

	// There could be older groups in here, just ignore them
	assert.GreaterOrEqual(t, len(groups), 1)

	var match bool

	for _, group := range groups {
		if group.GroupID == groupID {
			match = true
			break
		}
	}
	require.True(t, match)

	groupDetails, err := client.GetGroupDetails(ctx, groupID)
	require.Nil(t, err)
	assert.Equal(t, groupID, groupDetails.GroupID)
	assert.Equal(t, "Stable", groupDetails.State)
	assert.Equal(t, 1, len(groupDetails.Members))
	require.Equal(t, 1, len(groupDetails.Members))

	groupPartitions := groupDetails.Members[0].TopicPartitions[topicName]

	assert.ElementsMatch(
		t,
		[]int{0, 1},
		groupPartitions,
	)
}

func TestGetLags(t *testing.T) {
	ctx := context.Background()
	topicName := createTestTopic(t, ctx)
	groupID := fmt.Sprintf("test-group-%s", topicName)

	reader := kafka.NewReader(
		kafka.ReaderConfig{
			Brokers:  []string{util.TestKafkaAddr()},
			GroupID:  groupID,
			Topic:    topicName,
			MinBytes: 50,
			MaxBytes: 10000,
		},
	)

	readerCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	for i := 0; i < 3; i++ {
		_, err := reader.ReadMessage(readerCtx)
		require.Nil(t, err)
	}

	client := NewClient(util.TestKafkaAddr())
	lags, err := client.GetMemberLags(ctx, topicName, groupID)
	require.Nil(t, err)
	require.Equal(t, 2, len(lags))

	for l, lag := range lags {
		assert.Equal(t, l, lag.Partition)
		assert.Equal(t, int64(4), lag.NewestOffset)
		assert.LessOrEqual(t, lag.MemberOffset, int64(4))
	}
}

func createTestTopic(t *testing.T, ctx context.Context) string {
	controllerConn := util.TestKafkaContollerConn(t, ctx)
	defer controllerConn.Close()

	topicName := util.RandomString("topic-groups-", 6)

	err := controllerConn.CreateTopics(
		kafka.TopicConfig{
			Topic:             topicName,
			NumPartitions:     2,
			ReplicationFactor: 1,
		},
	)
	require.Nil(t, err)
	time.Sleep(200 * time.Millisecond)

	writer := kafka.NewWriter(
		kafka.WriterConfig{
			Brokers: []string{util.TestKafkaAddr()},
			Topic:   topicName,
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
	require.Nil(t, err)

	return topicName
}
