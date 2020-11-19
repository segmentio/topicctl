package messages

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

func TestTailerGetMessages(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	controllerConn := util.TestKafkaContollerConn(ctx, t)

	topicName := util.RandomString("topic-tail-", 6)

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

	tailer := NewTopicTailer(
		util.TestKafkaAddr(),
		topicName,
		[]int{0, 1, 2, 3},
		kafka.FirstOffset,
		1,
		1000,
	)
	messagesChan := make(chan TailMessage)
	tailer.GetMessages(ctx, messagesChan)

	timer := time.NewTimer(5 * time.Second)

	messageCount := 0
	seenKeys := map[string]struct{}{}

outerLoop:
	for {
		select {
		case message := <-messagesChan:
			assert.Nil(t, message.Err)
			seenKeys[string(message.Message.Key)] = struct{}{}
			messageCount++

			if messageCount == 10 {
				break outerLoop
			}
		case <-timer.C:
			break outerLoop
		}
	}

	assert.Equal(t, 10, len(seenKeys))
}
