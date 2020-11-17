package admin

import (
	"context"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBrokerClientUpdateTopicConfig(t *testing.T) {
	ctx := context.Background()
	client := NewBrokerAdminClient(util.TestKafkaAddr(), false)
	topicName := util.RandomString("topic-create-", 6)

	err := client.CreateTopic(
		ctx,
		kafka.TopicConfig{
			Topic:             topicName,
			NumPartitions:     -1,
			ReplicationFactor: -1,
			ReplicaAssignments: []kafka.ReplicaAssignment{
				{
					Partition: 0,
					Replicas:  []int{0, 1},
				},
				{
					Partition: 1,
					Replicas:  []int{2, 3},
				},
				{
					Partition: 2,
					Replicas:  []int{3, 4},
				},
			},
			ConfigEntries: []kafka.ConfigEntry{
				{
					ConfigName:  "flush.ms",
					ConfigValue: "2000",
				},
				{
					ConfigName:  "retention.ms",
					ConfigValue: "10000000",
				},
			},
		},
	)
	require.NoError(t, err)

	topicInfo, err := client.GetTopic(ctx, topicName, true)
	require.NoError(t, err)
	assert.Equal(t, topicName, topicInfo.Name)
	require.Equal(t, 3, len(topicInfo.Partitions))

	// Spot-check single partition
	assert.Equal(t, []int{2, 3}, topicInfo.Partitions[1].Replicas)
	assert.Equal(t, []int{2, 3}, topicInfo.Partitions[1].ISR)
	assert.Equal(t, 2, topicInfo.Partitions[1].Leader)

	assert.Equal(
		t, map[string]string{
			"flush.ms":     "2000",
			"retention.ms": "10000000",
		},
		topicInfo.Config,
	)

	_, err = client.UpdateTopicConfig(
		ctx,
		topicName,
		[]kafka.ConfigEntry{
			{
				ConfigName:  "max.message.bytes",
				ConfigValue: "16384",
			},
			{
				ConfigName:  "retention.ms",
				ConfigValue: "12000000",
			},
			{
				ConfigName:  "flush.ms",
				ConfigValue: "",
			},
		},
		true,
	)
	require.Nil(t, err)

	topicInfo, err = client.GetTopic(ctx, topicName, true)
	require.NoError(t, err)
	assert.Equal(
		t, map[string]string{
			"max.message.bytes": "16384",
			"retention.ms":      "12000000",
		},
		topicInfo.Config,
	)
}

func TestBrokerClientBrokers(t *testing.T) {
	ctx := context.Background()
	client := NewBrokerAdminClient(util.TestKafkaAddr(), false)

	brokerIDs, err := client.GetBrokerIDs(ctx)
	require.NoError(t, err)
	assert.Equal(
		t,
		[]int{1, 2, 3, 4, 5, 6},
		brokerIDs,
	)

	brokerInfos, err := client.GetBrokers(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, 6, len(brokerInfos))

	racks := []string{}

	for _, brokerInfo := range brokerInfos {
		assert.Equal(t, map[string]string{}, brokerInfo.Config)
		racks = append(racks, brokerInfo.Rack)
	}

	assert.Equal(
		t,
		racks,
		[]string{
			"zone1",
			"zone1",
			"zone2",
			"zone2",
			"zone3",
			"zone3",
		},
	)

	_, err = client.UpdateBrokerConfig(
		ctx,
		2,
		[]kafka.ConfigEntry{
			{
				ConfigName:  "leader.replication.throttled.rate",
				ConfigValue: "21000000",
			},
		},
		true,
	)
	require.NoError(t, err)
	time.Sleep(time.Second)

	brokerInfos, err = client.GetBrokers(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, 6, len(brokerInfos))

	for _, brokerInfo := range brokerInfos {
		if brokerInfo.ID == 2 {
			assert.Equal(
				t,
				map[string]string{
					"leader.replication.throttled.rate": "",
				},
				brokerInfo.Config,
			)
		} else {
			assert.Equal(t, map[string]string{}, brokerInfo.Config)
		}
	}

	_, err = client.UpdateBrokerConfig(
		ctx,
		2,
		[]kafka.ConfigEntry{
			{
				ConfigName:  "leader.replication.throttled.rate",
				ConfigValue: "",
			},
		},
		true,
	)
	require.NoError(t, err)
	time.Sleep(time.Second)

	brokerInfos, err = client.GetBrokers(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, 6, len(brokerInfos))

	for _, brokerInfo := range brokerInfos {
		assert.Equal(t, map[string]string{}, brokerInfo.Config)
	}
}

func TestBrokerClientAddPartitions(t *testing.T) {
	ctx := context.Background()
	client := NewBrokerAdminClient(util.TestKafkaAddr(), false)
	topicName := util.RandomString("topic-add-partitions-", 6)

	err := client.CreateTopic(
		ctx,
		kafka.TopicConfig{
			Topic:             topicName,
			NumPartitions:     3,
			ReplicationFactor: 2,
		},
	)
	require.NoError(t, err)
	time.Sleep(time.Second)

	err = client.AddPartitions(
		ctx,
		topicName,
		[]PartitionAssignment{
			{
				ID:       3,
				Replicas: []int{3, 4},
			},
			{
				ID:       4,
				Replicas: []int{3, 5},
			},
		},
	)
	require.NoError(t, err)
}
