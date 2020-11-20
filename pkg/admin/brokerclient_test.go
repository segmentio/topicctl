package admin

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

func TestBrokerClientGetClusterID(t *testing.T) {
	if !util.CanTestBrokerAdmin() {
		t.Skip("Skipping because KAFKA_TOPICS_TEST_BROKER_ADMIN is not set")
	}

	ctx := context.Background()
	client, err := NewBrokerAdminClient(
		ctx,
		BrokerAdminClientConfig{BrokerAddr: util.TestKafkaAddr()},
	)
	require.NoError(t, err)

	clusterID, err := client.GetClusterID(ctx)
	require.NoError(t, err)
	require.NotEqual(t, "", clusterID)
}

func TestBrokerClientUpdateTopicConfig(t *testing.T) {
	if !util.CanTestBrokerAdmin() {
		t.Skip("Skipping because KAFKA_TOPICS_TEST_BROKER_ADMIN is not set")
	}

	ctx := context.Background()
	client, err := NewBrokerAdminClient(
		ctx,
		BrokerAdminClientConfig{BrokerAddr: util.TestKafkaAddr()},
	)
	require.NoError(t, err)

	topicName := util.RandomString("topic-create-", 6)

	err = client.CreateTopic(
		ctx,
		kafka.TopicConfig{
			Topic:             topicName,
			NumPartitions:     -1,
			ReplicationFactor: -1,
			ReplicaAssignments: []kafka.ReplicaAssignment{
				{
					Partition: 0,
					Replicas:  []int{1, 2},
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
	util.RetryUntil(t, 5*time.Second, func() error {
		_, err := client.GetTopic(ctx, topicName, true)
		return err
	})

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
	require.NoError(t, err)

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
	if !util.CanTestBrokerAdmin() {
		t.Skip("Skipping because KAFKA_TOPICS_TEST_BROKER_ADMIN is not set")
	}

	ctx := context.Background()
	client, err := NewBrokerAdminClient(
		ctx,
		BrokerAdminClientConfig{BrokerAddr: util.TestKafkaAddr()},
	)
	require.NoError(t, err)

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
			{
				ConfigName:  "log.cleaner.io.buffer.load.factor",
				ConfigValue: "0.7",
			},
		},
		true,
	)
	require.NoError(t, err)

	// TODO: Replace this with some sort of retry until.
	time.Sleep(time.Second)

	brokerInfos, err = client.GetBrokers(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, 6, len(brokerInfos))

	var expectedThrottledRate string
	if client.GetSupportedFeatures().DynamicBrokerConfigs {
		expectedThrottledRate = "21000000"
	} else {
		expectedThrottledRate = ""
	}

	for _, brokerInfo := range brokerInfos {
		if brokerInfo.ID == 2 {
			assert.Equal(
				t,
				map[string]string{
					"leader.replication.throttled.rate": expectedThrottledRate,
					"log.cleaner.io.buffer.load.factor": "0.7",
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
			{
				ConfigName:  "log.cleaner.io.buffer.load.factor",
				ConfigValue: "",
			},
		},
		true,
	)
	require.NoError(t, err)

	// TODO: Replace this with some sort of retry until.
	time.Sleep(time.Second)

	brokerInfos, err = client.GetBrokers(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, 6, len(brokerInfos))

	for _, brokerInfo := range brokerInfos {
		assert.Equal(t, map[string]string{}, brokerInfo.Config)
	}
}

func TestBrokerClientAddPartitions(t *testing.T) {
	if !util.CanTestBrokerAdmin() {
		t.Skip("Skipping because KAFKA_TOPICS_TEST_BROKER_ADMIN is not set")
	}

	ctx := context.Background()
	client, err := NewBrokerAdminClient(
		ctx,
		BrokerAdminClientConfig{BrokerAddr: util.TestKafkaAddr()},
	)
	require.NoError(t, err)

	topicName := util.RandomString("topic-add-partitions-", 6)

	err = client.CreateTopic(
		ctx,
		kafka.TopicConfig{
			Topic:             topicName,
			NumPartitions:     3,
			ReplicationFactor: 2,
		},
	)
	require.NoError(t, err)
	util.RetryUntil(t, 5*time.Second, func() error {
		_, err := client.GetTopic(ctx, topicName, true)
		return err
	})

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
				Replicas: []int{6, 1},
			},
		},
	)
	require.NoError(t, err)

	var topicInfo TopicInfo

	util.RetryUntil(t, 5*time.Second, func() error {
		topicInfo, err = client.GetTopic(ctx, topicName, true)
		require.NoError(t, err)

		if len(topicInfo.Partitions) != 5 {
			return fmt.Errorf(
				"Expected 5 partitions, got %d",
				len(topicInfo.Partitions),
			)
		}

		return nil
	})

	assert.Equal(t, []int{3, 4}, topicInfo.Partitions[3].Replicas)
	assert.Equal(t, []int{6, 1}, topicInfo.Partitions[4].Replicas)
}

func TestBrokerClientAlterAssignments(t *testing.T) {
	if !util.CanTestBrokerAdmin() {
		t.Skip("Skipping because KAFKA_TOPICS_TEST_BROKER_ADMIN is not set")
	}

	ctx := context.Background()
	client, err := NewBrokerAdminClient(
		ctx,
		BrokerAdminClientConfig{BrokerAddr: util.TestKafkaAddr()},
	)
	require.NoError(t, err)

	topicName := util.RandomString("topic-alter-assignments-", 6)

	err = client.CreateTopic(
		ctx,
		kafka.TopicConfig{
			Topic:             topicName,
			NumPartitions:     -1,
			ReplicationFactor: -1,
			ReplicaAssignments: []kafka.ReplicaAssignment{
				{
					Partition: 0,
					Replicas:  []int{1, 2},
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
		},
	)
	require.NoError(t, err)
	util.RetryUntil(t, 5*time.Second, func() error {
		_, err := client.GetTopic(ctx, topicName, true)
		return err
	})

	err = client.AssignPartitions(
		ctx,
		topicName,
		[]PartitionAssignment{
			{
				ID:       1,
				Replicas: []int{2, 5},
			},
			{
				ID:       2,
				Replicas: []int{5, 1},
			},
		},
	)
	require.NoError(t, err)

	var topicInfo TopicInfo

	util.RetryUntil(t, 5*time.Second, func() error {
		topicInfo, err = client.GetTopic(ctx, topicName, true)
		require.NoError(t, err)

		if topicInfo.Partitions[2].Replicas[0] != 5 {
			return fmt.Errorf("Assign partitions change not reflected yet")
		}

		return nil
	})

	assert.Equal(
		t,
		[]int{1, 2},
		topicInfo.Partitions[0].Replicas,
	)
	assert.Equal(
		t,
		[]int{2, 5},
		topicInfo.Partitions[1].Replicas,
	)
	assert.Equal(
		t,
		[]int{5, 1},
		topicInfo.Partitions[2].Replicas,
	)
}

func TestBrokerClientRunLeaderElection(t *testing.T) {
	if !util.CanTestBrokerAdmin() {
		t.Skip("Skipping because KAFKA_TOPICS_TEST_BROKER_ADMIN is not set")
	}

	ctx := context.Background()
	client, err := NewBrokerAdminClient(
		ctx,
		BrokerAdminClientConfig{BrokerAddr: util.TestKafkaAddr()},
	)
	require.NoError(t, err)

	topicName := util.RandomString("topic-leader-election-", 6)

	err = client.CreateTopic(
		ctx,
		kafka.TopicConfig{
			Topic:             topicName,
			NumPartitions:     3,
			ReplicationFactor: 2,
		},
	)
	require.NoError(t, err)

	util.RetryUntil(t, 5*time.Second, func() error {
		_, err := client.GetTopic(ctx, topicName, true)
		return err
	})

	err = client.RunLeaderElection(
		ctx,
		topicName,
		[]int{1, 2},
	)
	require.NoError(t, err)
}

func TestBrokerClientGetApiVersions(t *testing.T) {
	if !util.CanTestBrokerAdmin() {
		t.Skip("Skipping because KAFKA_TOPICS_TEST_BROKER_ADMIN is not set")
	}

	ctx := context.Background()
	client, err := NewBrokerAdminClient(
		ctx,
		BrokerAdminClientConfig{BrokerAddr: util.TestKafkaAddr()},
	)
	require.NoError(t, err)

	_, err = client.getAPIVersions(ctx)
	require.NoError(t, err)
}
