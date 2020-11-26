package apply

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/config"
	"github.com/segmentio/topicctl/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplyBasicUpdates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	topicName := util.RandomString("apply-topic-", 6)
	topicConfig := config.TopicConfig{
		Meta: config.TopicMeta{
			Name:        topicName,
			Cluster:     "test-cluster",
			Region:      "test-region",
			Environment: "test-environment",
		},
		Spec: config.TopicSpec{
			Partitions:        9,
			ReplicationFactor: 2,
			RetentionMinutes:  500,
			Settings: config.TopicSettings{
				"cleanup.policy": "compact",
			},
			PlacementConfig: config.TopicPlacementConfig{
				Strategy: config.PlacementStrategyAny,
				Picker:   config.PickerMethodLowestIndex,
			},
			MigrationConfig: &config.TopicMigrationConfig{
				ThrottleMB:         2,
				PartitionBatchSize: 3,
			},
		},
	}

	applier := testApplier(ctx, t, topicConfig)
	applier.config.RetentionDropStepDuration = 50 * time.Minute

	assert.Equal(t, 3, applier.maxBatchSize)
	assert.Equal(t, int64(2000000), applier.throttleBytes)

	defer applier.adminClient.Close()
	err := applier.Apply(ctx)
	require.NoError(t, err)

	// Topic exists and is set up correctly
	topicInfo, err := applier.adminClient.GetTopic(ctx, topicName, true)
	require.NoError(t, err)
	assert.Equal(t, topicName, topicInfo.Name)
	assert.Equal(t, 9, len(topicInfo.Partitions))
	assert.Equal(t, 2, len(topicInfo.Partitions[0].Replicas))
	assert.Equal(t, "30000000", topicInfo.Config[admin.RetentionKey])
	assert.Equal(t, "compact", topicInfo.Config["cleanup.policy"])

	// Update retention and settings
	applier.topicConfig.Spec.RetentionMinutes = 400
	applier.topicConfig.Spec.Settings["cleanup.policy"] = "delete"
	err = applier.Apply(ctx)
	require.NoError(t, err)
	topicInfo, err = applier.adminClient.GetTopic(ctx, topicName, true)
	require.NoError(t, err)

	// Dropped to only 450 because of retention reduction
	assert.Equal(t, "27000000", topicInfo.Config[admin.RetentionKey])
	assert.Equal(t, "delete", topicInfo.Config["cleanup.policy"])

	// Updating replication factor not allowed
	applier.topicConfig.Spec.Partitions = 9
	applier.topicConfig.Spec.ReplicationFactor = 3
	err = applier.Apply(ctx)
	require.NotNil(t, err)
}

func TestApplyPlacementUpdates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	topicName := util.RandomString("apply-topic-", 6)
	topicConfig := config.TopicConfig{
		Meta: config.TopicMeta{
			Name:        topicName,
			Cluster:     "test-cluster",
			Region:      "test-region",
			Environment: "test-environment",
		},
		Spec: config.TopicSpec{
			Partitions:        6,
			ReplicationFactor: 2,
			RetentionMinutes:  500,
			PlacementConfig: config.TopicPlacementConfig{
				Strategy: config.PlacementStrategyStatic,
				Picker:   config.PickerMethodLowestIndex,
				StaticAssignments: [][]int{
					{1, 2},
					{2, 3},
					{1, 3},
					{1, 2},
					{2, 3},
					{1, 3},
				},
			},
			MigrationConfig: &config.TopicMigrationConfig{
				ThrottleMB:         2,
				PartitionBatchSize: 3,
			},
		},
	}

	// Initial apply lays out partitions as specified in static config
	applier := testApplier(ctx, t, topicConfig)
	defer applier.adminClient.Close()
	err := applier.Apply(ctx)
	require.NoError(t, err)

	topicInfo, err := applier.adminClient.GetTopic(ctx, topicName, true)
	require.NoError(t, err)
	updatedReplicas, err := admin.AssignmentsToReplicas(topicInfo.ToAssignments())
	require.NoError(t, err)

	assert.Equal(
		t,
		[][]int{
			{1, 2},
			{2, 3},
			{1, 3},
			{1, 2},
			{2, 3},
			{1, 3},
		},
		updatedReplicas,
	)
	assert.True(t, topicInfo.AllLeadersCorrect())

	// Next apply converts to balanced leaders
	applier.topicConfig.Spec.PlacementConfig.Strategy = config.PlacementStrategyBalancedLeaders
	err = applier.Apply(ctx)
	require.NoError(t, err)

	topicInfo, err = applier.adminClient.GetTopic(ctx, topicName, true)
	require.NoError(t, err)
	updatedReplicas, err = admin.AssignmentsToReplicas(topicInfo.ToAssignments())
	require.NoError(t, err)

	assert.Equal(
		t,
		[][]int{
			{5, 2},
			{6, 3},
			{3, 1},
			{1, 2},
			{2, 3},
			{3, 1},
		},
		updatedReplicas,
	)
	assert.True(t, topicInfo.AllLeadersCorrect())

	// Third apply switches to in-rack
	applier.topicConfig.Spec.PlacementConfig.Strategy = config.PlacementStrategyInRack
	err = applier.Apply(ctx)
	require.NoError(t, err)

	topicInfo, err = applier.adminClient.GetTopic(ctx, topicName, true)
	require.NoError(t, err)
	updatedReplicas, err = admin.AssignmentsToReplicas(topicInfo.ToAssignments())
	require.NoError(t, err)

	assert.Equal(
		t,
		[][]int{
			{5, 6},
			{6, 5},
			{3, 4},
			{1, 2},
			{2, 1},
			{3, 4},
		},
		updatedReplicas,
	)
	assert.True(t, topicInfo.AllLeadersCorrect())

	brokers, err := applier.adminClient.GetBrokers(ctx, nil)
	require.NoError(t, err)

	// No throttles on brokers or topic
	if applier.adminClient.GetSupportedFeatures().DynamicBrokerConfigs {
		assert.Equal(t, 0, len(admin.ThrottledBrokerIDs(brokers)))
	}
	assert.False(t, topicInfo.IsThrottled())
}

func TestApplyRebalance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	topicName := util.RandomString("apply-topic-", 6)
	topicConfig := config.TopicConfig{
		Meta: config.TopicMeta{
			Name:        topicName,
			Cluster:     "test-cluster",
			Region:      "test-region",
			Environment: "test-environment",
		},
		Spec: config.TopicSpec{
			Partitions:        3,
			ReplicationFactor: 2,
			RetentionMinutes:  500,
			PlacementConfig: config.TopicPlacementConfig{
				Strategy: config.PlacementStrategyStatic,
				Picker:   config.PickerMethodLowestIndex,
				StaticAssignments: [][]int{
					{1, 2},
					{2, 3},
					{1, 3},
				},
			},
			MigrationConfig: &config.TopicMigrationConfig{
				ThrottleMB:         2,
				PartitionBatchSize: 3,
			},
		},
	}

	// Initial apply lays out partitions as specified in static config
	applier := testApplier(ctx, t, topicConfig)
	defer applier.adminClient.Close()
	err := applier.Apply(ctx)
	require.NoError(t, err)

	topicInfo, err := applier.adminClient.GetTopic(ctx, topicName, true)
	require.NoError(t, err)
	updatedReplicas, err := admin.AssignmentsToReplicas(topicInfo.ToAssignments())
	require.NoError(t, err)

	assert.Equal(
		t,
		[][]int{
			{1, 2},
			{2, 3},
			{1, 3},
		},
		updatedReplicas,
	)
	assert.True(t, topicInfo.AllLeadersCorrect())

	// Next apply rebalances
	applier.topicConfig.Spec.PlacementConfig.Strategy = config.PlacementStrategyAny
	applier.config.Rebalance = true
	err = applier.Apply(ctx)
	require.NoError(t, err)

	topicInfo, err = applier.adminClient.GetTopic(ctx, topicName, true)
	require.NoError(t, err)
	updatedReplicas, err = admin.AssignmentsToReplicas(topicInfo.ToAssignments())
	require.NoError(t, err)

	// Unfortunately, because the rebalance is randomized based on the topic name, it's
	// hard to test the exact result here. For now, just check that a rebalance did occur.
	assert.NotEqual(
		t,
		[][]int{
			{1, 2},
			{2, 3},
			{1, 3},
		},
		updatedReplicas,
	)
	assert.True(t, topicInfo.AllLeadersCorrect())
}

func TestApplyExtendPartitions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topicName := util.RandomString("apply-topic-extend-", 6)
	topicConfig := config.TopicConfig{
		Meta: config.TopicMeta{
			Name:        topicName,
			Cluster:     "test-cluster",
			Region:      "test-region",
			Environment: "test-environment",
		},
		Spec: config.TopicSpec{
			Partitions:        3,
			ReplicationFactor: 2,
			RetentionMinutes:  500,
			PlacementConfig: config.TopicPlacementConfig{
				Strategy: config.PlacementStrategyStatic,
				Picker:   config.PickerMethodLowestIndex,
				StaticAssignments: [][]int{
					{1, 2},
					{3, 4},
					{5, 2},
				},
			},
			MigrationConfig: &config.TopicMigrationConfig{
				ThrottleMB:         2,
				PartitionBatchSize: 3,
			},
		},
	}

	// Initial apply lays out partitions as specified in static config
	applier := testApplier(ctx, t, topicConfig)
	defer applier.adminClient.Close()
	err := applier.Apply(ctx)
	require.NoError(t, err)

	topicInfo, err := applier.adminClient.GetTopic(ctx, topicName, true)
	require.NoError(t, err)
	updatedReplicas, err := admin.AssignmentsToReplicas(topicInfo.ToAssignments())
	require.NoError(t, err)

	assert.Equal(
		t,
		[][]int{
			{1, 2},
			{3, 4},
			{5, 2},
		},
		updatedReplicas,
	)
	assert.True(t, topicInfo.AllLeadersCorrect())

	// Next apply extends by 3 partitions with balanced leader strategy
	applier.topicConfig.Spec.Partitions = 6
	applier.topicConfig.Spec.PlacementConfig.Strategy = config.PlacementStrategyBalancedLeaders
	err = applier.Apply(ctx)
	require.NoError(t, err)

	topicInfo, err = applier.adminClient.GetTopic(ctx, topicName, true)
	require.NoError(t, err)
	updatedReplicas, err = admin.AssignmentsToReplicas(topicInfo.ToAssignments())
	require.NoError(t, err)

	assert.Equal(
		t,
		[][]int{
			{1, 2},
			{3, 4},
			{5, 2},
			{2, 3},
			{4, 5},
			{6, 1},
		},
		updatedReplicas,
	)
	assert.True(t, topicInfo.AllLeadersCorrect())

	brokers, err := applier.adminClient.GetBrokers(ctx, nil)
	require.NoError(t, err)

	// No throttles on brokers or topic
	if applier.adminClient.GetSupportedFeatures().DynamicBrokerConfigs {
		assert.Equal(t, 0, len(admin.ThrottledBrokerIDs(brokers)))
	}
	assert.False(t, topicInfo.IsThrottled())
}

func TestApplyExistingThrottles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topicName1 := util.RandomString("apply-topic-extend-", 6)
	topicName2 := util.RandomString("apply-topic-extend-", 6)

	topicConfig1 := config.TopicConfig{
		Meta: config.TopicMeta{
			Name:        topicName1,
			Cluster:     "test-cluster",
			Region:      "test-region",
			Environment: "test-environment",
		},
		Spec: config.TopicSpec{
			Partitions:        3,
			ReplicationFactor: 2,
			RetentionMinutes:  500,
			PlacementConfig: config.TopicPlacementConfig{
				Strategy: config.PlacementStrategyAny,
				Picker:   config.PickerMethodLowestIndex,
			},
			MigrationConfig: &config.TopicMigrationConfig{
				ThrottleMB:         2,
				PartitionBatchSize: 3,
			},
		},
	}
	topicConfig2 := config.TopicConfig{
		Meta: config.TopicMeta{
			Name:        topicName2,
			Cluster:     "test-cluster",
			Region:      "test-region",
			Environment: "test-environment",
		},
		Spec: config.TopicSpec{
			Partitions:        3,
			ReplicationFactor: 2,
			RetentionMinutes:  500,
			PlacementConfig: config.TopicPlacementConfig{
				Strategy: config.PlacementStrategyStaticInRack,
				Picker:   config.PickerMethodLowestIndex,
				StaticRackAssignments: []string{
					"zone1",
					"zone2",
					"zone3",
				},
			},
			MigrationConfig: &config.TopicMigrationConfig{
				ThrottleMB:         2,
				PartitionBatchSize: 3,
			},
		},
	}

	// Create topics
	applier1 := testApplier(ctx, t, topicConfig1)
	defer applier1.adminClient.Close()
	err := applier1.Apply(ctx)
	require.NoError(t, err)

	applier2 := testApplier(ctx, t, topicConfig2)
	defer applier2.adminClient.Close()
	err = applier2.Apply(ctx)
	require.NoError(t, err)

	supported1 := applier1.adminClient.GetSupportedFeatures()
	supported2 := applier1.adminClient.GetSupportedFeatures()
	if !(supported1.Locks && supported1.DynamicBrokerConfigs &&
		supported2.Locks && supported2.DynamicBrokerConfigs) {
		// This test only works on zk-based clients for now
		return
	}

	// Add some throttles
	_, err = applier1.adminClient.UpdateTopicConfig(
		ctx,
		topicName1,
		[]kafka.ConfigEntry{
			{
				ConfigName:  admin.FollowerReplicasThrottledKey,
				ConfigValue: "1:3,3:4",
			},
		},
		true,
	)
	require.NoError(t, err)
	_, err = applier2.adminClient.UpdateTopicConfig(
		ctx,
		topicName2,
		[]kafka.ConfigEntry{
			{
				ConfigName:  admin.FollowerReplicasThrottledKey,
				ConfigValue: "1:3,3:4",
			},
		},
		true,
	)
	require.NoError(t, err)

	_, err = applier1.adminClient.UpdateBrokerConfig(
		ctx,
		1,
		[]kafka.ConfigEntry{
			{
				ConfigName:  admin.FollowerThrottledKey,
				ConfigValue: "123456",
			},
		},
		true,
	)
	require.NoError(t, err)
	_, err = applier1.adminClient.UpdateBrokerConfig(
		ctx,
		2,
		[]kafka.ConfigEntry{
			{
				ConfigName:  admin.LeaderThrottledKey,
				ConfigValue: "123456",
			},
		},
		true,
	)
	require.NoError(t, err)

	// Acquire lock to simulate ongoing migration
	lock, _, err := applier2.acquireClusterLock(ctx)
	require.NoError(t, err)

	// Reapply topic1 with new applier (to pick up updated brokers)
	updatedApplier1 := testApplier(ctx, t, topicConfig1)
	defer updatedApplier1.adminClient.Close()
	err = updatedApplier1.Apply(ctx)
	require.NoError(t, err)

	updatedTopic, err := updatedApplier1.adminClient.GetTopic(ctx, topicName1, false)
	require.NoError(t, err)

	// Topic1 is still throttled because the lock is in place
	assert.True(t, updatedTopic.IsThrottled())

	// Remove the lock and reapply
	require.Nil(t, lock.Unlock())
	err = updatedApplier1.Apply(ctx)
	require.NoError(t, err)

	updatedTopic, err = updatedApplier1.adminClient.GetTopic(ctx, topicName1, false)
	require.NoError(t, err)

	// Topic1 throttle has been removed
	assert.False(t, updatedTopic.IsThrottled())

	brokers, err := updatedApplier1.adminClient.GetBrokers(ctx, nil)
	require.NoError(t, err)

	// But throttles are still on brokers because of the throttle on topic2
	assert.Equal(t, 2, len(admin.ThrottledBrokerIDs(brokers)))

	// Remove topic2 throttle and re-apply
	_, err = applier2.adminClient.UpdateTopicConfig(
		ctx,
		topicName2,
		[]kafka.ConfigEntry{
			{
				ConfigName:  admin.FollowerReplicasThrottledKey,
				ConfigValue: "",
			},
		},
		true,
	)
	require.NoError(t, err)

	err = updatedApplier1.Apply(ctx)
	require.NoError(t, err)
	brokers, err = updatedApplier1.adminClient.GetBrokers(ctx, nil)
	require.NoError(t, err)

	// Broker throttles now removed
	assert.Equal(t, 0, len(admin.ThrottledBrokerIDs(brokers)))
}

func TestApplyDryRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	topicName := util.RandomString("apply-topic-dry-run-", 6)
	topicConfig := config.TopicConfig{
		Meta: config.TopicMeta{
			Name:        topicName,
			Cluster:     "test-cluster",
			Region:      "test-region",
			Environment: "test-environment",
		},
		Spec: config.TopicSpec{
			Partitions:        9,
			ReplicationFactor: 2,
			RetentionMinutes:  500,
			PlacementConfig: config.TopicPlacementConfig{
				Strategy: config.PlacementStrategyAny,
				Picker:   config.PickerMethodLowestIndex,
			},
			MigrationConfig: &config.TopicMigrationConfig{
				ThrottleMB:         2,
				PartitionBatchSize: 3,
			},
		},
	}

	applier := testApplier(ctx, t, topicConfig)
	defer applier.adminClient.Close()
	applier.config.DryRun = true
	err := applier.Apply(ctx)
	require.NoError(t, err)

	// Dry-run on, topic not created
	topics, err := applier.adminClient.GetTopics(ctx, []string{topicName}, false)
	require.NoError(t, err)
	require.Equal(t, 0, len(topics))

	applier.config.DryRun = false
	err = applier.Apply(ctx)
	require.NoError(t, err)

	// Dry-run off, topic created
	topic, err := applier.adminClient.GetTopic(ctx, topicName, false)
	require.NoError(t, err)
	require.Equal(t, topicName, topic.Name)

	// Try modifications with dry-run set to true again
	applier.topicConfig.Spec.RetentionMinutes = 600
	applier.topicConfig.Spec.Partitions = 12
	applier.topicConfig.Spec.PlacementConfig.Strategy = config.PlacementStrategyInRack

	applier.config.DryRun = true
	err = applier.Apply(ctx)
	require.NoError(t, err)

	// Changes not made
	updatedTopic, err := applier.adminClient.GetTopic(ctx, topicName, false)
	require.NoError(t, err)
	require.Equal(t, topicName, topic.Name)
	require.Equal(t, 500, int(updatedTopic.Retention().Minutes()))
	require.Equal(t, 9, len(updatedTopic.Partitions))
	require.Equal(t, topic.ToAssignments(), updatedTopic.ToAssignments())
}

func TestApplyThrottles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	topicName := util.RandomString("apply-topic-", 6)
	topicConfig := config.TopicConfig{
		Meta: config.TopicMeta{
			Name:        topicName,
			Cluster:     "test-cluster",
			Region:      "test-region",
			Environment: "test-environment",
		},
		Spec: config.TopicSpec{
			Partitions:        6,
			ReplicationFactor: 2,
			RetentionMinutes:  500,
			PlacementConfig: config.TopicPlacementConfig{
				Strategy: config.PlacementStrategyStatic,
				Picker:   config.PickerMethodLowestIndex,
				StaticAssignments: [][]int{
					{1, 2},
					{2, 3},
					{1, 3},
					{1, 2},
					{2, 3},
					{1, 3},
				},
			},
			MigrationConfig: &config.TopicMigrationConfig{
				ThrottleMB:         20,
				PartitionBatchSize: 3,
			},
		},
	}
	applier := testApplier(ctx, t, topicConfig)
	defer applier.adminClient.Close()
	kafkaTopicConfig, err := topicConfig.ToNewTopicConfig()
	require.NoError(t, err)

	err = applier.adminClient.CreateTopic(ctx, kafkaTopicConfig)
	require.NoError(t, err)
	time.Sleep(250 * time.Millisecond)

	// Creating new partitions- don't throttle
	throttledTopic, throttledBrokers, err := applier.applyThrottles(
		ctx,
		nil,
		[]admin.PartitionAssignment{
			{
				ID:       1,
				Replicas: []int{3, 2, 1},
			},
			{
				ID:       2,
				Replicas: []int{5, 4, 6},
			},
		},
		false,
	)
	require.NoError(t, err)
	assert.False(t, throttledTopic)
	assert.Equal(t, 0, len(throttledBrokers))

	// New topic- don't throttle
	throttledTopic, throttledBrokers, err = applier.applyThrottles(
		ctx,
		[]admin.PartitionAssignment{
			{
				ID:       1,
				Replicas: []int{1, 2, 3},
			},
			{
				ID:       2,
				Replicas: []int{4, 5, 6},
			},
		},
		[]admin.PartitionAssignment{
			{
				ID:       1,
				Replicas: []int{3, 2, 1},
			},
			{
				ID:       2,
				Replicas: []int{7, 4, 6},
			},
		},
		true,
	)
	require.NoError(t, err)
	assert.False(t, throttledTopic)
	assert.Equal(t, 0, len(throttledBrokers))

	// Leader election only- don't throttle
	throttledTopic, throttledBrokers, err = applier.applyThrottles(
		ctx,
		[]admin.PartitionAssignment{
			{
				ID:       1,
				Replicas: []int{1, 2, 3},
			},
			{
				ID:       2,
				Replicas: []int{4, 5, 6},
			},
		},
		[]admin.PartitionAssignment{
			{
				ID:       1,
				Replicas: []int{3, 2, 1},
			},
			{
				ID:       2,
				Replicas: []int{5, 4, 6},
			},
		},
		false,
	)
	require.NoError(t, err)
	assert.False(t, throttledTopic)
	assert.Equal(t, 0, len(throttledBrokers))

	err = applier.removeThottles(ctx, throttledTopic, throttledBrokers)
	assert.Nil(t, err)

	_, err = applier.adminClient.UpdateBrokerConfig(
		ctx,
		5,
		[]kafka.ConfigEntry{
			{
				ConfigName:  admin.LeaderThrottledKey,
				ConfigValue: "500000",
			},
			{
				ConfigName:  admin.FollowerThrottledKey,
				ConfigValue: "500000",
			},
		},
		true,
	)
	require.NoError(t, err)

	defer func() {
		applier.adminClient.UpdateBrokerConfig(
			ctx,
			5,
			[]kafka.ConfigEntry{
				{
					ConfigName:  admin.LeaderThrottledKey,
					ConfigValue: "",
				},
				{
					ConfigName:  admin.FollowerThrottledKey,
					ConfigValue: "",
				},
			},
			true,
		)
	}()

	throttledTopic, throttledBrokers, err = applier.applyThrottles(
		ctx,
		[]admin.PartitionAssignment{
			{
				ID:       1,
				Replicas: []int{1, 2, 3},
			},
		},
		[]admin.PartitionAssignment{
			{
				ID:       1,
				Replicas: []int{2, 4, 5},
			},
		},
		false,
	)
	require.NoError(t, err)
	assert.True(t, throttledTopic)

	if applier.adminClient.GetSupportedFeatures().DynamicBrokerConfigs {
		assert.Equal(t, 4, len(throttledBrokers))
	}

	topicInfo, err := applier.adminClient.GetTopic(ctx, topicName, false)
	require.NoError(t, err)
	assert.NotEqual(t, "", topicInfo.Config[admin.LeaderReplicasThrottledKey])
	assert.NotEqual(t, "", topicInfo.Config[admin.FollowerReplicasThrottledKey])

	if applier.adminClient.GetSupportedFeatures().DynamicBrokerConfigs {
		brokers, err := applier.adminClient.GetBrokers(ctx, []int{1, 2, 3, 4, 5})
		require.NoError(t, err)
		for _, broker := range brokers {
			if broker.ID == 5 {
				// Existing values are kept in-place
				assert.Equal(t, "500000", broker.Config[admin.LeaderThrottledKey])
				assert.Equal(t, "500000", broker.Config[admin.FollowerThrottledKey])
			} else {
				assert.Equal(t, "20000000", broker.Config[admin.LeaderThrottledKey])
				assert.Equal(t, "20000000", broker.Config[admin.FollowerThrottledKey])
			}
		}
	}

	err = applier.removeThottles(ctx, throttledTopic, throttledBrokers)
	require.NoError(t, err)

	topicInfo, err = applier.adminClient.GetTopic(ctx, topicName, false)
	require.NoError(t, err)
	assert.Equal(t, "", topicInfo.Config[admin.LeaderReplicasThrottledKey])
	assert.Equal(t, "", topicInfo.Config[admin.FollowerReplicasThrottledKey])

	if applier.adminClient.GetSupportedFeatures().DynamicBrokerConfigs {
		brokers, err := applier.adminClient.GetBrokers(ctx, []int{1, 2, 3, 4, 5})
		require.NoError(t, err)
		for _, broker := range brokers {
			if broker.ID == 5 {
				// Existing values are kept in place
				assert.Equal(t, "500000", broker.Config[admin.LeaderThrottledKey])
				assert.Equal(t, "500000", broker.Config[admin.FollowerThrottledKey])
			} else {
				assert.Equal(t, "", broker.Config[admin.LeaderThrottledKey])
				assert.Equal(t, "", broker.Config[admin.FollowerThrottledKey])
			}
		}
	}
}

func TestApplyOverrides(t *testing.T) {
	ctx := context.Background()

	clusterConfig := config.ClusterConfig{
		Meta: config.ClusterMeta{
			Name:        "test-cluster",
			Region:      "test-region",
			Environment: "test-environment",
		},
		Spec: config.ClusterSpec{
			BootstrapAddrs: []string{util.TestKafkaAddr()},
			ZKAddrs:        []string{util.TestZKAddr()},
			ZKLockPath:     "/topicctl/locks",
		},
	}

	topicName := util.RandomString("apply-topic-", 6)
	topicConfig := config.TopicConfig{
		Meta: config.TopicMeta{
			Name:        topicName,
			Cluster:     "test-cluster",
			Region:      "test-region",
			Environment: "test-environment",
		},
		Spec: config.TopicSpec{
			Partitions:        9,
			ReplicationFactor: 2,
			RetentionMinutes:  500,
			PlacementConfig: config.TopicPlacementConfig{
				Strategy: config.PlacementStrategyAny,
				Picker:   config.PickerMethodLowestIndex,
			},
			MigrationConfig: &config.TopicMigrationConfig{
				ThrottleMB:         2,
				PartitionBatchSize: 3,
			},
		},
	}

	adminClient, err := clusterConfig.NewAdminClient(ctx, nil, false, "", "")
	require.NoError(t, err)

	applier, err := NewTopicApplier(
		ctx,
		adminClient,
		TopicApplierConfig{
			BrokerThrottleMBsOverride:  50,
			ClusterConfig:              clusterConfig,
			TopicConfig:                topicConfig,
			DryRun:                     false,
			SkipConfirm:                true,
			SleepLoopDuration:          500 * time.Millisecond,
			PartitionBatchSizeOverride: 8,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, int64(50000000), applier.throttleBytes)
	assert.Equal(t, applier.maxBatchSize, 8)
}

func testTopicName(name string) string {
	return util.RandomString(fmt.Sprintf("topic-%s-", name), 6)
}

func testApplier(
	ctx context.Context,
	t *testing.T,
	topicConfig config.TopicConfig,
) *TopicApplier {
	clusterConfig := config.ClusterConfig{
		Meta: config.ClusterMeta{
			Name:        "test-cluster",
			Region:      "test-region",
			Environment: "test-environment",
		},
		Spec: config.ClusterSpec{
			BootstrapAddrs: []string{util.TestKafkaAddr()},
			ZKAddrs:        []string{util.TestZKAddr()},
			ZKLockPath:     "/topicctl/locks",
		},
	}

	adminClient, err := clusterConfig.NewAdminClient(ctx, nil, false, "", "")
	require.NoError(t, err)

	applier, err := NewTopicApplier(
		ctx,
		adminClient,
		TopicApplierConfig{
			ClusterConfig:     clusterConfig,
			TopicConfig:       topicConfig,
			DryRun:            false,
			SkipConfirm:       true,
			SleepLoopDuration: 500 * time.Millisecond,
		},
	)
	require.NoError(t, err)
	return applier
}
