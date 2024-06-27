package check

import (
	"context"
	"testing"
	"time"

	"github.com/segmentio/topicctl/pkg/apply"
	"github.com/segmentio/topicctl/pkg/config"
	"github.com/segmentio/topicctl/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheck(t *testing.T) {
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

	adminClient, err := clusterConfig.NewAdminClient(ctx, nil, config.AdminClientOpts{})
	require.NoError(t, err)

	topicName := util.RandomString("check-topic-", 6)
	topicConfig := config.TopicConfig{
		Meta: config.ResourceMeta{
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

	applier, err := apply.NewTopicApplier(
		ctx,
		adminClient,
		apply.TopicApplierConfig{
			ClusterConfig:     clusterConfig,
			TopicConfig:       topicConfig,
			DryRun:            false,
			SkipConfirm:       true,
			SleepLoopDuration: 500 * time.Millisecond,
		},
	)
	require.NoError(t, err)

	_, err = applier.Apply(ctx)
	require.NoError(t, err)

	type testCase struct {
		description      string
		checkTopicConfig config.TopicConfig
		expectedResults  map[CheckName]bool
		validateOnly     bool
		numRacks         int
	}

	testCases := []testCase{
		{
			description:      "all good",
			checkTopicConfig: topicConfig,
			expectedResults: map[CheckName]bool{
				CheckNameConfigCorrect:            true,
				CheckNameConfigsConsistent:        true,
				CheckNameTopicExists:              true,
				CheckNameConfigSettingsCorrect:    true,
				CheckNameReplicationFactorCorrect: true,
				CheckNamePartitionCountCorrect:    true,
				CheckNameThrottlesClear:           true,
				CheckNameReplicasInSync:           true,
				CheckNameLeadersCorrect:           true,
			},
		},
		{
			description:      "all good (validate only)",
			checkTopicConfig: topicConfig,
			expectedResults: map[CheckName]bool{
				CheckNameConfigCorrect:     true,
				CheckNameConfigsConsistent: true,
			},
			validateOnly: true,
		},
		{
			description: "topic does not exist",
			checkTopicConfig: config.TopicConfig{
				Meta: config.ResourceMeta{
					Name:        "non-existent-topic",
					Cluster:     "non-matching-cluster",
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
			},
			expectedResults: map[CheckName]bool{
				CheckNameConfigCorrect:     true,
				CheckNameConfigsConsistent: false,
			},
		},
		{
			description: "topic does not exist",
			checkTopicConfig: config.TopicConfig{
				Meta: config.ResourceMeta{
					Name:        "non-existent-topic",
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
			},
			expectedResults: map[CheckName]bool{
				CheckNameConfigCorrect:     true,
				CheckNameConfigsConsistent: true,
				CheckNameTopicExists:       false,
			},
		},
		{
			description: "wrong configuration",
			checkTopicConfig: config.TopicConfig{
				Meta: config.ResourceMeta{
					Name:        topicName,
					Cluster:     "test-cluster",
					Region:      "test-region",
					Environment: "test-environment",
				},
				Spec: config.TopicSpec{
					Partitions:        10,
					ReplicationFactor: 3,
					RetentionMinutes:  600,
					PlacementConfig: config.TopicPlacementConfig{
						Strategy: config.PlacementStrategyAny,
						Picker:   config.PickerMethodLowestIndex,
					},
					MigrationConfig: &config.TopicMigrationConfig{
						ThrottleMB:         2,
						PartitionBatchSize: 3,
					},
				},
			},
			expectedResults: map[CheckName]bool{
				CheckNameConfigCorrect:            true,
				CheckNameConfigsConsistent:        true,
				CheckNameTopicExists:              true,
				CheckNameConfigSettingsCorrect:    false,
				CheckNameReplicationFactorCorrect: false,
				CheckNamePartitionCountCorrect:    false,
				CheckNameThrottlesClear:           true,
				CheckNameReplicasInSync:           true,
				CheckNameLeadersCorrect:           true,
			},
		},
		{
			description: "too few racks",
			checkTopicConfig: config.TopicConfig{
				Meta: topicConfig.Meta,
				// topicConfig.Spec but CrossRack
				Spec: config.TopicSpec{
					Partitions:        9,
					ReplicationFactor: 2,
					RetentionMinutes:  500,
					PlacementConfig: config.TopicPlacementConfig{
						Strategy: config.PlacementStrategyCrossRack,
						Picker:   config.PickerMethodLowestIndex,
					},
					MigrationConfig: &config.TopicMigrationConfig{
						ThrottleMB:         2,
						PartitionBatchSize: 3,
					},
				},
			},
			expectedResults: map[CheckName]bool{
				CheckNameConfigCorrect: false,
			},
			numRacks: 1, // ReplicationFactor - 1
		},
	}

	for _, testCase := range testCases {
		results, err := CheckTopic(
			ctx,
			CheckConfig{
				AdminClient:   adminClient,
				ClusterConfig: clusterConfig,
				CheckLeaders:  true,
				NumRacks:      testCase.numRacks,
				TopicConfig:   testCase.checkTopicConfig,
				ValidateOnly:  testCase.validateOnly,
			},
		)
		require.Nil(t, err, testCase.description)

		resultsSummary := map[CheckName]bool{}
		for _, result := range results.Results {
			resultsSummary[result.Name] = result.OK
		}

		assert.Equal(
			t,
			testCase.expectedResults, resultsSummary,
			testCase.description,
		)
	}
}
