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

	adminClient, err := clusterConfig.NewAdminClient(ctx, nil, false)
	require.Nil(t, err)

	topicName := util.RandomString("check-topic-", 6)
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

	applier, err := apply.NewTopicApplier(
		ctx,
		adminClient,
		apply.TopicApplierConfig{
			ClusterConfig: clusterConfig,
			TopicConfig:   topicConfig,
			DryRun:        false,
			SkipConfirm:   true,
			SleepLoopTime: 500 * time.Millisecond,
		},
	)
	require.Nil(t, err)

	err = applier.Apply(ctx)
	require.Nil(t, err)

	type testCase struct {
		description      string
		checkTopicConfig config.TopicConfig
		expectedResults  map[CheckName]bool
	}

	testCases := []testCase{
		{
			description:      "all good",
			checkTopicConfig: topicConfig,
			expectedResults: map[CheckName]bool{
				CheckNameTopicExists:              true,
				CheckNameRetentionCorrect:         true,
				CheckNameReplicationFactorCorrect: true,
				CheckNamePartitionCountCorrect:    true,
				CheckNameThrottlesClear:           true,
				CheckNameReplicasInSync:           true,
				CheckNameLeadersCorrect:           true,
			},
		},
		{
			description: "topic does not exist",
			checkTopicConfig: config.TopicConfig{
				Meta: config.TopicMeta{
					Name: "non-matching-name",
				},
			},
			expectedResults: map[CheckName]bool{
				CheckNameTopicExists: false,
			},
		},
		{
			description:      "wrong retention",
			checkTopicConfig: topicConfig,
			expectedResults: map[CheckName]bool{
				CheckNameTopicExists:              true,
				CheckNameRetentionCorrect:         true,
				CheckNameReplicationFactorCorrect: true,
				CheckNamePartitionCountCorrect:    true,
				CheckNameThrottlesClear:           true,
				CheckNameReplicasInSync:           true,
				CheckNameLeadersCorrect:           true,
			},
		},
	}

	for _, testCase := range testCases {
		results, err := CheckTopic(
			ctx,
			CheckConfig{
				AdminClient:   adminClient,
				BrokerRacks:   -1,
				ClusterConfig: clusterConfig,
				CheckLeaders:  true,
				TopicConfig:   testCase.checkTopicConfig,
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
