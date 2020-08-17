package config

import (
	"testing"

	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/stretchr/testify/assert"
)

func TestTopicValidate(t *testing.T) {
	type testCase struct {
		description string
		topicConfig TopicConfig
		numRacks    int
		expError    bool
	}

	testCases := []testCase{
		{
			description: "all good any placement",
			topicConfig: TopicConfig{
				Meta: TopicMeta{
					Name:        "test-topic",
					Cluster:     "test-cluster",
					Region:      "test-region",
					Environment: "test-environment",
					Description: "Bootstrapped via topicctl bootstrap",
				},
				Spec: TopicSpec{
					Partitions:        2,
					ReplicationFactor: 3,
					RetentionMinutes:  120,
					Settings: TopicSettings{
						"cleanup.policy": "compact",
					},
					PlacementConfig: TopicPlacementConfig{
						Strategy: PlacementStrategyAny,
					},
				},
			},
			expError: false,
		},
		{
			description: "all good balanced-leaders placement",
			topicConfig: TopicConfig{
				Meta: TopicMeta{
					Name:        "test-topic",
					Cluster:     "test-cluster",
					Region:      "test-region",
					Environment: "test-environment",
					Description: "Bootstrapped via topicctl bootstrap",
				},
				Spec: TopicSpec{
					Partitions:        6,
					ReplicationFactor: 3,
					RetentionMinutes:  120,
					PlacementConfig: TopicPlacementConfig{
						Strategy: PlacementStrategyBalancedLeaders,
					},
				},
			},
			numRacks: 3,
			expError: false,
		},
		{
			description: "all good static placement",
			topicConfig: TopicConfig{
				Meta: TopicMeta{
					Name:        "test-topic",
					Cluster:     "test-cluster",
					Region:      "test-region",
					Environment: "test-environment",
					Description: "Bootstrapped via topicctl bootstrap",
				},
				Spec: TopicSpec{
					Partitions:        2,
					ReplicationFactor: 3,
					RetentionMinutes:  120,
					PlacementConfig: TopicPlacementConfig{
						Strategy: PlacementStrategyStatic,
						StaticAssignments: [][]int{
							{1, 2, 3},
							{4, 5, 6},
						},
					},
				},
			},
			expError: false,
		},
		{
			description: "all good static-in-rack placement",
			topicConfig: TopicConfig{
				Meta: TopicMeta{
					Name:        "test-topic",
					Cluster:     "test-cluster",
					Region:      "test-region",
					Environment: "test-environment",
					Description: "Bootstrapped via topicctl bootstrap",
				},
				Spec: TopicSpec{
					Partitions:        2,
					ReplicationFactor: 3,
					RetentionMinutes:  120,
					PlacementConfig: TopicPlacementConfig{
						Strategy: PlacementStrategyStaticInRack,
						StaticRackAssignments: []string{
							"zone1",
							"zone2",
						},
					},
				},
			},
			expError: false,
		},
		{
			description: "missing meta fields",
			topicConfig: TopicConfig{
				Meta: TopicMeta{
					Name:        "test-topic",
					Environment: "test-environment",
					Description: "Bootstrapped via topicctl bootstrap",
				},
				Spec: TopicSpec{
					Partitions:        2,
					ReplicationFactor: 3,
					RetentionMinutes:  120,
					PlacementConfig: TopicPlacementConfig{
						Strategy: PlacementStrategyAny,
					},
				},
			},
			expError: true,
		},
		{
			description: "double-setting retention",
			topicConfig: TopicConfig{
				Meta: TopicMeta{
					Name:        "test-topic",
					Cluster:     "test-cluster",
					Region:      "test-region",
					Environment: "test-environment",
					Description: "Bootstrapped via topicctl bootstrap",
				},
				Spec: TopicSpec{
					Partitions:        2,
					ReplicationFactor: 3,
					RetentionMinutes:  120,
					Settings: TopicSettings{
						"retention.ms": "1234",
					},
					PlacementConfig: TopicPlacementConfig{
						Strategy: PlacementStrategyAny,
					},
				},
			},
			expError: true,
		},
		{
			description: "balanced leaders invalid rack count",
			topicConfig: TopicConfig{
				Meta: TopicMeta{
					Name:        "test-topic",
					Cluster:     "test-cluster",
					Region:      "test-region",
					Environment: "test-environment",
					Description: "Bootstrapped via topicctl bootstrap",
				},
				Spec: TopicSpec{
					Partitions:        7,
					ReplicationFactor: 3,
					PlacementConfig: TopicPlacementConfig{
						Strategy: PlacementStrategyBalancedLeaders,
					},
				},
			},
			numRacks: 3,
			expError: true,
		},
		{
			description: "static placement invalid num partitions",
			topicConfig: TopicConfig{
				Meta: TopicMeta{
					Name:        "test-topic",
					Cluster:     "test-cluster",
					Region:      "test-region",
					Environment: "test-environment",
					Description: "Bootstrapped via topicctl bootstrap",
				},
				Spec: TopicSpec{
					Partitions:        7,
					ReplicationFactor: 3,
					PlacementConfig: TopicPlacementConfig{
						Strategy: PlacementStrategyStatic,
						StaticAssignments: [][]int{
							{1, 2, 3},
							{4, 5, 6},
						},
					},
				},
			},
			expError: true,
		},
		{
			description: "static placement invalid replication",
			topicConfig: TopicConfig{
				Meta: TopicMeta{
					Name:        "test-topic",
					Cluster:     "test-cluster",
					Region:      "test-region",
					Environment: "test-environment",
					Description: "Bootstrapped via topicctl bootstrap",
				},
				Spec: TopicSpec{
					Partitions:        2,
					ReplicationFactor: 3,
					PlacementConfig: TopicPlacementConfig{
						Strategy: PlacementStrategyStatic,
						StaticAssignments: [][]int{
							{1, 2, 3},
							{4, 5},
						},
					},
				},
			},
			expError: true,
		},
		{
			description: "static-in-rack placement invalid partition count",
			topicConfig: TopicConfig{
				Meta: TopicMeta{
					Name:        "test-topic",
					Cluster:     "test-cluster",
					Region:      "test-region",
					Environment: "test-environment",
					Description: "Bootstrapped via topicctl bootstrap",
				},
				Spec: TopicSpec{
					Partitions:        2,
					ReplicationFactor: 3,
					PlacementConfig: TopicPlacementConfig{
						Strategy: PlacementStrategyStaticInRack,
						StaticRackAssignments: []string{
							"rack1",
							"rack2",
							"rack3",
						},
					},
				},
			},
			expError: true,
		},
	}

	for _, testCase := range testCases {
		testCase.topicConfig.SetDefaults()
		err := testCase.topicConfig.Validate(testCase.numRacks)
		if testCase.expError {
			assert.Error(t, err, testCase.description)
		} else {
			assert.NoError(t, err, testCase.description)
		}
	}
}

func TestTopicConfigFromTopicInfo(t *testing.T) {
	type testCase struct {
		description    string
		clusterConfig  ClusterConfig
		topicInfo      admin.TopicInfo
		expTopicConfig TopicConfig
	}

	testCases := []testCase{
		{
			description: "retention valid minutes",
			clusterConfig: ClusterConfig{
				Meta: ClusterMeta{
					Name:        "test-cluster",
					Region:      "test-region",
					Environment: "test-environment",
					Description: "test-description",
				},
			},
			topicInfo: admin.TopicInfo{
				Name: "test-topic",
				Config: map[string]string{
					"cleanup.policy": "compact",
					"retention.ms":   "7200000",
				},
				Partitions: []admin.PartitionInfo{
					{
						Topic:    "test-topic",
						ID:       0,
						Replicas: []int{1, 2, 3},
						ISR:      []int{1, 2},
					},
					{
						Topic:    "test-topic",
						ID:       1,
						Replicas: []int{3, 4, 5},
						ISR:      []int{3, 4, 5},
					},
				},
				Version: 1,
			},
			expTopicConfig: TopicConfig{
				Meta: TopicMeta{
					Name:        "test-topic",
					Cluster:     "test-cluster",
					Region:      "test-region",
					Environment: "test-environment",
					Description: "Bootstrapped via topicctl bootstrap",
				},
				Spec: TopicSpec{
					Partitions:        2,
					ReplicationFactor: 3,
					RetentionMinutes:  120,
					Settings: TopicSettings{
						"cleanup.policy": "compact",
					},
					PlacementConfig: TopicPlacementConfig{
						Strategy: PlacementStrategyAny,
					},
				},
			},
		},
		{
			description: "retention not positive minutes",
			clusterConfig: ClusterConfig{
				Meta: ClusterMeta{
					Name:        "test-cluster",
					Region:      "test-region",
					Environment: "test-environment",
					Description: "test-description",
				},
			},
			topicInfo: admin.TopicInfo{
				Name: "test-topic",
				Config: map[string]string{
					"cleanup.policy": "compact",
					"retention.ms":   "560",
				},
				Partitions: []admin.PartitionInfo{
					{
						Topic:    "test-topic",
						ID:       0,
						Replicas: []int{1, 2, 3},
						ISR:      []int{1, 2},
					},
					{
						Topic:    "test-topic",
						ID:       1,
						Replicas: []int{3, 4, 5},
						ISR:      []int{3, 4, 5},
					},
				},
				Version: 1,
			},
			expTopicConfig: TopicConfig{
				Meta: TopicMeta{
					Name:        "test-topic",
					Cluster:     "test-cluster",
					Region:      "test-region",
					Environment: "test-environment",
					Description: "Bootstrapped via topicctl bootstrap",
				},
				Spec: TopicSpec{
					Partitions:        2,
					ReplicationFactor: 3,
					Settings: TopicSettings{
						"cleanup.policy": "compact",
						"retention.ms":   "560",
					},
					PlacementConfig: TopicPlacementConfig{
						Strategy: PlacementStrategyAny,
					},
				},
			},
		},
		{
			description: "retention empty",
			clusterConfig: ClusterConfig{
				Meta: ClusterMeta{
					Name:        "test-cluster",
					Region:      "test-region",
					Environment: "test-environment",
					Description: "test-description",
				},
			},
			topicInfo: admin.TopicInfo{
				Name:   "test-topic",
				Config: map[string]string{},
				Partitions: []admin.PartitionInfo{
					{
						Topic:    "test-topic",
						ID:       0,
						Replicas: []int{1, 2, 3},
						ISR:      []int{1, 2},
					},
					{
						Topic:    "test-topic",
						ID:       1,
						Replicas: []int{3, 4, 5},
						ISR:      []int{3, 4, 5},
					},
				},
				Version: 1,
			},
			expTopicConfig: TopicConfig{
				Meta: TopicMeta{
					Name:        "test-topic",
					Cluster:     "test-cluster",
					Region:      "test-region",
					Environment: "test-environment",
					Description: "Bootstrapped via topicctl bootstrap",
				},
				Spec: TopicSpec{
					Partitions:        2,
					ReplicationFactor: 3,
					Settings:          TopicSettings{},
					PlacementConfig: TopicPlacementConfig{
						Strategy: PlacementStrategyAny,
					},
				},
			},
		},
	}

	for _, testCase := range testCases {
		topicConfig := TopicConfigFromTopicInfo(
			testCase.clusterConfig,
			testCase.topicInfo,
		)
		assert.Equal(t, testCase.expTopicConfig, topicConfig)
	}
}
