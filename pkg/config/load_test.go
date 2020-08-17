package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadCluster(t *testing.T) {
	clusterConfig, err := LoadClusterFile("testdata/test-cluster/cluster.yaml")
	assert.Nil(t, err)
	assert.Equal(
		t,
		ClusterConfig{
			Meta: ClusterMeta{
				Name:        "test-cluster",
				Region:      "test-region",
				Environment: "test-env",
				Description: "Test cluster\n",
			},
			Spec: ClusterSpec{
				VersionMajor: KafkaVersionMajor010,
				BootstrapAddrs: []string{
					"bootstrap-addr:9092",
				},
				ZKAddrs: []string{
					"zk-addr:2181",
				},
				ZKPrefix:   "/test-cluster-id",
				ZKLockPath: "/topicctl/locks",
			},
		},
		clusterConfig,
	)
	assert.Nil(t, clusterConfig.Validate())

	clusterConfig, err = LoadClusterFile("testdata/test-cluster/cluster-invalid.yaml")
	assert.Nil(t, err)
	assert.NotNil(t, clusterConfig.Validate())
}

func TestLoadTopic(t *testing.T) {
	topicConfig, err := LoadTopicFile("testdata/test-cluster/topics/topic-test.yaml")
	require.Nil(t, err)
	topicConfig.SetDefaults()

	assert.Equal(
		t,
		TopicConfig{
			Meta: TopicMeta{
				Name:        "topic-test",
				Cluster:     "test-cluster",
				Region:      "test-region",
				Environment: "test-env",
				Description: "Test topic\n",
			},
			Spec: TopicSpec{
				Partitions:        9,
				ReplicationFactor: 2,
				RetentionMinutes:  100,
				PlacementConfig: TopicPlacementConfig{
					Strategy: PlacementStrategyInRack,
					Picker:   PickerMethodRandomized,
				},
				MigrationConfig: &TopicMigrationConfig{
					PartitionBatchSize: 1,
				},
				Settings: TopicSettings{
					"cleanup.policy": "compact",
					"follower.replication.throttled.replicas": []interface{}{
						"1:3",
						"4:5",
					},
					"max.compaction.lag.ms": 12345.0,
				},
			},
		},
		topicConfig,
	)
	assert.Nil(t, topicConfig.Validate(3))

	topicConfig, err = LoadTopicFile("testdata/test-cluster/topics/topic-test-invalid.yaml")
	require.Nil(t, err)
	assert.NotNil(t, topicConfig.Validate(3))
}

func TestCheckConsistency(t *testing.T) {
	clusterConfig, err := LoadClusterFile("testdata/test-cluster/cluster.yaml")
	assert.Nil(t, err)
	assert.Nil(t, clusterConfig.Validate())

	topicConfig, err := LoadTopicFile("testdata/test-cluster/topics/topic-test.yaml")
	topicConfig.SetDefaults()
	assert.Nil(t, err)
	assert.Nil(t, topicConfig.Validate(3))

	topicConfigNoMatch, err := LoadTopicFile(
		"testdata/test-cluster/topics/topic-test-no-match.yaml",
	)
	topicConfigNoMatch.SetDefaults()
	assert.Nil(t, err)
	assert.Nil(t, topicConfig.Validate(3))

	assert.Nil(t, CheckConsistency(topicConfig, clusterConfig))
	assert.NotNil(t, CheckConsistency(topicConfigNoMatch, clusterConfig))
}
