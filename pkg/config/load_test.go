package config

import (
	"os"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadCluster(t *testing.T) {
	os.Setenv("K2_TEST_ENV_VAR", "test-region")
	defer os.Unsetenv("K2_TEST_ENV_VAR")

	clusterConfig, err := LoadClusterFile("testdata/test-cluster/cluster.yaml", true)
	assert.NoError(t, err)

	// Empty RootDir since this will vary based on where test is run.
	clusterConfig.RootDir = ""

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
	assert.NoError(t, clusterConfig.Validate())

	clusterConfig, err = LoadClusterFile("testdata/test-cluster/cluster-invalid.yaml", true)
	assert.NoError(t, err)
	assert.Error(t, clusterConfig.Validate())

	clusterConfig, err = LoadClusterFile("testdata/test-cluster/cluster-extra-fields.yaml", true)
	assert.Error(t, err)
}

func TestLoadTopicsFile(t *testing.T) {
	topicConfigs, err := LoadTopicsFile("testdata/test-cluster/topics/topic-test.yaml")
	assert.Equal(t, 1, len(topicConfigs))
	topicConfig := topicConfigs[0]
	require.NoError(t, err)
	topicConfig.SetDefaults()

	assert.Equal(
		t,
		TopicConfig{
			Meta: ResourceMeta{
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
	assert.NoError(t, topicConfig.Validate(3))

	topicConfigs, err = LoadTopicsFile("testdata/test-cluster/topics/topic-test-invalid.yaml")
	assert.Equal(t, 1, len(topicConfigs))
	topicConfig = topicConfigs[0]
	require.NoError(t, err)
	assert.Error(t, topicConfig.Validate(3))

	topicConfigs, err = LoadTopicsFile("testdata/test-cluster/topics/topic-test-multi.yaml")
	assert.Equal(t, 2, len(topicConfigs))
	assert.Equal(t, "topic-test1", topicConfigs[0].Meta.Name)
	assert.Equal(t, "topic-test2", topicConfigs[1].Meta.Name)
}

func TestLoadACLsFile(t *testing.T) {
	aclConfigs, err := LoadACLsFile("testdata/test-cluster/acls/acl-test.yaml")
	require.NoError(t, err)
	assert.Equal(t, 1, len(aclConfigs))
	aclConfig := aclConfigs[0]

	assert.Equal(
		t,
		ACLConfig{
			Meta: ResourceMeta{
				Name:        "acl-test",
				Cluster:     "test-cluster",
				Region:      "test-region",
				Environment: "test-env",
				Description: "Test acl\n",
			},
			Spec: ACLSpec{
				ACLs: []ACL{
					{
						Resource: ACLResource{
							Type:        kafka.ResourceTypeTopic,
							Name:        "test-topic",
							PatternType: kafka.PatternTypeLiteral,
							Principal:   "User:Alice",
							Host:        "*",
							Permission:  kafka.ACLPermissionTypeAllow,
						},
						Operations: []kafka.ACLOperationType{
							kafka.ACLOperationTypeRead,
							kafka.ACLOperationTypeDescribe,
						},
					},
					{
						Resource: ACLResource{
							Type:        kafka.ResourceTypeGroup,
							Name:        "test-group",
							PatternType: kafka.PatternTypePrefixed,
							Principal:   "User:Alice",
							Host:        "*",
							Permission:  kafka.ACLPermissionTypeAllow,
						},
						Operations: []kafka.ACLOperationType{
							kafka.ACLOperationTypeRead,
						},
					},
				},
			},
		},
		aclConfig,
	)

	invalidAclConfigs, err := LoadACLsFile("testdata/test-cluster/acls/acl-test-invalid.yaml")
	assert.Equal(t, 0, len(invalidAclConfigs))
	require.Error(t, err)

	multiAclConfigs, err := LoadACLsFile("testdata/test-cluster/acls/acl-test-multi.yaml")
	assert.Equal(t, 2, len(multiAclConfigs))
	assert.Equal(t, "acl-test1", multiAclConfigs[0].Meta.Name)
	assert.Equal(t, "acl-test2", multiAclConfigs[1].Meta.Name)
}

func TestCheckConsistency(t *testing.T) {
	os.Setenv("K2_TEST_ENV_VAR", "test-region")
	defer os.Unsetenv("K2_TEST_ENV_VAR")

	clusterConfig, err := LoadClusterFile("testdata/test-cluster/cluster.yaml", true)
	assert.NoError(t, err)
	assert.NoError(t, clusterConfig.Validate())

	topicConfigs, err := LoadTopicsFile("testdata/test-cluster/topics/topic-test.yaml")
	assert.Equal(t, 1, len(topicConfigs))
	topicConfig := topicConfigs[0]
	topicConfig.SetDefaults()
	assert.NoError(t, err)
	assert.NoError(t, topicConfig.Validate(3))

	topicConfigNoMatchs, err := LoadTopicsFile(
		"testdata/test-cluster/topics/topic-test-no-match.yaml",
	)
	assert.Equal(t, 1, len(topicConfigNoMatchs))
	topicConfigNoMatch := topicConfigNoMatchs[0]
	topicConfigNoMatch.SetDefaults()
	assert.NoError(t, err)
	assert.NoError(t, topicConfig.Validate(3))

	assert.NoError(t, CheckConsistency(topicConfig.Meta, clusterConfig))
	assert.Error(t, CheckConsistency(topicConfigNoMatch.Meta, clusterConfig))

	aclConfigs, err := LoadACLsFile("testdata/test-cluster/acls/acl-test.yaml")
	assert.Equal(t, 1, len(aclConfigs))
	assert.NoError(t, err)

	aclConfigsNoMatches, err := LoadACLsFile("testdata/test-cluster/acls/acl-test-no-match.yaml")
	assert.Equal(t, 1, len(aclConfigsNoMatches))
	assert.NoError(t, err)

	assert.NoError(t, CheckConsistency(aclConfigs[0].Meta, clusterConfig))
	assert.Error(t, CheckConsistency(aclConfigsNoMatches[0].Meta, clusterConfig))
}
