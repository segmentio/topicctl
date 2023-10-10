package apply

import (
	"context"
	"testing"
	"time"

	"github.com/segmentio/topicctl/pkg/config"
	"github.com/segmentio/topicctl/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TODO: write these tests
func TestApplyNewUser(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	userName := util.RandomString("apply-user-", 6)
	userConfig := config.UserConfig{
		Meta: config.UserMeta{
			Name:        userName,
			Cluster:     "test-cluster",
			Region:      "test-region",
			Environment: "test-environment",
		},
		Spec: config.UserSpec{
			Authentication: config.AuthenticationConfig{
				Type:     "scram-sha-512",
				Password: "test-password",
			},
			Authorization: config.AuthorizationConfig{
				Type: "simple",
				ACLs: []config.ACL{
					{
						Resource: config.ACLResource{
							Type:        "topic",
							Name:        "test-topic",
							PatternType: "literal",
							Host:        "*",
						},
						Operations: []string{
							"read",
							"describe",
						},
					},
				},
			},
		},
	}

	applier := testUserApplier(ctx, t, userConfig)

	defer applier.adminClient.Close()
	err := applier.Apply(ctx)
	require.NoError(t, err)

	userInfo, err := applier.adminClient.GetUsers(ctx, []string{userName})
	require.NoError(t, err)
	assert.Equal(t, 1, len(userInfo))
	assert.Equal(t, userName, userInfo[0].Name)

	// TODO: login with user creds
	clusterConfig := config.ClusterConfig{
		Meta: config.ClusterMeta{
			Name:        "test-cluster",
			Region:      "test-region",
			Environment: "test-environment",
		},

		Spec: config.ClusterSpec{
			BootstrapAddrs: []string{util.TestKafkaAddr()},
			SASL: config.SASLConfig{
				Enabled:   true,
				Mechanism: "scram-sha-512",
				Username:  userName,
				Password:  "test-password",
			},
		},
	}

	adminClient, err := clusterConfig.NewAdminClient(ctx, nil, false, "", "")
	require.NoError(t, err)
	_, err = adminClient.GetUsers(ctx, []string{userName})
	require.NoError(t, err)
	// TODO: check acls
	// TODO: check empty host gets coalesced to "*"
}

func TestApplyExistingUser(t *testing.T) {}

func TestApplyUserDryRun(t *testing.T) {}

func testUserApplier(
	ctx context.Context,
	t *testing.T,
	userConfig config.UserConfig,
) *UserApplier {
	clusterConfig := config.ClusterConfig{
		Meta: config.ClusterMeta{
			Name:        "test-cluster",
			Region:      "test-region",
			Environment: "test-environment",
		},

		Spec: config.ClusterSpec{
			BootstrapAddrs: []string{util.TestKafkaAddr()},
			//ZKAddrs:        []string{util.TestZKAddr()},
			ZKLockPath: "/topicctl/locks",
		},
	}

	adminClient, err := clusterConfig.NewAdminClient(ctx, nil, false, "", "")
	require.NoError(t, err)

	applier, err := NewUserApplier(
		ctx,
		adminClient,
		UserApplierConfig{
			ClusterConfig: clusterConfig,
			UserConfig:    userConfig,
			DryRun:        false,
			SkipConfirm:   true,
		},
	)
	require.NoError(t, err)
	return applier
}

// create a function that lists all users and deletes them
func TestDeleteUser(t *testing.T) {
	// create a user

	// delete the user
}
