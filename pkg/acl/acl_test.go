package acl

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/config"
	"github.com/segmentio/topicctl/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestCreateNewACLs(t *testing.T) {
	if !util.CanTestBrokerAdminSecurity() {
		t.Skip("Skipping because KAFKA_TOPICS_TEST_BROKER_ADMIN_SECURITY is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	principal := util.RandomString("User:acl-create-", 6)
	topicName := util.RandomString("acl-create-", 6)

	aclConfig := config.ACLConfig{
		Meta: config.ResourceMeta{
			Name:        "test-acl",
			Cluster:     "test-cluster",
			Region:      "test-region",
			Environment: "test-environment",
		},
		Spec: config.ACLSpec{
			ACLs: []config.ACL{
				{
					Resource: config.ACLResource{
						Type:        kafka.ResourceTypeTopic,
						Name:        topicName,
						PatternType: kafka.PatternTypeLiteral,
						Principal:   principal,
						Host:        "*",
						Permission:  kafka.ACLPermissionTypeAllow,
					},
					Operations: []kafka.ACLOperationType{
						kafka.ACLOperationTypeRead,
					},
				},
			},
		},
	}
	aclAdmin := testACLAdmin(ctx, t, aclConfig)
	defer aclAdmin.adminClient.Close()

	defer func() {
		_, err := aclAdmin.adminClient.GetConnector().KafkaClient.DeleteACLs(ctx,
			&kafka.DeleteACLsRequest{
				Filters: []kafka.DeleteACLsFilter{
					{
						ResourceTypeFilter:        kafka.ResourceTypeTopic,
						ResourceNameFilter:        topicName,
						ResourcePatternTypeFilter: kafka.PatternTypeLiteral,
						PrincipalFilter:           principal,
						HostFilter:                "*",
						PermissionType:            kafka.ACLPermissionTypeAllow,
						Operation:                 kafka.ACLOperationTypeRead,
					},
				},
			},
		)

		if err != nil {
			t.Fatal(fmt.Errorf("failed to clean up ACL, err: %v", err))
		}
	}()
	err := aclAdmin.Create(ctx)
	require.NoError(t, err)
	acl, err := aclAdmin.adminClient.GetACLs(ctx, kafka.ACLFilter{
		ResourceTypeFilter:        kafka.ResourceTypeTopic,
		ResourceNameFilter:        topicName,
		ResourcePatternTypeFilter: kafka.PatternTypeLiteral,
		PrincipalFilter:           principal,
		HostFilter:                "*",
		PermissionType:            kafka.ACLPermissionTypeAllow,
		Operation:                 kafka.ACLOperationTypeRead,
	})
	require.NoError(t, err)
	require.Equal(t, []admin.ACLInfo{
		{
			ResourceType:   admin.ResourceType(kafka.ResourceTypeTopic),
			ResourceName:   topicName,
			PatternType:    admin.PatternType(kafka.PatternTypeLiteral),
			Principal:      principal,
			Host:           "*",
			Operation:      admin.ACLOperationType(kafka.ACLOperationTypeRead),
			PermissionType: admin.ACLPermissionType(kafka.ACLPermissionTypeAllow),
		},
	}, acl)
}

func TestCreateExistingACLs(t *testing.T) {
	if !util.CanTestBrokerAdminSecurity() {
		t.Skip("Skipping because KAFKA_TOPICS_TEST_BROKER_ADMIN_SECURITY is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	principal := util.RandomString("User:acl-create-", 6)
	topicName := util.RandomString("acl-create-", 6)

	aclConfig := config.ACLConfig{
		Meta: config.ResourceMeta{
			Name:        "test-acl",
			Cluster:     "test-cluster",
			Region:      "test-region",
			Environment: "test-environment",
		},
		Spec: config.ACLSpec{
			ACLs: []config.ACL{
				{
					Resource: config.ACLResource{
						Type:        kafka.ResourceTypeTopic,
						Name:        topicName,
						PatternType: kafka.PatternTypeLiteral,
						Principal:   principal,
						Host:        "*",
						Permission:  kafka.ACLPermissionTypeAllow,
					},
					Operations: []kafka.ACLOperationType{
						kafka.ACLOperationTypeRead,
					},
				},
			},
		},
	}
	aclAdmin := testACLAdmin(ctx, t, aclConfig)
	defer aclAdmin.adminClient.Close()

	defer func() {
		_, err := aclAdmin.adminClient.GetConnector().KafkaClient.DeleteACLs(ctx,
			&kafka.DeleteACLsRequest{
				Filters: []kafka.DeleteACLsFilter{
					{
						ResourceTypeFilter:        kafka.ResourceTypeTopic,
						ResourceNameFilter:        topicName,
						ResourcePatternTypeFilter: kafka.PatternTypeLiteral,
						PrincipalFilter:           principal,
						HostFilter:                "*",
						PermissionType:            kafka.ACLPermissionTypeAllow,
						Operation:                 kafka.ACLOperationTypeRead,
					},
				},
			},
		)

		if err != nil {
			t.Fatal(fmt.Errorf("failed to clean up ACL, err: %v", err))
		}
	}()
	err := aclAdmin.Create(ctx)
	require.NoError(t, err)
	acl, err := aclAdmin.adminClient.GetACLs(ctx, kafka.ACLFilter{
		ResourceTypeFilter:        kafka.ResourceTypeTopic,
		ResourceNameFilter:        topicName,
		ResourcePatternTypeFilter: kafka.PatternTypeLiteral,
		PrincipalFilter:           principal,
		HostFilter:                "*",
		PermissionType:            kafka.ACLPermissionTypeAllow,
		Operation:                 kafka.ACLOperationTypeRead,
	})
	require.NoError(t, err)
	require.Equal(t, []admin.ACLInfo{
		{
			ResourceType:   admin.ResourceType(kafka.ResourceTypeTopic),
			ResourceName:   topicName,
			PatternType:    admin.PatternType(kafka.PatternTypeLiteral),
			Principal:      principal,
			Host:           "*",
			Operation:      admin.ACLOperationType(kafka.ACLOperationTypeRead),
			PermissionType: admin.ACLPermissionType(kafka.ACLPermissionTypeAllow),
		},
	}, acl)
	// Run create again and make sure it is idempotent
	err = aclAdmin.Create(ctx)
	require.NoError(t, err)
	acl, err = aclAdmin.adminClient.GetACLs(ctx, kafka.ACLFilter{
		ResourceTypeFilter:        kafka.ResourceTypeTopic,
		ResourceNameFilter:        topicName,
		ResourcePatternTypeFilter: kafka.PatternTypeLiteral,
		PrincipalFilter:           principal,
		HostFilter:                "*",
		PermissionType:            kafka.ACLPermissionTypeAllow,
		Operation:                 kafka.ACLOperationTypeRead,
	})
	require.NoError(t, err)
	require.Equal(t, []admin.ACLInfo{
		{
			ResourceType:   admin.ResourceType(kafka.ResourceTypeTopic),
			ResourceName:   topicName,
			PatternType:    admin.PatternType(kafka.PatternTypeLiteral),
			Principal:      principal,
			Host:           "*",
			Operation:      admin.ACLOperationType(kafka.ACLOperationTypeRead),
			PermissionType: admin.ACLPermissionType(kafka.ACLPermissionTypeAllow),
		},
	}, acl)
}

func TestCreateACLsDryRun(t *testing.T) {
	if !util.CanTestBrokerAdminSecurity() {
		t.Skip("Skipping because KAFKA_TOPICS_TEST_BROKER_ADMIN_SECURITY is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	principal := util.RandomString("User:acl-create-", 6)
	topicName := util.RandomString("acl-create-", 6)

	aclConfig := config.ACLConfig{
		Meta: config.ResourceMeta{
			Name:        "test-acl",
			Cluster:     "test-cluster",
			Region:      "test-region",
			Environment: "test-environment",
		},
		Spec: config.ACLSpec{
			ACLs: []config.ACL{
				{
					Resource: config.ACLResource{
						Type:        kafka.ResourceTypeTopic,
						Name:        topicName,
						PatternType: kafka.PatternTypeLiteral,
						Principal:   principal,
						Host:        "*",
						Permission:  kafka.ACLPermissionTypeAllow,
					},
					Operations: []kafka.ACLOperationType{
						kafka.ACLOperationTypeRead,
					},
				},
			},
		},
	}
	aclAdmin := testACLAdmin(ctx, t, aclConfig)
	defer aclAdmin.adminClient.Close()
	aclAdmin.config.DryRun = true

	defer func() {
		_, err := aclAdmin.adminClient.GetConnector().KafkaClient.DeleteACLs(ctx,
			&kafka.DeleteACLsRequest{
				Filters: []kafka.DeleteACLsFilter{
					{
						ResourceTypeFilter:        kafka.ResourceTypeTopic,
						ResourceNameFilter:        topicName,
						ResourcePatternTypeFilter: kafka.PatternTypeLiteral,
						PrincipalFilter:           principal,
						HostFilter:                "*",
						PermissionType:            kafka.ACLPermissionTypeAllow,
						Operation:                 kafka.ACLOperationTypeRead,
					},
				},
			},
		)

		if err != nil {
			t.Fatal(fmt.Errorf("failed to clean up ACL, err: %v", err))
		}
	}()
	err := aclAdmin.Create(ctx)
	require.NoError(t, err)
	acl, err := aclAdmin.adminClient.GetACLs(ctx, kafka.ACLFilter{
		ResourceTypeFilter:        kafka.ResourceTypeTopic,
		ResourceNameFilter:        topicName,
		ResourcePatternTypeFilter: kafka.PatternTypeLiteral,
		PrincipalFilter:           principal,
		HostFilter:                "*",
		PermissionType:            kafka.ACLPermissionTypeAllow,
		Operation:                 kafka.ACLOperationTypeRead,
	})
	require.NoError(t, err)
	require.Equal(t, []admin.ACLInfo{}, acl)
}

func testACLAdmin(
	ctx context.Context,
	t *testing.T,
	aclConfig config.ACLConfig,
) *ACLAdmin {
	clusterConfig := config.ClusterConfig{
		Meta: config.ClusterMeta{
			Name:        "test-cluster",
			Region:      "test-region",
			Environment: "test-environment",
		},
		Spec: config.ClusterSpec{
			BootstrapAddrs: []string{util.TestKafkaAddr()},
			ZKLockPath:     "/topicctl/locks",
		},
	}

	adminClient, err := clusterConfig.NewAdminClient(ctx, nil, false, "", "")
	require.NoError(t, err)

	aclAdmin, err := NewACLAdmin(
		ctx,
		adminClient,
		ACLAdminConfig{
			ClusterConfig: clusterConfig,
			ACLConfig:     aclConfig,
			DryRun:        false,
			SkipConfirm:   true,
		},
	)
	require.NoError(t, err)
	return aclAdmin
}
