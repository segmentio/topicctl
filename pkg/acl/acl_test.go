package acl

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
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

func TestDeleteACLs(t *testing.T) {
	if !util.CanTestBrokerAdminSecurity() {
		t.Skip("Skipping because KAFKA_TOPICS_TEST_BROKER_ADMIN_SECURITY is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	principal := util.RandomString("User:acl-delete-", 6)
	topicName := util.RandomString("acl-delete-", 6)

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

	err = aclAdmin.Delete(ctx, kafka.DeleteACLsFilter{
		ResourceTypeFilter:        kafka.ResourceTypeTopic,
		ResourceNameFilter:        topicName,
		ResourcePatternTypeFilter: kafka.PatternTypeLiteral,
		PrincipalFilter:           principal,
		HostFilter:                "*",
		PermissionType:            kafka.ACLPermissionTypeAllow,
		Operation:                 kafka.ACLOperationTypeRead,
	})
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
	require.Equal(t, []admin.ACLInfo{}, acl)
}

func TestDeleteMultipleACLs(t *testing.T) {
	if !util.CanTestBrokerAdminSecurity() {
		t.Skip("Skipping because KAFKA_TOPICS_TEST_BROKER_ADMIN_SECURITY is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	principal := util.RandomString("User:acl-delete-multi-", 6)
	topicName := util.RandomString("acl-delete-multi-", 6)

	// Create 3 ACLs, two for topics and one for groups
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
				{
					Resource: config.ACLResource{
						Type:        kafka.ResourceTypeTopic,
						Name:        topicName,
						PatternType: kafka.PatternTypeLiteral,
						Principal:   principal,
						Host:        "*",
						Permission:  kafka.ACLPermissionTypeDeny,
					},
					Operations: []kafka.ACLOperationType{
						kafka.ACLOperationTypeRead,
					},
				},
				{
					Resource: config.ACLResource{
						Type:        kafka.ResourceTypeGroup,
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

	err := aclAdmin.Create(ctx)
	require.NoError(t, err)

	defer func() {
		_, err := aclAdmin.adminClient.GetConnector().KafkaClient.DeleteACLs(ctx,
			&kafka.DeleteACLsRequest{
				Filters: []kafka.DeleteACLsFilter{
					{
						ResourceTypeFilter:        kafka.ResourceTypeGroup,
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
	acl, err := aclAdmin.adminClient.GetACLs(ctx, kafka.ACLFilter{
		ResourceTypeFilter:        kafka.ResourceTypeTopic,
		ResourceNameFilter:        topicName,
		ResourcePatternTypeFilter: kafka.PatternTypeLiteral,
		PrincipalFilter:           principal,
		HostFilter:                "*",
		PermissionType:            kafka.ACLPermissionTypeAny,
		Operation:                 kafka.ACLOperationTypeRead,
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []admin.ACLInfo{
		{
			ResourceType:   admin.ResourceType(kafka.ResourceTypeTopic),
			ResourceName:   topicName,
			PatternType:    admin.PatternType(kafka.PatternTypeLiteral),
			Principal:      principal,
			Host:           "*",
			Operation:      admin.ACLOperationType(kafka.ACLOperationTypeRead),
			PermissionType: admin.ACLPermissionType(kafka.ACLPermissionTypeAllow),
		},
		{
			ResourceType:   admin.ResourceType(kafka.ResourceTypeTopic),
			ResourceName:   topicName,
			PatternType:    admin.PatternType(kafka.PatternTypeLiteral),
			Principal:      principal,
			Host:           "*",
			Operation:      admin.ACLOperationType(kafka.ACLOperationTypeRead),
			PermissionType: admin.ACLPermissionType(kafka.ACLPermissionTypeDeny),
		},
	}, acl)

	acl, err = aclAdmin.adminClient.GetACLs(ctx, kafka.ACLFilter{
		ResourceTypeFilter:        kafka.ResourceTypeGroup,
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
			ResourceType:   admin.ResourceType(kafka.ResourceTypeGroup),
			ResourceName:   topicName,
			PatternType:    admin.PatternType(kafka.PatternTypeLiteral),
			Principal:      principal,
			Host:           "*",
			Operation:      admin.ACLOperationType(kafka.ACLOperationTypeRead),
			PermissionType: admin.ACLPermissionType(kafka.ACLPermissionTypeAllow),
		},
	}, acl)

	// Delete the two topic ACLs
	err = aclAdmin.Delete(ctx, kafka.DeleteACLsFilter{
		ResourceTypeFilter:        kafka.ResourceTypeTopic,
		ResourceNameFilter:        topicName,
		ResourcePatternTypeFilter: kafka.PatternTypeLiteral,
		PrincipalFilter:           principal,
		HostFilter:                "*",
		PermissionType:            kafka.ACLPermissionTypeAllow,
		Operation:                 kafka.ACLOperationTypeAny,
	})
	require.NoError(t, err)
	acl, err = aclAdmin.adminClient.GetACLs(ctx, kafka.ACLFilter{
		ResourceTypeFilter:        kafka.ResourceTypeTopic,
		ResourceNameFilter:        topicName,
		ResourcePatternTypeFilter: kafka.PatternTypeLiteral,
		PrincipalFilter:           principal,
		HostFilter:                "*",
		PermissionType:            kafka.ACLPermissionTypeAllow,
		Operation:                 kafka.ACLOperationTypeAny,
	})
	require.NoError(t, err)
	require.Equal(t, []admin.ACLInfo{}, acl)

	// Verify the group ACL remains
	acl, err = aclAdmin.adminClient.GetACLs(ctx, kafka.ACLFilter{
		ResourceTypeFilter:        kafka.ResourceTypeGroup,
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
			ResourceType:   admin.ResourceType(kafka.ResourceTypeGroup),
			ResourceName:   topicName,
			PatternType:    admin.PatternType(kafka.PatternTypeLiteral),
			Principal:      principal,
			Host:           "*",
			Operation:      admin.ACLOperationType(kafka.ACLOperationTypeRead),
			PermissionType: admin.ACLPermissionType(kafka.ACLPermissionTypeAllow),
		},
	}, acl)
}

func TestDeleteACLDoesNotExist(t *testing.T) {
	if !util.CanTestBrokerAdminSecurity() {
		t.Skip("Skipping because KAFKA_TOPICS_TEST_BROKER_ADMIN_SECURITY is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	principal := util.RandomString("User:acl-delete-", 6)
	topicName := util.RandomString("acl-delete-", 6)

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
						ResourceNameFilter:        "does-not-exist",
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

	err = aclAdmin.Delete(ctx, kafka.DeleteACLsFilter{
		ResourceTypeFilter:        kafka.ResourceTypeTopic,
		ResourceNameFilter:        "does-not-exist",
		ResourcePatternTypeFilter: kafka.PatternTypeLiteral,
		PrincipalFilter:           principal,
		HostFilter:                "*",
		PermissionType:            kafka.ACLPermissionTypeAllow,
		Operation:                 kafka.ACLOperationTypeRead,
	})
	require.Error(t, err)
	// ACL still exists
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

	adminClient, err := clusterConfig.NewAdminClient(ctx, aws.Config{}, config.AdminClientOpts{})
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
