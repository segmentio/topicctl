package config

import (
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

func TestACLValidate(t *testing.T) {
	type testCase struct {
		description string
		aclConfig   ACLConfig
		expError    bool
	}

	testCases := []testCase{
		{
			description: "valid ACL config",
			aclConfig: ACLConfig{
				Meta: ResourceMeta{
					Name:        "acl-test",
					Cluster:     "test-cluster",
					Region:      "test-region",
					Environment: "test-env",
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
							},
						},
					},
				},
			},
			expError: false,
		},
		{
			description: "unknown resource type",
			aclConfig: ACLConfig{
				Meta: ResourceMeta{
					Name:        "acl-test",
					Cluster:     "test-cluster",
					Region:      "test-region",
					Environment: "test-env",
				},
				Spec: ACLSpec{
					ACLs: []ACL{
						{
							Resource: ACLResource{
								Type:        kafka.ResourceTypeUnknown,
								Name:        "test-topic",
								PatternType: kafka.PatternTypeLiteral,
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
			expError: true,
		},
		{
			description: "empty resource name",
			aclConfig: ACLConfig{
				Meta: ResourceMeta{
					Name:        "acl-test",
					Cluster:     "test-cluster",
					Region:      "test-region",
					Environment: "test-env",
				},
				Spec: ACLSpec{
					ACLs: []ACL{
						{
							Resource: ACLResource{
								Type:        kafka.ResourceTypeTopic,
								PatternType: kafka.PatternTypeLiteral,
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
			expError: true,
		},
		{
			description: "unknown resource pattern type",
			aclConfig: ACLConfig{
				Meta: ResourceMeta{
					Name:        "acl-test",
					Cluster:     "test-cluster",
					Region:      "test-region",
					Environment: "test-env",
				},
				Spec: ACLSpec{
					ACLs: []ACL{
						{
							Resource: ACLResource{
								Type:        kafka.ResourceTypeTopic,
								PatternType: kafka.PatternTypeUnknown,
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
			expError: true,
		},
		{
			description: "empty principal",
			aclConfig: ACLConfig{
				Meta: ResourceMeta{
					Name:        "acl-test",
					Cluster:     "test-cluster",
					Region:      "test-region",
					Environment: "test-env",
				},
				Spec: ACLSpec{
					ACLs: []ACL{
						{
							Resource: ACLResource{
								Type:        kafka.ResourceTypeTopic,
								PatternType: kafka.PatternTypeUnknown,
								Principal:   "",
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
			expError: true,
		},
		{
			description: "unknown ACL operation type",
			aclConfig: ACLConfig{
				Meta: ResourceMeta{
					Name:        "acl-test",
					Cluster:     "test-cluster",
					Region:      "test-region",
					Environment: "test-env",
				},
				Spec: ACLSpec{
					ACLs: []ACL{
						{
							Resource: ACLResource{
								Type:        kafka.ResourceTypeTopic,
								PatternType: kafka.PatternTypeUnknown,
								Principal:   "",
								Host:        "*",
								Permission:  kafka.ACLPermissionTypeAllow,
							},
							Operations: []kafka.ACLOperationType{
								kafka.ACLOperationTypeUnknown,
							},
						},
					},
				},
			},
			expError: true,
		},
	}

	for _, testCase := range testCases {
		testCase.aclConfig.SetDefaults()
		err := testCase.aclConfig.Validate()
		if testCase.expError {
			assert.Error(t, err, testCase.description)
		} else {
			assert.NoError(t, err, testCase.description)
		}
	}
}

func TestACLSetDefaults(t *testing.T) {
	type testCase struct {
		description string
		aclConfig   ACLConfig
		expConfig   ACLConfig
	}

	testCases := []testCase{
		{
			description: "set defaults",
			aclConfig: ACLConfig{
				Meta: ResourceMeta{
					Name:        "acl-test",
					Cluster:     "test-cluster",
					Region:      "test-region",
					Environment: "test-env",
				},
				Spec: ACLSpec{
					ACLs: []ACL{
						{
							Resource: ACLResource{
								Type:        kafka.ResourceTypeTopic,
								Name:        "test-topic",
								PatternType: kafka.PatternTypeLiteral,
								Principal:   "User:Alice",
								Permission:  kafka.ACLPermissionTypeUnknown,
							},
							Operations: []kafka.ACLOperationType{
								kafka.ACLOperationTypeRead,
							},
						},
					},
				},
			},
			expConfig: ACLConfig{
				Meta: ResourceMeta{
					Name:        "acl-test",
					Cluster:     "test-cluster",
					Region:      "test-region",
					Environment: "test-env",
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
							},
						},
					},
				},
			},
		},
	}

	for _, testCase := range testCases {
		testCase.aclConfig.SetDefaults()
		assert.Equal(t, testCase.expConfig, testCase.aclConfig, testCase.description)
	}
}
