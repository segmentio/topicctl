package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUserValidate(t *testing.T) {
	type testCase struct {
		description string
		userConfig  UserConfig
		expError    bool
	}

	testCases := []testCase{
		{
			description: "happy path",
			userConfig: UserConfig{
				Meta: UserMeta{
					Name:        "test-user",
					Cluster:     "test-cluster",
					Region:      "test-region",
					Environment: "test-environment",
					Description: "Bootstrapped via topicctl bootstrap",
				},
				Spec: UserSpec{
					Authentication: AuthenticationConfig{
						Type:     "scram-sha-512",
						Password: "test-password",
					},
					Authorization: AuthorizationConfig{
						Type: "simple",
						ACLs: []ACL{
							{
								Resource: ACLResource{
									Type:        "topic",
									Name:        "test-topic",
									PatternType: "literal",
									Principal:   "User:alice",
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
			},
			expError: false,
		},
		{
			description: "missing meta fields",
			userConfig: UserConfig{
				Meta: UserMeta{
					Name:    "test-user",
					Cluster: "test-cluster",
					Region:  "test-region",
				},
				Spec: UserSpec{
					Authentication: AuthenticationConfig{
						Type:     "scram-sha-512",
						Password: "test-password",
					},
					Authorization: AuthorizationConfig{
						Type: "simple",
						ACLs: []ACL{
							{
								Resource: ACLResource{
									Type:        "topic",
									Name:        "test-topic",
									PatternType: "literal",
									Principal:   "User:alice",
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
			},
			expError: true,
		},
		{
			description: "invalid authentication type",
			userConfig: UserConfig{
				Meta: UserMeta{
					Name:        "test-user",
					Cluster:     "test-cluster",
					Region:      "test-region",
					Environment: "test-environment",
					Description: "Bootstrapped via topicctl bootstrap",
				},
				Spec: UserSpec{
					Authentication: AuthenticationConfig{
						Type:     "invalid",
						Password: "test-password",
					},
					Authorization: AuthorizationConfig{
						Type: "simple",
						ACLs: []ACL{
							{
								Resource: ACLResource{
									Type:        "topic",
									Name:        "test-topic",
									PatternType: "literal",
									Principal:   "User:alice",
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
			},
			expError: true,
		},
		{
			description: "invalid authorization type",
			userConfig: UserConfig{
				Meta: UserMeta{
					Name:        "test-user",
					Cluster:     "test-cluster",
					Region:      "test-region",
					Environment: "test-environment",
					Description: "Bootstrapped via topicctl bootstrap",
				},
				Spec: UserSpec{
					Authentication: AuthenticationConfig{
						Type:     "scram-sha-512",
						Password: "test-password",
					},
					Authorization: AuthorizationConfig{
						Type: "invalid",
						ACLs: []ACL{
							{
								Resource: ACLResource{
									Type:        "topic",
									Name:        "test-topic",
									PatternType: "literal",
									Principal:   "User:alice",
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
			},
			expError: true,
		},
		{
			description: "invalid ACL resource type",
			userConfig: UserConfig{
				Meta: UserMeta{
					Name:        "test-user",
					Cluster:     "test-cluster",
					Region:      "test-region",
					Environment: "test-environment",
					Description: "Bootstrapped via topicctl bootstrap",
				},
				Spec: UserSpec{
					Authentication: AuthenticationConfig{
						Type:     "scram-sha-512",
						Password: "test-password",
					},
					Authorization: AuthorizationConfig{
						Type: "simple",
						ACLs: []ACL{
							{
								Resource: ACLResource{
									Type:        "invalid",
									Name:        "test-topic",
									PatternType: "literal",
									Principal:   "User:alice",
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
			},
			expError: true,
		},
		{
			description: "invalid ACL resource pattern type",
			userConfig: UserConfig{
				Meta: UserMeta{
					Name:        "test-user",
					Cluster:     "test-cluster",
					Region:      "test-region",
					Environment: "test-environment",
					Description: "Bootstrapped via topicctl bootstrap",
				},
				Spec: UserSpec{
					Authentication: AuthenticationConfig{
						Type:     "scram-sha-512",
						Password: "test-password",
					},
					Authorization: AuthorizationConfig{
						Type: "simple",
						ACLs: []ACL{
							{
								Resource: ACLResource{
									Type:        "topic",
									Name:        "test-topic",
									PatternType: "invalid",
									Principal:   "User:alice",
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
			},
			expError: true,
		},
		{
			description: "invalid ACL operation type",
			userConfig: UserConfig{
				Meta: UserMeta{
					Name:        "test-user",
					Cluster:     "test-cluster",
					Region:      "test-region",
					Environment: "test-environment",
					Description: "Bootstrapped via topicctl bootstrap",
				},
				Spec: UserSpec{
					Authentication: AuthenticationConfig{
						Type:     "scram-sha-512",
						Password: "test-password",
					},
					Authorization: AuthorizationConfig{
						Type: "simple",
						ACLs: []ACL{
							{
								Resource: ACLResource{
									Type:        "topic",
									Name:        "test-topic",
									PatternType: "literal",
									Principal:   "User:alice",
									Host:        "*",
								},
								Operations: []string{
									"invalid",
									"describe",
								},
							},
						},
					},
				},
			},
			expError: true,
		},
	}

	for _, testCase := range testCases {
		err := testCase.userConfig.Validate()
		if testCase.expError {
			assert.Error(t, err, testCase.description)
		} else {
			assert.NoError(t, err, testCase.description)
		}
	}
}

func TestUserConfigFromUserInfo(t *testing.T) {
	t.Fatal("implement me")
}
