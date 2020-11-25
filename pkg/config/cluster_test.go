package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClusterValidate(t *testing.T) {
	type testCase struct {
		description   string
		clusterConfig ClusterConfig
		expError      bool
	}

	testCases := []testCase{
		{
			description: "all good",
			clusterConfig: ClusterConfig{
				Meta: ClusterMeta{
					Name:        "test-cluster",
					Region:      "test-region",
					Environment: "test-environment",
					Description: "test-description",
				},
				Spec: ClusterSpec{
					BootstrapAddrs:                      []string{"broker-addr"},
					ZKAddrs:                             []string{"zk-addr"},
					VersionMajor:                        "v2",
					DefaultRetentionDropStepDurationStr: "5m",
				},
			},
			expError: false,
		},
		{
			description: "missing meta fields",
			clusterConfig: ClusterConfig{
				Meta: ClusterMeta{
					Environment: "test-environment",
					Description: "test-description",
				},
				Spec: ClusterSpec{
					BootstrapAddrs: []string{"broker-addr"},
					ZKAddrs:        []string{"zk-addr"},
					VersionMajor:   "v2",
				},
			},
			expError: true,
		},
		{
			description: "missing bootstrap addresses",
			clusterConfig: ClusterConfig{
				Meta: ClusterMeta{
					Name:        "test-cluster",
					Region:      "test-region",
					Environment: "test-environment",
					Description: "test-description",
				},
				Spec: ClusterSpec{
					ZKAddrs:      []string{"zk-addr"},
					VersionMajor: "v2",
				},
			},
			expError: true,
		},
		{
			description: "missing zk addresses",
			clusterConfig: ClusterConfig{
				Meta: ClusterMeta{
					Name:        "test-cluster",
					Region:      "test-region",
					Environment: "test-environment",
					Description: "test-description",
				},
				Spec: ClusterSpec{
					BootstrapAddrs: []string{"broker-addr"},
					VersionMajor:   "v2",
				},
			},
			expError: false,
		},
		{
			description: "bad retention drop format",
			clusterConfig: ClusterConfig{
				Meta: ClusterMeta{
					Name:        "test-cluster",
					Region:      "test-region",
					Environment: "test-environment",
					Description: "test-description",
				},
				Spec: ClusterSpec{
					BootstrapAddrs:                      []string{"broker-addr"},
					ZKAddrs:                             []string{"zk-addr"},
					VersionMajor:                        "v2",
					DefaultRetentionDropStepDurationStr: "10xxx",
				},
			},
			expError: true,
		},
	}

	for _, testCase := range testCases {
		err := testCase.clusterConfig.Validate()
		if testCase.expError {
			assert.Error(t, err, testCase.description)
		} else {
			assert.NoError(t, err, testCase.description)
		}
	}
}
