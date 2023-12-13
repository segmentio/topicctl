package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetaValidate(t *testing.T) {
	type testCase struct {
		description string
		meta        ResourceMeta
		expError    bool
	}

	testCases := []testCase{
		{
			description: "valid meta",
			meta: ResourceMeta{
				Name:        "test-topic",
				Cluster:     "test-cluster",
				Region:      "test-region",
				Environment: "test-environment",
				Description: "test-description",
			},
			expError: false,
		},
		{
			description: "meta missing fields",
			meta: ResourceMeta{
				Name:        "test-topic",
				Environment: "test-environment",
				Description: "Bootstrapped via topicctl bootstrap",
			},
			expError: true,
		},
	}

	for _, testCase := range testCases {
		err := testCase.meta.Validate()
		if testCase.expError {
			assert.Error(t, err, testCase.description)
		} else {
			assert.NoError(t, err, testCase.description)
		}
	}
}
