package config

import (
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

func TestUserConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		userConfig  UserConfig
		shouldError bool
		errorCount  int
	}{
		{
			name: "valid user config with SHA256",
			userConfig: UserConfig{
				Meta: ResourceMeta{
					Name:        "test-user",
					Cluster:     "test-cluster",
					Region:      "us-west-2",
					Environment: "dev",
				},
				Spec: UserSpec{
					Name:       "test-user",
					Mechanism:  "scram-sha-256",
					Password:   "secure-password-123",
					Iterations: 4096,
				},
			},
			shouldError: false,
		},
		{
			name: "valid user config with SHA512",
			userConfig: UserConfig{
				Meta: ResourceMeta{
					Name:        "test-user",
					Cluster:     "test-cluster",
					Region:      "us-west-2",
					Environment: "dev",
				},
				Spec: UserSpec{
					Name:       "test-user",
					Mechanism:  "scram-sha-512",
					Password:   "secure-password-123",
					Iterations: 8192,
				},
			},
			shouldError: false,
		},
		{
			name: "invalid - empty user name",
			userConfig: UserConfig{
				Meta: ResourceMeta{
					Name:        "test-user",
					Cluster:     "test-cluster",
					Region:      "us-west-2",
					Environment: "dev",
				},
				Spec: UserSpec{
					Name:       "",
					Mechanism:  "scram-sha-256",
					Password:   "secure-password-123",
					Iterations: 4096,
				},
			},
			shouldError: true,
			errorCount:  1,
		},
		{
			name: "invalid - empty password",
			userConfig: UserConfig{
				Meta: ResourceMeta{
					Name:        "test-user",
					Cluster:     "test-cluster",
					Region:      "us-west-2",
					Environment: "dev",
				},
				Spec: UserSpec{
					Name:       "test-user",
					Mechanism:  "scram-sha-256",
					Password:   "",
					Iterations: 4096,
				},
			},
			shouldError: true,
			errorCount:  1,
		},
		{
			name: "invalid - unsupported mechanism",
			userConfig: UserConfig{
				Meta: ResourceMeta{
					Name:        "test-user",
					Cluster:     "test-cluster",
					Region:      "us-west-2",
					Environment: "dev",
				},
				Spec: UserSpec{
					Name:       "test-user",
					Mechanism:  "invalid-mechanism",
					Password:   "secure-password-123",
					Iterations: 4096,
				},
			},
			shouldError: true,
			errorCount:  1,
		},
		{
			name: "invalid - too few iterations",
			userConfig: UserConfig{
				Meta: ResourceMeta{
					Name:        "test-user",
					Cluster:     "test-cluster",
					Region:      "us-west-2",
					Environment: "dev",
				},
				Spec: UserSpec{
					Name:       "test-user",
					Mechanism:  "scram-sha-256",
					Password:   "secure-password-123",
					Iterations: 500,
				},
			},
			shouldError: true,
			errorCount:  1,
		},
		{
			name: "invalid meta - missing required fields",
			userConfig: UserConfig{
				Meta: ResourceMeta{
					Name: "",
					// Missing cluster, region, environment
				},
				Spec: UserSpec{
					Name:       "test-user",
					Mechanism:  "scram-sha-256",
					Password:   "secure-password-123",
					Iterations: 4096,
				},
			},
			shouldError: true,
			errorCount:  4, // name, cluster, region, environment
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.userConfig.Validate()
			if test.shouldError {
				assert.Error(t, err)
				if test.errorCount > 0 {
					// Check that we have the expected number of errors
					assert.Contains(t, err.Error(), "error")
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestUserConfigSetDefaults(t *testing.T) {
	userConfig := UserConfig{
		Meta: ResourceMeta{
			Name:        "test-user",
			Cluster:     "test-cluster",
			Region:      "us-west-2",
			Environment: "dev",
		},
		Spec: UserSpec{
			Name:     "test-user",
			Password: "secure-password-123",
			// Mechanism and Iterations not set - should get defaults
		},
	}

	userConfig.SetDefaults()

	assert.Equal(t, "scram-sha-256", userConfig.Spec.Mechanism)
	assert.Equal(t, 4096, userConfig.Spec.Iterations)
}

func TestUserConfigToScramMechanism(t *testing.T) {
	tests := []struct {
		name      string
		mechanism string
		expected  kafka.ScramMechanism
	}{
		{
			name:      "SHA256 lowercase",
			mechanism: "scram-sha-256",
			expected:  kafka.ScramMechanismSha256,
		},
		{
			name:      "SHA256 uppercase",
			mechanism: "SCRAM-SHA-256",
			expected:  kafka.ScramMechanismSha256,
		},
		{
			name:      "SHA256 with underscores",
			mechanism: "scram_sha_256",
			expected:  kafka.ScramMechanismSha256,
		},
		{
			name:      "SHA512 lowercase",
			mechanism: "scram-sha-512",
			expected:  kafka.ScramMechanismSha512,
		},
		{
			name:      "SHA512 uppercase",
			mechanism: "SCRAM-SHA-512",
			expected:  kafka.ScramMechanismSha512,
		},
		{
			name:      "SHA512 with underscores",
			mechanism: "scram_sha_512",
			expected:  kafka.ScramMechanismSha512,
		},
		{
			name:      "unknown mechanism defaults to SHA256",
			mechanism: "unknown",
			expected:  kafka.ScramMechanismSha256,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			userConfig := UserConfig{
				Spec: UserSpec{
					Mechanism: test.mechanism,
				},
			}
			result := userConfig.ToScramMechanism()
			assert.Equal(t, test.expected, result)
		})
	}
}

func TestLoadUserBytes(t *testing.T) {
	yamlContent := `
meta:
  name: test-user
  cluster: test-cluster
  region: us-west-2
  environment: dev
spec:
  name: test-user
  mechanism: scram-sha-256
  password: test-password
  iterations: 4096
`

	userConfig, err := LoadUserBytes([]byte(yamlContent))
	assert.NoError(t, err)
	assert.Equal(t, "test-user", userConfig.Meta.Name)
	assert.Equal(t, "test-cluster", userConfig.Meta.Cluster)
	assert.Equal(t, "test-user", userConfig.Spec.Name)
	assert.Equal(t, "scram-sha-256", userConfig.Spec.Mechanism)
	assert.Equal(t, "test-password", userConfig.Spec.Password)
	assert.Equal(t, 4096, userConfig.Spec.Iterations)
}
