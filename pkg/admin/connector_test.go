package admin

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseARN(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    ARN
		expectError bool
	}{
		{
			name:  "valid secrets manager ARN",
			input: "arn:aws:secretsmanager:us-west-2:123456789012:secret:mysecret-AbCdEf",
			expected: ARN{
				Partition: "aws",
				Service:   "secretsmanager",
				Region:    "us-west-2",
				AccountID: "123456789012",
				Resource:  "secret:mysecret-AbCdEf",
			},
			expectError: false,
		},
		{
			name:  "ARN with colons in resource",
			input: "arn:aws:secretsmanager:us-west-2:123456789012:secret:my-secret:version-123",
			expected: ARN{
				Partition: "aws",
				Service:   "secretsmanager",
				Region:    "us-west-2",
				AccountID: "123456789012",
				Resource:  "secret:my-secret:version-123",
			},
			expectError: false,
		},
		{
			name:        "invalid ARN prefix",
			input:       "invalid:aws:secretsmanager:us-west-2:123456789012:secret:test",
			expectError: true,
		},
		{
			name:        "too few parts",
			input:       "arn:aws:secretsmanager",
			expectError: true,
		},
		{
			name:        "empty string",
			input:       "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseARN(tt.input)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}
