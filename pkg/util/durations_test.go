package util

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPrettyDuration(t *testing.T) {
	type testCase struct {
		duration time.Duration
		expected string
	}

	testCases := []testCase{
		{
			duration: 5 * time.Millisecond,
			expected: "5ms",
		},
		{
			duration: 25*time.Second + 410*time.Millisecond,
			expected: "25s",
		},
		{
			duration: 30*time.Minute + 10*time.Second,
			expected: "30m",
		},
		{
			duration: 60*6*time.Minute + 15*time.Minute,
			expected: "6h",
		},
	}

	for _, testCaseObj := range testCases {
		assert.Equal(
			t,
			testCaseObj.expected,
			PrettyDuration(testCaseObj.duration),
		)
	}
}

func TestPrettyRate(t *testing.T) {
	type testCase struct {
		count    int64
		duration time.Duration
		expected string
	}
	testCases := []testCase{
		{
			count:    300,
			duration: 0,
			expected: "",
		},
		{
			count:    0,
			duration: time.Second,
			expected: "0",
		},
		{
			count:    300,
			duration: time.Second,
			expected: "300/sec",
		},
		{
			count:    3,
			duration: time.Second + 100*time.Millisecond,
			expected: "2.7/sec",
		},
		{
			count:    35,
			duration: time.Minute,
			expected: "35/min",
		},
		{
			count:    3,
			duration: time.Minute,
			expected: "3.0/min",
		},
		{
			count:    3,
			duration: time.Hour,
			expected: "3.0/hour",
		},
		{
			count:    1,
			duration: time.Hour * 1000,
			expected: "~0",
		},
	}

	for _, testCaseObj := range testCases {
		assert.Equal(
			t,
			testCaseObj.expected,
			PrettyRate(testCaseObj.count, testCaseObj.duration),
		)
	}
}
