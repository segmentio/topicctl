package extenders

import (
	"testing"

	"github.com/segmentio/topicctl/pkg/admin"
)

func TestStaticExtender(t *testing.T) {
	extender := &StaticExtender{
		Assignments: admin.ReplicasToAssignments(
			[][]int{
				{1, 2, 3},
				{3, 4, 5},
				{5, 6, 7},
				{8, 9, 10},
			},
		),
	}

	testCases := []extenderTestCase{
		{
			curr: [][]int{
				{1, 2, 3},
				{2, 4, 5},
			},
			expected: [][]int{
				{1, 2, 3},
				{3, 4, 5},
				{5, 6, 7},
				{8, 9, 10},
			},
			extraPartitions: 2,
		},
	}

	for _, testCase := range testCases {
		testCase.evaluate(t, extender)
	}
}
