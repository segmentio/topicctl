package assigners

import (
	"testing"

	"github.com/efcloud/topicctl/pkg/admin"
)

func TestStaticAssigner(t *testing.T) {
	assigner := &StaticAssigner{
		Assignments: admin.ReplicasToAssignments(
			[][]int{
				{1, 2, 3},
				{3, 4, 5},
				{5, 6, 7},
			},
		),
	}

	testCases := []assignerTestCase{
		{
			curr: [][]int{
				{1, 2, 3},
				{2, 4, 5},
			},
			expected: [][]int{
				{1, 2, 3},
				{3, 4, 5},
				{5, 6, 7},
			},
		},
	}

	for _, testCase := range testCases {
		testCase.evaluate(t, assigner)
	}
}
