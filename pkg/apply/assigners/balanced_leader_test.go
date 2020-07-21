package assigners

import (
	"testing"

	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/apply/pickers"
	"github.com/segmentio/topicctl/pkg/config"
)

func TestBalancedLeaderAssigner(t *testing.T) {
	brokers := testBrokers(12, 3)
	assigner := NewBalancedLeaderAssigner(brokers, pickers.NewLowestIndexPicker())
	checker := func(result []admin.PartitionAssignment) bool {
		ok, _ := EvaluateAssignments(
			result,
			brokers,
			config.TopicPlacementConfig{
				Strategy: config.PlacementStrategyBalancedLeaders,
			},
		)
		return ok
	}

	testCases := []assignerTestCase{
		{
			description: "Already balanced",
			curr: [][]int{
				{1, 2, 3},
				{2, 4, 5},
				{3, 6, 7},
			},
			expected: [][]int{
				{1, 2, 3},
				{2, 4, 5},
				{3, 6, 7},
			},
			checker: checker,
		},
		{
			description: "Single swap",
			curr: [][]int{
				{1, 2, 3},
				{2, 4, 5},
				{5, 6, 7},
			},
			expected: [][]int{
				{1, 2, 3},
				{2, 4, 5},
				// Single swap
				{6, 5, 7},
			},
			checker: checker,
		},
		{
			description: "Multiple swaps",
			curr: [][]int{
				{1, 2, 3},
				{2, 3, 4},
				{3, 4, 5},
				{1, 2, 3},
				{2, 3, 4},
				{3, 4, 5},
				{1, 2, 3},
				{2, 3, 4},
				{3, 4, 5},
				// Rack 2 is overrepresented as a leader
				{2, 5, 6},
				{2, 3, 4},
				{2, 4, 5},
			},
			expected: [][]int{
				{1, 2, 3},
				// Swap here to get a rack1 leader
				{4, 3, 2},
				{3, 4, 5},
				{1, 2, 3},
				// Swap here to get a rack3 leader
				{3, 2, 4},
				{3, 4, 5},
				{1, 2, 3},
				{2, 3, 4},
				{3, 4, 5},
				{2, 5, 6},
				{2, 3, 4},
				{2, 4, 5},
			},
			checker: checker,
		},
		{
			description: "Swap not possible",
			curr: [][]int{
				{1, 2, 3},
				// Overrepresented leader racks with no swap possible (want rack3)
				{2, 5, 1},
				{2, 5, 1},
				{1, 2, 3},
				{2, 5, 1},
				{3, 4, 5},
			},
			expected: [][]int{
				{1, 2, 3},
				// Pick the first rack3 broker that hasn't already been a leader
				{6, 5, 1},
				{2, 5, 1},
				{1, 2, 3},
				{2, 5, 1},
				{3, 4, 5},
			},
			checker: checker,
		},
	}

	for _, testCase := range testCases {
		testCase.evaluate(t, assigner)
	}
}

func TestBalancedLeaderAssignerRandomized(t *testing.T) {
	brokers := testBrokers(12, 3)
	assigner := NewBalancedLeaderAssigner(brokers, pickers.NewRandomizedPicker())
	checker := func(result []admin.PartitionAssignment) bool {
		ok, _ := EvaluateAssignments(
			result,
			brokers,
			config.TopicPlacementConfig{
				Strategy: config.PlacementStrategyBalancedLeaders,
			},
		)
		return ok
	}

	testCases := []assignerTestCase{
		{
			description: "Single swap",
			curr: [][]int{
				{1, 2, 3},
				{2, 4, 5},
				{5, 6, 7},
			},
			expected: [][]int{
				{1, 2, 3},
				{2, 4, 5},
				// Single swap
				{6, 5, 7},
			},
			checker: checker,
		},
		{
			description: "Multiple swaps",
			curr: [][]int{
				{1, 2, 3},
				{2, 3, 4},
				{3, 4, 5},
				{1, 2, 3},
				{2, 3, 4},
				{3, 4, 5},
				{1, 2, 3},
				{2, 3, 4},
				{3, 4, 5},
				// Rack 2 is overrepresented as a leader
				{2, 5, 6},
				{2, 3, 4},
				{2, 4, 5},
			},
			expected: [][]int{
				{1, 2, 3},
				// Swap here to get a rack1 leader
				{4, 3, 2},
				{3, 4, 5},
				{1, 2, 3},
				// Swap here to get a rack3 leader
				{3, 2, 4},
				{3, 4, 5},
				{1, 2, 3},
				{2, 3, 4},
				{3, 4, 5},
				{2, 5, 6},
				{2, 3, 4},
				{2, 4, 5},
			},
			checker: checker,
		},
		{
			description: "Swap not possible",
			curr: [][]int{
				{1, 2, 3},
				// Overrepresented leader racks with no swap possible (want rack3)
				{2, 5, 1},
				{2, 5, 1},
				{1, 2, 3},
				{2, 5, 1},
				{3, 4, 5},
			},
			expected: [][]int{
				{1, 2, 3},
				// Pick random rack3 broker that hasn't already been a leader
				{9, 5, 1},
				{2, 5, 1},
				{1, 2, 3},
				{2, 5, 1},
				{3, 4, 5},
			},
			checker: checker,
		},
	}

	for _, testCase := range testCases {
		testCase.evaluate(t, assigner)
	}
}
