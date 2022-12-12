package assigners

import (
	"testing"

	"github.com/efcloud/topicctl/pkg/admin"
	"github.com/efcloud/topicctl/pkg/apply/pickers"
	"github.com/efcloud/topicctl/pkg/config"
)

func TestSingleRackAssignerThreeReplicas(t *testing.T) {
	brokers := testBrokers(12, 3)
	assigner := NewSingleRackAssigner(brokers, pickers.NewLowestIndexPicker())
	checker := func(result []admin.PartitionAssignment) bool {
		ok, _ := EvaluateAssignments(
			result,
			brokers,
			config.TopicPlacementConfig{
				Strategy: config.PlacementStrategyInRack,
			},
		)
		return ok
	}

	testCases := []assignerTestCase{
		{
			description: "Already single rack per partition",
			curr: [][]int{
				{1, 4, 7},
				{2, 5, 8},
				{3, 6, 9},
			},
			expected: [][]int{
				{1, 4, 7},
				{2, 5, 8},
				{3, 6, 9},
			},
			checker: checker,
		},
		{
			description: "Single change",
			curr: [][]int{
				{1, 4, 7},
				{2, 5, 8},
				{3, 5, 9},
			},
			expected: [][]int{
				{1, 4, 7},
				{2, 5, 8},
				{3, 6, 9},
			},
			checker: checker,
		},
		{
			description: "Multiple changes",
			curr: [][]int{
				{1, 4, 7},
				{2, 5, 8},
				// Multi-rack
				{3, 5, 9},
				// Multi-rack
				{1, 3, 8},
				{2, 5, 8},
				// Multi-rack
				{3, 5, 9},
				{1, 4, 7},
				{2, 5, 8},
				// Multi-rack
				{3, 5, 9},
			},
			expected: [][]int{
				{1, 4, 7},
				{2, 5, 8},
				{3, 6, 9},
				// Skip over broker 4 for second position since it's already used
				{1, 7, 4},
				{2, 5, 8},
				// Skip over broker 6 since it's already been used in this position
				{3, 12, 9},
				{1, 4, 7},
				{2, 5, 8},
				// Then, come back to broker 6
				{3, 6, 9},
			},
			checker: checker,
		},
	}

	for _, testCase := range testCases {
		testCase.evaluate(t, assigner)
	}
}

func TestSingleRackAssignerThreeReplicasRandomized(t *testing.T) {
	brokers := testBrokers(12, 3)
	assigner := NewSingleRackAssigner(brokers, pickers.NewRandomizedPicker())
	checker := func(result []admin.PartitionAssignment) bool {
		ok, _ := EvaluateAssignments(
			result,
			brokers,
			config.TopicPlacementConfig{
				Strategy: config.PlacementStrategyInRack,
			},
		)
		return ok
	}

	testCases := []assignerTestCase{
		{
			description: "Single change",
			topic:       "test-topic1",
			curr: [][]int{
				{1, 4, 7},
				{2, 5, 8},
				{3, 5, 9},
			},
			expected: [][]int{
				{1, 4, 7},
				{2, 5, 8},
				{3, 12, 9},
			},
			checker: checker,
		},
		{
			description: "Multiple changes",
			topic:       "test-topic3",
			curr: [][]int{
				{1, 4, 7},
				{2, 5, 8},
				// Multi-rack
				{3, 5, 9},
				// Multi-rack
				{1, 3, 8},
				{2, 5, 8},
				// Multi-rack
				{3, 5, 9},
				{1, 4, 7},
				{2, 5, 8},
				// Multi-rack
				{3, 5, 9},
			},
			expected: [][]int{
				{1, 4, 7},
				{2, 5, 8},
				{3, 12, 9},
				// Skip over broker 4 for second position since it's already used
				{1, 10, 4},
				{2, 5, 8},
				// Skip over broker 6 since it's already been used in this position
				{3, 6, 9},
				{1, 4, 7},
				{2, 5, 8},
				// Then, come back to either broker 6 or 12
				{3, 6, 9},
			},
			checker: checker,
		},
	}

	for _, testCase := range testCases {
		testCase.evaluate(t, assigner)
	}
}

func TestSingleRackAssignerTwoReplicas(t *testing.T) {
	brokers := testBrokers(6, 3)
	assigner := NewSingleRackAssigner(brokers, pickers.NewLowestIndexPicker())
	checker := func(result []admin.PartitionAssignment) bool {
		ok, _ := EvaluateAssignments(
			result,
			brokers,
			config.TopicPlacementConfig{
				Strategy: config.PlacementStrategyInRack,
			},
		)
		return ok
	}

	testCases := []assignerTestCase{
		{
			description: "Already single rack per partition",
			curr: [][]int{
				{1, 4},
				{2, 5},
				{3, 6},
			},
			expected: [][]int{
				{1, 4},
				{2, 5},
				{3, 6},
			},
			checker: checker,
		},
		{
			description: "Error",
			curr: [][]int{
				{1, 2},
				{3, 2},
				{5, 3},
				{6, 5},
				{1, 6},
				{2, 6},
				{1, 3},
				{5, 3},
				{6, 5},
			},
			expected: [][]int{
				{1, 4},
				{3, 6},
				{5, 2},
				{6, 3},
				{1, 4},
				{2, 5},
				{1, 4},
				{5, 2},
				{6, 3},
			},
			checker: checker,
		},
	}

	for _, testCase := range testCases {
		testCase.evaluate(t, assigner)
	}
}
