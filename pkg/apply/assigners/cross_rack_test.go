package assigners

import (
	"errors"
	"testing"

	"github.com/efcloud/topicctl/pkg/admin"
	"github.com/efcloud/topicctl/pkg/apply/pickers"
	"github.com/efcloud/topicctl/pkg/config"
)

func TestCrossRackAssignerThreeReplicas(t *testing.T) {
	brokers := testBrokers(12, 3)
	assigner := NewCrossRackAssigner(brokers, pickers.NewLowestIndexPicker())
	checker := func(result []admin.PartitionAssignment) bool {
		ok, _ := EvaluateAssignments(
			result,
			brokers,
			config.TopicPlacementConfig{
				Strategy: config.PlacementStrategyCrossRack,
			},
		)
		return ok
	}

	testCases := []assignerTestCase{
		{
			description: "Already cross rack",
			curr: [][]int{
				{1, 2, 3},
				{4, 5, 6},
				{7, 8, 9},
			},
			expected: [][]int{
				{1, 2, 3},
				{4, 5, 6},
				{7, 8, 9},
			},
			checker: checker,
		},
		{
			description: "Single change",
			curr: [][]int{
				{1, 2, 3},
				{4, 5, 6},
				{7, 4, 9},
			},
			expected: [][]int{
				{1, 2, 3},
				{4, 5, 6},
				{7, 8, 9},
			},
			checker: checker,
		},
		{
			description: "Multiple changes",
			curr: [][]int{
				{1, 4, 7},
				{2, 5, 8},
				{9, 3, 11},
				{8, 4, 11},
				{10, 4, 11},
				{5, 4, 11},
				{7, 4, 11},
				{3, 4, 11},
				{12, 4, 11},
			},
			expected: [][]int{
				{1, 2, 3},
				{2, 1, 6},
				// 2nd position should be replaced with different rack than other 2 replicas
				// 3rd position is a valid rack already, should be left alone
				{9, 7, 11},
				// 2nd & 3rd both were invalid racks
				{8, 4, 9},
				// 3rd was a valid rack
				{10, 3, 11},
				{5, 4, 12},
				{7, 6, 11},
				{3, 4, 11},
				{12, 4, 11},
			},
			checker: checker,
		},
		{
			description: "Changes with all replicas",
			curr: [][]int{
				{1, 4, 7},
				{2, 5, 8},
				{3, 6, 9},
				{4, 7, 10},
				{5, 8, 11},
				{6, 9, 12},
				{7, 10, 1},
				{8, 11, 2},
				{9, 12, 3},
				{10, 1, 4},
				{11, 2, 5},
				{12, 3, 6},
			},
			expected: [][]int{
				{1, 2, 3},
				{2, 1, 6},
				{3, 4, 2},
				{4, 5, 9},
				{5, 7, 12},
				{6, 10, 5},
				{7, 8, 3},
				{8, 1, 6},
				{9, 4, 8},
				{10, 11, 9},
				{11, 7, 12},
				{12, 10, 11},
			},
			checker: checker,
		},
	}

	for _, testCase := range testCases {
		testCase.evaluate(t, assigner)
	}
}

func TestCrossRackAssignerThreeReplicasRandomized(t *testing.T) {
	brokers := testBrokers(12, 3)
	assigner := NewCrossRackAssigner(brokers, pickers.NewRandomizedPicker())
	checker := func(result []admin.PartitionAssignment) bool {
		ok, _ := EvaluateAssignments(
			result,
			brokers,
			config.TopicPlacementConfig{
				Strategy: config.PlacementStrategyCrossRack,
			},
		)
		return ok
	}

	testCases := []assignerTestCase{
		{
			description: "Already cross rack",
			curr: [][]int{
				{1, 2, 3},
				{4, 5, 6},
				{7, 8, 9},
			},
			expected: [][]int{
				{1, 2, 3},
				{4, 5, 6},
				{7, 8, 9},
			},
			checker: checker,
		},
		{
			description: "Single change",
			curr: [][]int{
				{1, 2, 3},
				{4, 10, 6},
				{7, 10, 9},
			},
			expected: [][]int{
				{1, 2, 3},
				{4, 11, 6},
				{7, 5, 9},
			},
			checker: checker,
		},
		{
			description: "Changes with all replicas",
			curr: [][]int{
				{1, 4, 7},
				{2, 5, 8},
				{3, 6, 9},
				{4, 7, 10},
				{5, 8, 11},
				{6, 9, 12},
				{7, 10, 1},
				{8, 11, 2},
				{9, 12, 3},
				{10, 1, 4},
				{11, 2, 5},
				{12, 3, 6},
			},
			expected: [][]int{
				{1, 2, 6},
				{2, 10, 9},
				{3, 1, 8},
				{4, 5, 3},
				{5, 4, 12},
				{6, 7, 11},
				{7, 8, 9},
				{8, 10, 12},
				{9, 1, 5},
				{10, 11, 3},
				{11, 4, 6},
				{12, 7, 2},
			},
			checker: checker,
		},
	}

	for _, testCase := range testCases {
		testCase.evaluate(t, assigner)
	}
}

func TestCrossRackAssignerTwoReplicas(t *testing.T) {
	brokers := testBrokers(6, 3)
	assigner := NewCrossRackAssigner(brokers, pickers.NewLowestIndexPicker())
	checker := func(result []admin.PartitionAssignment) bool {
		ok, _ := EvaluateAssignments(
			result,
			brokers,
			config.TopicPlacementConfig{
				Strategy: config.PlacementStrategyCrossRack,
			},
		)
		return ok
	}

	testCases := []assignerTestCase{
		{
			description: "Already cross rack",
			curr: [][]int{
				{1, 2},
				{3, 4},
				{5, 6},
			},
			expected: [][]int{
				{1, 2},
				{3, 4},
				{5, 6},
			},
			checker: checker,
		},
		{
			description: "Error due to more replicas than racks",
			curr: [][]int{
				{1, 2, 3, 4},
				{5, 6, 1, 2},
			},
			err: errors.New("more replicas than racks"),
		},
	}

	for _, testCase := range testCases {
		testCase.evaluate(t, assigner)
	}
}
