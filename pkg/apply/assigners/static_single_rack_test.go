package assigners

import (
	"testing"

	"github.com/efcloud/topicctl/pkg/admin"
	"github.com/efcloud/topicctl/pkg/apply/pickers"
	"github.com/efcloud/topicctl/pkg/config"
)

func TestStaticSingleRackAssignerThreePartitions(t *testing.T) {
	brokers := testBrokers(12, 3)

	staticAssignments := []string{
		"zone1",
		"zone2",
		"zone3",
	}

	assigner := NewStaticSingleRackAssigner(
		brokers,
		staticAssignments,
		pickers.NewLowestIndexPicker(),
	)
	checker := func(result []admin.PartitionAssignment) bool {
		ok, _ := EvaluateAssignments(
			result,
			brokers,
			config.TopicPlacementConfig{
				Strategy:              config.PlacementStrategyStaticInRack,
				StaticRackAssignments: staticAssignments,
			},
		)
		return ok
	}

	testCases := []assignerTestCase{
		{
			description: "Already correct racks",
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
			description: "Single partition change",
			curr: [][]int{
				{1, 4, 7},
				{2, 5, 8},
				{4, 5, 9},
			},
			expected: [][]int{
				{1, 4, 7},
				{2, 5, 8},
				{3, 6, 9},
			},
			checker: checker,
		},
	}

	for _, testCase := range testCases {
		testCase.evaluate(t, assigner)
	}
}

func TestStaticSingleRackAssignerNinePartitions(t *testing.T) {
	brokers := testBrokers(12, 3)

	staticAssignments := []string{
		"zone1",
		"zone2",
		"zone3",
		"zone1",
		"zone2",
		"zone3",
		"zone1",
		"zone2",
		"zone3",
	}

	assigner := NewStaticSingleRackAssigner(
		brokers,
		staticAssignments,
		pickers.NewLowestIndexPicker(),
	)
	checker := func(result []admin.PartitionAssignment) bool {
		ok, _ := EvaluateAssignments(
			result,
			brokers,
			config.TopicPlacementConfig{
				Strategy:              config.PlacementStrategyStaticInRack,
				StaticRackAssignments: staticAssignments,
			},
		)
		return ok
	}

	testCases := []assignerTestCase{
		{
			description: "Multiple changes",
			curr: [][]int{
				{1, 4, 7},
				// Multi-rack
				{3, 5, 8},
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
				// Skip over broker 2 for this position because it's already a leader for
				// other partitions, and broker 5 and 8 because they're already in this partition
				{11, 5, 8},
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

func TestStaticSingleRackAssignerNinePartitionsRandomized(t *testing.T) {
	brokers := testBrokers(12, 3)

	staticAssignments := []string{
		"zone1",
		"zone2",
		"zone3",
		"zone1",
		"zone2",
		"zone3",
		"zone1",
		"zone2",
		"zone3",
	}

	assigner := NewStaticSingleRackAssigner(
		brokers,
		staticAssignments,
		pickers.NewRandomizedPicker(),
	)
	checker := func(result []admin.PartitionAssignment) bool {
		ok, _ := EvaluateAssignments(
			result,
			brokers,
			config.TopicPlacementConfig{
				Strategy:              config.PlacementStrategyStaticInRack,
				StaticRackAssignments: staticAssignments,
			},
		)
		return ok
	}

	testCases := []assignerTestCase{
		{
			description: "Multiple changes",
			curr: [][]int{
				{1, 4, 7},
				// Multi-rack
				{3, 5, 8},
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
				// Skip over broker 2 for this position because it's already a leader for
				// other partitions, and broker 5 and 8 because they're already in this partition
				{11, 5, 8},
				{3, 6, 9},
				// Skip over broker 4 for second position since it's already used
				{1, 10, 4},
				{2, 5, 8},
				// Skip over broker 6 since it's already been used in this position
				{3, 12, 9},
				{1, 4, 7},
				{2, 5, 8},
				{3, 12, 9},
			},
			checker: checker,
		},
	}

	for _, testCase := range testCases {
		testCase.evaluate(t, assigner)
	}
}
