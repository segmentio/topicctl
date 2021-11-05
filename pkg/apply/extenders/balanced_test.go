package extenders

import (
	"testing"

	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/apply/assigners"
	"github.com/segmentio/topicctl/pkg/apply/pickers"
	"github.com/segmentio/topicctl/pkg/config"
)

func TestBalancedExtenderCrossRack(t *testing.T) {
	brokers := testBrokers(12, 3)
	extender := NewBalancedExtender(brokers, false, pickers.NewRandomizedPicker())
	checker := func(result []admin.PartitionAssignment) bool {
		ok, _ := assigners.EvaluateAssignments(
			result,
			brokers,
			config.TopicPlacementConfig{
				Strategy: config.PlacementStrategyBalancedLeaders,
			},
		)
		return ok
	}

	testCases := []extenderTestCase{
		{
			description: "Add partitions",
			topic:       "test-topic",
			curr: [][]int{
				{1, 2, 3},
				{2, 3, 4},
				{3, 4, 5},
			},
			extraPartitions: 6,
			expected: [][]int{
				{1, 2, 3},
				{2, 3, 4},
				{3, 4, 5},
				{7, 11, 9},
				{8, 12, 1},
				{6, 1, 8},
				{10, 5, 6},
				{5, 9, 10},
				{12, 10, 11},
			},
			checker: checker,
		},
	}

	for _, testCase := range testCases {
		testCase.evaluate(t, extender)
	}
}

func TestBalancedExtenderInRack(t *testing.T) {
	brokers := testBrokers(12, 3)
	extender := NewBalancedExtender(brokers, true, pickers.NewRandomizedPicker())
	checker := func(result []admin.PartitionAssignment) bool {
		ok, _ := assigners.EvaluateAssignments(
			result,
			brokers,
			config.TopicPlacementConfig{
				Strategy: config.PlacementStrategyInRack,
			},
		)
		return ok
	}

	testCases := []extenderTestCase{
		{
			description: "Add partitions",
			topic:       "test-topic",
			curr: [][]int{
				{1, 4, 7},
				{2, 5, 8},
				{3, 6, 9},
			},
			extraPartitions: 6,
			expected: [][]int{
				{1, 4, 7},
				{2, 5, 8},
				{3, 6, 9},
				{7, 10, 4},
				{8, 11, 2},
				{6, 3, 12},
				{10, 7, 1},
				{5, 8, 11},
				{12, 9, 6},
			},
			checker: checker,
		},
	}

	for _, testCase := range testCases {
		testCase.evaluate(t, extender)
	}
}

func TestBalancedExtenderInRackPartitionCountNotMultipleOfRacks(t *testing.T) {
	brokers := testBrokers(12, 3)
	extender := NewBalancedExtender(brokers, true, pickers.NewRandomizedPicker())
	checker := func(result []admin.PartitionAssignment) bool {
		ok, _ := assigners.EvaluateAssignments(
			result,
			brokers,
			config.TopicPlacementConfig{
				Strategy: config.PlacementStrategyInRack,
			},
		)
		return ok
	}

	testCases := []extenderTestCase{
		{
			description: "Add partitions",
			topic:       "test-topic",
			curr: [][]int{
				{1, 4, 7},
				{2, 5, 8},
				{3, 6, 9},
				{7, 10, 4},
			},
			extraPartitions: 5,
			expected: [][]int{
				{1, 4, 7},
				{2, 5, 8},
				{3, 6, 9},
				{7, 10, 4},
				{4, 1, 10},
				{5, 2, 11},
				{12, 9, 6},
				{10, 7, 1},
				{11, 8, 5},
			},
			checker: checker,
		},
	}

	for _, testCase := range testCases {
		testCase.evaluate(t, extender)
	}
}
