package assigners

import (
	"testing"

	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/config"
	"github.com/stretchr/testify/assert"
)

func TestEvaluateAssignmentsNonStatic(t *testing.T) {
	brokers := testBrokers(12, 3)

	type evaluateTestCase struct {
		replicaSlices   [][]int
		expectedResults map[config.PlacementStrategy]bool
		expectedErr     map[config.PlacementStrategy]bool
	}

	testCases := []evaluateTestCase{
		{
			replicaSlices: [][]int{
				{1, 2, 3},
				{4, 5, 6},
				{6, 7, 8},
			},
			expectedResults: map[config.PlacementStrategy]bool{
				config.PlacementStrategyAny:             true,
				config.PlacementStrategyStatic:          false,
				config.PlacementStrategyStaticInRack:    false,
				config.PlacementStrategyBalancedLeaders: false,
				config.PlacementStrategyInRack:          false,
			},
		},
		{
			// Matches static assignments set in test run loop below
			replicaSlices: [][]int{
				{1, 2, 3},
				{4, 5, 6},
				{6, 7, 1},
			},
			expectedResults: map[config.PlacementStrategy]bool{
				config.PlacementStrategyAny:             true,
				config.PlacementStrategyStatic:          true,
				config.PlacementStrategyStaticInRack:    false,
				config.PlacementStrategyBalancedLeaders: false,
				config.PlacementStrategyInRack:          false,
			},
		},
		{
			// Matches static assignments set in test run loop below
			replicaSlices: [][]int{
				{1, 2, 3},
				{4, 5, 6},
				{6, 7, 1},
			},
			expectedResults: map[config.PlacementStrategy]bool{
				config.PlacementStrategyAny:             true,
				config.PlacementStrategyStatic:          true,
				config.PlacementStrategyStaticInRack:    false,
				config.PlacementStrategyBalancedLeaders: false,
				config.PlacementStrategyInRack:          false,
			},
		},
		{
			// Matches static racks set in test run loop below
			replicaSlices: [][]int{
				{1, 4, 7},
				{2, 5, 8},
				{3, 6, 9},
				{4, 7, 10},
			},
			expectedResults: map[config.PlacementStrategy]bool{
				config.PlacementStrategyAny:             true,
				config.PlacementStrategyStatic:          false,
				config.PlacementStrategyStaticInRack:    true,
				config.PlacementStrategyBalancedLeaders: false,
				config.PlacementStrategyInRack:          true,
			},
		},
		{
			replicaSlices: [][]int{
				{1, 4, 6},
				{1, 4, 6},
				{1, 4, 6},
			},
			expectedResults: map[config.PlacementStrategy]bool{
				config.PlacementStrategyAny:             true,
				config.PlacementStrategyStatic:          false,
				config.PlacementStrategyStaticInRack:    false,
				config.PlacementStrategyBalancedLeaders: false,
				config.PlacementStrategyInRack:          false,
			},
		},
		{
			replicaSlices: [][]int{
				{1, 4, 7},
				{2, 5, 8},
				{3, 6, 9},
				{1, 4, 7},
				{2, 5, 8},
				{3, 6, 9},
			},
			expectedResults: map[config.PlacementStrategy]bool{
				config.PlacementStrategyAny:             true,
				config.PlacementStrategyStatic:          false,
				config.PlacementStrategyStaticInRack:    false,
				config.PlacementStrategyBalancedLeaders: true,
				config.PlacementStrategyInRack:          true,
			},
		},
		{
			replicaSlices: [][]int{
				{1, 4, 7},
				{2, 5, 8},
			},
			expectedResults: map[config.PlacementStrategy]bool{
				config.PlacementStrategyAny:             true,
				config.PlacementStrategyStatic:          false,
				config.PlacementStrategyStaticInRack:    false,
				config.PlacementStrategyBalancedLeaders: false,
				config.PlacementStrategyInRack:          true,
			},
		},
		{
			replicaSlices: [][]int{
				{1, 1, 1},
			},
			expectedResults: map[config.PlacementStrategy]bool{
				config.PlacementStrategyAny: false,
			},
			expectedErr: map[config.PlacementStrategy]bool{
				config.PlacementStrategyAny: true,
			},
		},
	}

	for _, testCase := range testCases {
		for strategy, expectedResult := range testCase.expectedResults {
			result, err := EvaluateAssignments(
				admin.ReplicasToAssignments(testCase.replicaSlices),
				brokers,
				config.TopicPlacementConfig{
					Strategy: strategy,
					StaticAssignments: [][]int{
						{1, 2, 3},
						{4, 5, 6},
						{6, 7, 1},
					},
					StaticRackAssignments: []string{
						"zone1",
						"zone2",
						"zone3",
						"zone1",
					},
				},
			)
			if testCase.expectedErr[strategy] {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(
					t,
					expectedResult,
					result,
					"Input: %+v, strategy: %s",
					testCase.replicaSlices,
					strategy,
				)
			}
		}
	}
}
