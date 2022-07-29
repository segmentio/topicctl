package rebalancers

import (
	"fmt"
	"testing"

	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/apply/pickers"
	"github.com/segmentio/topicctl/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFrequencyRebalancerAnyLowestIndex(t *testing.T) {
	brokers := testBrokers(12, 3)
	rebalancer := NewFrequencyRebalancer(
		brokers,
		pickers.NewLowestIndexPicker(),
		config.TopicPlacementConfig{
			Strategy: config.PlacementStrategyAny,
		},
	)

	testCases := []rebalancerTestCase{
		{
			description: "Simple",
			// Broker 1 is overrepresented in first index, 2 is overrepresented in second; each
			// should be replaced with the lowest-index choice that has no existing representation
			// in the topic. The 3 in the third partition is also replaced because, although it's
			// not over-represented in the index, it is for the topic as a whole.
			curr: [][]int{
				{1, 2, 3},
				{1, 4, 5},
				{3, 2, 7},
			},
			expected: [][]int{
				{6, 9, 3},
				{1, 4, 5},
				{8, 2, 7},
			},
		},
		{
			description: "Simple with removals",
			curr: [][]int{
				{1, 2, 3},
				{1, 4, 5},
				{3, 2, 7},
			},
			expected: [][]int{
				{6, 10, 3},
				{8, 4, 5},
				{9, 2, 11},
			},
			toRemove: []int{1, 7},
		},
		{
			description: "Simple with large removals",
			curr: [][]int{
				{1, 2, 3},
				{4, 5, 6},
				{7, 8, 9},
			},
			expected: [][]int{
				{12, 10, 11},
				{11, 10, 12},
				{10, 11, 12},
			},
			toRemove: []int{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			description: "Big",
			curr: [][]int{
				{1, 2, 3},
				{4, 5, 6},
				{7, 8, 9},
				{1, 2, 3},
				{4, 5, 6},
				{7, 8, 9},
				{1, 2, 3},
				{4, 5, 6},
				{7, 8, 9},
			},
			expected: [][]int{
				{12, 4, 8},
				{11, 1, 2},
				{10, 7, 1},
				{5, 11, 4},
				{3, 10, 7},
				{2, 12, 5},
				{1, 2, 3},
				{4, 5, 6},
				{7, 8, 9},
			},
		},
		{
			description: "Big with removals",
			curr: [][]int{
				{1, 2, 3},
				{4, 5, 6},
				{7, 8, 9},
				{1, 2, 3},
				{4, 5, 6},
				{7, 8, 9},
				{1, 2, 3},
				{4, 5, 6},
				{7, 8, 9},
			},
			expected: [][]int{
				{10, 6, 4},
				{3, 7, 2},
				{2, 4, 5},
				{11, 10, 8},
				{8, 12, 7},
				{5, 11, 10},
				{12, 2, 3},
				{4, 5, 6},
				{7, 8, 9},
			},
			toRemove: []int{1},
		},
	}

	for _, testCase := range testCases {
		testCase.evaluate(t, rebalancer)
	}
}

func TestFrequencyRebalancerAnyRandomized(t *testing.T) {
	brokers := testBrokers(12, 3)
	rebalancer := NewFrequencyRebalancer(
		brokers,
		pickers.NewRandomizedPicker(),
		config.TopicPlacementConfig{
			Strategy: config.PlacementStrategyAny,
		},
	)

	testCases := []rebalancerTestCase{
		{
			description: "Simple",
			// Broker 1 is overrepresented in first index, 2 is overrepresented in second; each
			// should be replaced with a rabdin choice that has no existing representation
			// in the topic. The 3 in the third partition is also replaced because, although it's
			// not over-represented in the index, it is for the topic as a whole.
			curr: [][]int{
				{1, 2, 3},
				{1, 4, 5},
				{3, 2, 7},
			},
			expected: [][]int{
				{1, 6, 3},
				{12, 4, 5},
				{8, 2, 7},
			},
		},
		{
			description: "Big",
			curr: [][]int{
				{1, 2, 3},
				{4, 5, 6},
				{7, 8, 9},
				{1, 2, 3},
				{4, 5, 6},
				{7, 8, 9},
				{1, 2, 3},
				{4, 5, 6},
				{7, 8, 9},
			},
			expected: [][]int{
				{1, 2, 3},
				{4, 5, 2},
				{11, 8, 5},
				{12, 10, 7},
				{9, 7, 6},
				{7, 11, 8},
				{8, 4, 12},
				{10, 1, 4},
				{6, 12, 9},
			},
		},
	}

	for _, testCase := range testCases {
		testCase.evaluate(t, rebalancer)
	}
}

func TestFrequencyRebalancerAnyInRack(t *testing.T) {
	brokers := testBrokers(12, 3)
	rebalancer := NewFrequencyRebalancer(
		brokers,
		pickers.NewLowestIndexPicker(),
		config.TopicPlacementConfig{
			Strategy: config.PlacementStrategyInRack,
		},
	)

	testCases := []rebalancerTestCase{
		{
			description: "Simple",
			curr: [][]int{
				{1, 4, 7},
				{2, 5, 8},
				{3, 6, 9},
				{4, 7, 1},
				{5, 8, 2},
				{6, 9, 3},
			},
			expected: [][]int{
				{1, 4, 7},
				{2, 5, 8},
				{3, 6, 9},
				{10, 7, 1},
				{11, 8, 2},
				{12, 9, 3},
			},
		},
	}

	for _, testCase := range testCases {
		testCase.evaluate(t, rebalancer)
	}
}

func TestFrequencyRebalancerSpreadsAcrossAllBrokers(t *testing.T) {
	// numPartitions is intentionally not divisible by numRacks
	var numBrokers, numRacks, numPartitions = 39, 3, 100
	brokers := testBrokers(numBrokers, numRacks)
	rebalancer := NewFrequencyRebalancer(
		brokers,
		pickers.NewRandomizedPicker(),
		config.TopicPlacementConfig{
			Strategy: config.PlacementStrategyAny,
		},
	)

	partitions := make([][]int, numPartitions)

	for i := 0; i < numPartitions; i++ {
		// every partition starts very unevenly spread across 3 brokers
		partitions[i] = []int{1, 2, 3}
	}

	desired, err := rebalancer.Rebalance(
		"test-topics",
		admin.ReplicasToAssignments(partitions),
		nil,
	)

	replicas, err := admin.AssignmentsToReplicas(desired)
	require.NoError(t, err)

	if err != nil {
		require.NotNil(t, err)
	}

	brokerCounter := map[int]int{}

	for _, replica := range replicas {
		for _, broker := range replica {
			if val, ok := brokerCounter[broker]; ok {
				brokerCounter[broker] = val + 1
			} else {
				brokerCounter[broker] = 1
			}
		}
	}

	for i := 1; i <= numBrokers; i++ {
		assert.Contains(t, brokerCounter, i, fmt.Sprintf("Broker %d contains no partitions", i))
	}
}

func TestBrokerCounts(t *testing.T) {
	brokers := testBrokers(6, 3)
	rebalancer := NewFrequencyRebalancer(
		brokers,
		pickers.NewLowestIndexPicker(),
		config.TopicPlacementConfig{
			Strategy: config.PlacementStrategyInRack,
		},
	)

	brokerCounts := rebalancer.brokerCounts(
		admin.ReplicasToAssignments(
			[][]int{
				{1, 2, 3},
				{3, 4, 5},
				{6, 1, 2},
				{6, 1, 3},
			},
		),
		0,
		"test-topic",
		map[int]struct{}{
			3: {},
		},
	)

	assert.Equal(
		t,
		[]brokerCount{
			{
				brokerID:   4,
				indexCount: 0,
				totalCount: 1,
				partitions: nil,
				score:      4,
			},
			{
				brokerID:   5,
				indexCount: 0,
				totalCount: 1,
				partitions: nil,
				score:      5,
			},
			{
				brokerID:   2,
				indexCount: 0,
				totalCount: 2,
				partitions: nil,
				score:      2,
			},
			{
				brokerID:   1,
				indexCount: 1,
				totalCount: 3,
				partitions: []int{0},
				score:      1,
			},
			{
				brokerID:   6,
				indexCount: 2,
				totalCount: 2,
				partitions: []int{2, 3},
				score:      6,
			},
			{
				brokerID:    3,
				indexCount:  1,
				totalCount:  3,
				partitions:  []int{1},
				score:       3,
				toBeRemoved: true,
			},
		},
		brokerCounts,
	)

}

func TestBrokerCountComparison(t *testing.T) {
	brokerCount1 := brokerCount{
		brokerID:   1,
		indexCount: 2,
		totalCount: 3,
	}
	brokerCount2 := brokerCount{
		brokerID:   2,
		indexCount: 2,
		totalCount: 3,
	}
	brokerCount3 := brokerCount{
		brokerID:   3,
		indexCount: 4,
		totalCount: 3,
	}
	brokerCount4 := brokerCount{
		brokerID:   4,
		indexCount: 4,
		totalCount: 5,
		score:      20,
	}
	brokerCount5 := brokerCount{
		brokerID:   5,
		indexCount: 4,
		totalCount: 5,
		score:      50,
	}
	brokerCount6 := brokerCount{
		brokerID:    6,
		indexCount:  1,
		totalCount:  2,
		score:       30,
		toBeRemoved: true,
	}

	assert.False(t, brokerCount1.isSmaller(brokerCount2))
	assert.True(t, brokerCount1.isSmaller(brokerCount3))
	assert.True(t, brokerCount3.isSmaller(brokerCount4))
	assert.True(t, brokerCount4.isSmaller(brokerCount5))
	assert.False(t, brokerCount6.isSmaller(brokerCount5))
}

func TestPartitionBrokerCounts(t *testing.T) {
	lower, upper := partitionCounts(
		[]brokerCount{
			{
				brokerID:   1,
				indexCount: 2,
			},
			{
				brokerID:   2,
				indexCount: 2,
			},
			{
				brokerID:   3,
				indexCount: 4,
			},
			{
				brokerID:   4,
				indexCount: 4,
			},
			{
				brokerID:   5,
				indexCount: 4,
			},
		},
	)

	assert.Equal(t, []int{1, 2}, brokerIDs(lower))
	assert.Equal(t, []int{5, 4, 3}, brokerIDs(upper))

	lower2, upper2 := partitionCounts(
		[]brokerCount{
			{
				brokerID:   1,
				indexCount: 0,
			},
			{
				brokerID:   2,
				indexCount: 0,
			},
			{
				brokerID:   3,
				indexCount: 0,
			},
			{
				brokerID:   4,
				indexCount: 0,
			},
			{
				brokerID:    5,
				indexCount:  2,
				toBeRemoved: true,
			},
			{
				brokerID:   6,
				indexCount: 2,
			},
		},
	)

	assert.Equal(t, []int{1, 2, 3, 4}, brokerIDs(lower2))
	assert.Equal(t, []int{6, 5}, brokerIDs(upper2))
}

func brokerIDs(counts []brokerCount) []int {
	ids := []int{}

	for _, count := range counts {
		ids = append(ids, count.brokerID)
	}

	return ids
}

func TestBrokerCountShouldReplace(t *testing.T) {
	brokerCount1 := brokerCount{
		brokerID:   1,
		indexCount: 2,
		totalCount: 3,
	}
	brokerCount2 := brokerCount{
		brokerID:   2,
		indexCount: 2,
		totalCount: 3,
	}
	brokerCount3 := brokerCount{
		brokerID:   3,
		indexCount: 4,
		totalCount: 3,
	}
	brokerCount4 := brokerCount{
		brokerID:   4,
		indexCount: 4,
		totalCount: 5,
	}
	brokerCount5 := brokerCount{
		brokerID:   5,
		indexCount: 5,
		totalCount: 7,
	}

	brokers := testBrokers(12, 3)
	rebalancer := NewFrequencyRebalancer(
		brokers,
		pickers.NewLowestIndexPicker(),
		config.TopicPlacementConfig{
			Strategy: config.PlacementStrategyAny,
		},
	)

	assert.False(t, rebalancer.shouldTryReplace(brokerCount1, brokerCount2))
	assert.True(t, rebalancer.shouldTryReplace(brokerCount1, brokerCount3))
	assert.True(t, rebalancer.shouldTryReplace(brokerCount1, brokerCount4))
	assert.False(t, rebalancer.shouldTryReplace(brokerCount3, brokerCount4))
	assert.True(t, rebalancer.shouldTryReplace(brokerCount4, brokerCount5))
}
