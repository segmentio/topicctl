package pickers

import (
	"testing"

	"github.com/efcloud/topicctl/pkg/admin"
	"github.com/stretchr/testify/assert"
)

func TestClusterUsePickerPickNew(t *testing.T) {
	brokers := testBrokers(12, 3)
	topics := []admin.TopicInfo{
		{
			Name: "test-topic1",
			Partitions: []admin.PartitionInfo{
				{
					ID:       0,
					Replicas: []int{1, 2, 3},
				},
				{
					ID:       1,
					Replicas: []int{1, 2, 3},
				},
			},
		},
		{
			Name: "test-topic2",
			Partitions: []admin.PartitionInfo{
				{
					ID:       0,
					Replicas: []int{4, 3, 1},
				},
				{
					ID:       1,
					Replicas: []int{5, 2, 3},
				},
			},
		},
	}

	picker := NewClusterUsePicker(brokers, topics)

	testCases := []pickNewTestCase{
		{
			description:   "Simple replacement, part 1",
			topic:         "test-topic",
			brokerChoices: []int{1, 2, 3},
			curr: [][]int{
				{1, 5, 4},
				{2, -1, 4},
				{2, 1, 5},
			},
			partition: 1,
			index:     1,
			// Of the feasible choices, 3 is the least used in position 1
			expectedChoice: 3,
		},
		{
			description:   "Simple replacement, part 2",
			topic:         "test-topic",
			brokerChoices: []int{1, 2, 3, 8},
			curr: [][]int{
				{1, 5, 4},
				{2, -1, 4},
				{2, 1, 5},
			},
			partition: 1,
			index:     1,
			// Of the feasible choices, 8 is the least used in position 1
			expectedChoice: 8,
		},
		{
			description:   "Simple replacement, part 3",
			topic:         "test-topic",
			brokerChoices: []int{1, 2, 3, 4},
			curr: [][]int{
				{1, 5, 4},
				{6, 7, -1},
				{2, 1, 5},
			},
			partition: 1,
			index:     2,
			// Of the feasible choices, 2 is the least used in position 1
			expectedChoice: 2,
		},
		{
			description:   "Not feasible, part 1",
			topic:         "test-topic",
			brokerChoices: []int{2, 7},
			curr: [][]int{
				{1, 5, 4},
				{2, 7, 4},
				{2, 1, 5},
			},
			partition:   1,
			index:       2,
			expectedErr: true,
		},
		{
			description:   "Not feasible, part 2",
			topic:         "test-topic",
			brokerChoices: []int{},
			curr: [][]int{
				{1, 5, 4},
				{2, 3, 4},
				{2, 1, 5},
			},
			partition:   1,
			index:       2,
			expectedErr: true,
		},
	}

	for _, testCase := range testCases {
		testCase.evaluate(t, picker)
	}
}

func TestClusterUsePickerSortRemovals(t *testing.T) {
	brokers := testBrokers(12, 3)
	topics := []admin.TopicInfo{
		{
			Name: "test-topic1",
			Partitions: []admin.PartitionInfo{
				{
					ID:       0,
					Replicas: []int{1, 2, 3},
				},
				{
					ID:       1,
					Replicas: []int{2, 7, 3},
				},
			},
		},
		{
			Name: "test-topic2",
			Partitions: []admin.PartitionInfo{
				{
					ID:       0,
					Replicas: []int{3, 8, 1},
				},
				{
					ID:       1,
					Replicas: []int{3, 5, 8},
				},
			},
		},
	}

	picker := NewClusterUsePicker(brokers, topics)

	testCases := []sortRemovalsTestCase{
		{
			description:      "No tie-breaking required",
			topic:            "test-topic",
			partitionChoices: []int{0, 1, 2, 3, 4, 5},
			curr: [][]int{
				{1, 5, 4},
				{3, 5, 4},
				{3, 1, 5},
				{2, 1, 4},
				{2, 4, 5},
				{3, 4, 7},
			},
			index:            0,
			expectedOrdering: []int{1, 2, 5, 3, 4, 0},
		},
		{
			description:      "Simple sort",
			topic:            "test-topic",
			partitionChoices: []int{0, 2, 3, 4, 5},
			curr: [][]int{
				{1, 5, 4},
				{3, 5, 4},
				{3, 1, 5},
				{2, 1, 4},
				{2, 4, 5},
				{3, 4, 7},
			},
			index: 0,
			// Break tie in favor of 3 since it's more overrepresented in the cluster as a whole
			expectedOrdering: []int{2, 5, 3, 4, 0},
		},
	}

	for _, testCase := range testCases {
		testCase.evaluate(t, picker)
	}
}

func TestClusterUsePickerScoreBroker(t *testing.T) {
	brokers := testBrokers(12, 3)
	topics := []admin.TopicInfo{
		{
			Name: "test-topic1",
			Partitions: []admin.PartitionInfo{
				{
					ID:       0,
					Replicas: []int{1, 2, 3},
				},
				{
					ID:       1,
					Replicas: []int{2, 7, 3},
				},
			},
		},
		{
			Name: "test-topic2",
			Partitions: []admin.PartitionInfo{
				{
					ID:       0,
					Replicas: []int{3, 8, 1},
				},
				{
					ID:       1,
					Replicas: []int{3, 5, 8},
				},
			},
		},
	}

	picker := NewClusterUsePicker(brokers, topics)
	score := picker.ScoreBroker("test-topic3", 3, 1, 0)
	assert.Equal(t, 2, score)
}
