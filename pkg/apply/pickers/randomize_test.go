package pickers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRandomizedPickerPickNew(t *testing.T) {
	picker := NewRandomizedPicker()

	testCases := []pickNewTestCase{
		{
			description:   "Simple replacement, part 1",
			topic:         "test-topic2",
			brokerChoices: []int{1, 2, 3},
			curr: [][]int{
				{1, 5, 4},
				{2, -1, 4},
				{2, 1, 5},
			},
			partition:      1,
			index:          1,
			expectedChoice: 3,
		},
		{
			description:   "Simple replacement, part 2",
			topic:         "test-topic2",
			brokerChoices: []int{2, 3, 4},
			curr: [][]int{
				{1, 5, 4},
				{6, 7, 4},
				{2, 1, 5},
			},
			partition:      1,
			index:          2,
			expectedChoice: 3,
		},
		{
			description:   "Not feasible, part 1",
			topic:         "test-topic2",
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
			topic:         "test-topic2",
			brokerChoices: []int{},
			curr: [][]int{
				{1, 5, 4},
				{2, 7, 4},
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

func TestRandomizedPickerSortRemovals(t *testing.T) {
	picker := NewRandomizedPicker()

	testCases := []sortRemovalsTestCase{
		{
			description:      "Simple sort",
			topic:            "test-topic4",
			partitionChoices: []int{0, 1, 2, 3, 4},
			curr: [][]int{
				{1, 5, 4},
				{3, 5, 4},
				{3, 1, 5},
				{2, 1, 4},
				{2, 4, 5},
				{3, 4, 7},
			},
			index:            0,
			expectedOrdering: []int{1, 2, 3, 4, 0},
		},
	}

	for _, testCase := range testCases {
		testCase.evaluate(t, picker)
	}
}

func TestRandomizedPickerScoreBroker(t *testing.T) {
	picker := NewRandomizedPicker()
	score := picker.ScoreBroker("test-topic", 2, 3, 4)
	assert.Equal(t, 1182295229, score)
}
