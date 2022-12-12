package pickers

import (
	"github.com/efcloud/topicctl/pkg/admin"
	"github.com/efcloud/topicctl/pkg/util"
)

// LowestIndexPicker is a picker that uses broker index to break ties.
type LowestIndexPicker struct{}

var _ Picker = (*LowestIndexPicker)(nil)

// NewLowestIndexPicker returns a new LowestIndexPicker instance.
func NewLowestIndexPicker() *LowestIndexPicker {
	return &LowestIndexPicker{}
}

// PickNew updates the replica for the argument partition and index, using the choices in
// brokerChoices.
func (l *LowestIndexPicker) PickNew(
	topic string,
	brokerChoices []int,
	curr []admin.PartitionAssignment,
	partition int,
	index int,
) error {
	return pickNewByPositionFrequency(
		topic,
		brokerChoices,
		curr,
		partition,
		index,
		util.SortedKeys,
	)
}

// SortRemovals sorts the argument partitions in order of priority for removing the broker
// at the argument index.
func (l *LowestIndexPicker) SortRemovals(
	topic string,
	partitionChoices []int,
	curr []admin.PartitionAssignment,
	index int,
) error {
	return sortRemovalsByPositionFrequency(
		topic,
		partitionChoices,
		curr,
		index,
		util.SortedKeys,
	)
}

// ScoreBroker returns an integer score for the given broker at the provided partition and index.
func (l *LowestIndexPicker) ScoreBroker(
	topic string,
	brokerID int,
	partition int,
	index int,
) int {
	return brokerID
}
