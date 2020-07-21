package pickers

import (
	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/util"
)

// LowestIndexPicker is a picker that uses broker index to break ties.
type LowestIndexPicker struct{}

var _ Picker = (*LowestIndexPicker)(nil)

func NewLowestIndexPicker() *LowestIndexPicker {
	return &LowestIndexPicker{}
}

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

func (l *LowestIndexPicker) ScoreBroker(
	topic string,
	brokerID int,
	partition int,
	index int,
) int {
	return brokerID
}
