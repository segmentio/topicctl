package pickers

import (
	"fmt"
	"hash/fnv"

	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/util"
)

// RandomizedPicker is a picker that uses broker index to break ties.
type RandomizedPicker struct{}

var _ Picker = (*RandomizedPicker)(nil)

// NewRandomizedPicker returns a new RandomizedPicker instance.
func NewRandomizedPicker() *RandomizedPicker {
	return &RandomizedPicker{}
}

// PickNew updates the replica for the argument partition and index, using the choices in
// brokerChoices.
func (r *RandomizedPicker) PickNew(
	topic string,
	brokerChoices []int,
	curr []admin.PartitionAssignment,
	partition int,
	index int,
) error {
	keySorter := func(input map[int]int) []int {
		seed := fmt.Sprintf("%s-%d-%d", topic, partition, index)
		return util.ShuffledKeys(input, seed)
	}

	return pickNewByPositionFrequency(
		topic,
		brokerChoices,
		curr,
		partition,
		index,
		keySorter,
	)
}

// SortRemovals sorts the argument partitions in order of priority for removing the broker
// at the argument index.
func (r *RandomizedPicker) SortRemovals(
	topic string,
	partitionChoices []int,
	curr []admin.PartitionAssignment,
	index int,
) error {
	keySorter := func(input map[int]int) []int {
		seed := fmt.Sprintf("%s-%+v-%d", topic, partitionChoices, index)
		return util.ShuffledKeys(input, seed)
	}
	return sortRemovalsByPositionFrequency(
		topic,
		partitionChoices,
		curr,
		index,
		keySorter,
	)
}

// ScoreBroker returns an integer score for the given broker at the provided partition and index.
func (r *RandomizedPicker) ScoreBroker(
	topic string,
	brokerID int,
	partition int,
	index int,
) int {
	// Hash the string of the inputs
	seed := fmt.Sprintf("%s-%d-%d-%d", topic, brokerID, partition, index)
	hash := fnv.New32()
	hash.Write([]byte(seed))
	return int(hash.Sum32())
}
