package pickers

import (
	"errors"
	"sort"

	"github.com/efcloud/topicctl/pkg/admin"
	"github.com/efcloud/topicctl/pkg/util"
)

var (
	// ErrNoFeasibleChoice is returned by a picker when there is no feasible choice among
	// the offered possibilities.
	ErrNoFeasibleChoice = errors.New("Picker could not find a feasible choice")
)

// Picker is an interface that picks a replica assignment based on arbitrary criteria (e.g.,
// the current number of brokers in the given index). It's used by assigners and extenders to
// make choices, subject to specific constraints (e.g., must be in certain rack).
type Picker interface {
	// PickNew is the primary method used for assignment and extension. It chooses a new
	// replica for the given partition and index and directly modifies the argument assignments.
	PickNew(
		topic string,
		brokerChoices []int,
		curr []admin.PartitionAssignment,
		partition int,
		index int,
	) error

	// SortRemovals is a helper for choosing which replica in a set of partitions to replace.
	// Because the actual replacement logic is somewhat complex at the moment, the interface
	// is a little different than the PickNew function above. In particular, the choices are
	// a slice of partitions and these are sorted in place (without any replacement in curr).
	SortRemovals(
		topic string,
		partitionChoices []int,
		curr []admin.PartitionAssignment,
		index int,
	) error

	// ScoreBroker is a helper for generating a static "score" for a broker. It's used in
	// rebalancing and other applications where we're doing swaps as opposed to a single
	// addition or subtraction. A higher score should correspond to higher frequency, i.e.
	// more likely to be removed.
	ScoreBroker(
		topic string,
		brokerID int,
		partition int,
		index int,
	) int
}

func pickNewByPositionFrequency(
	topic string,
	brokerChoices []int,
	curr []admin.PartitionAssignment,
	partition int,
	index int,
	keySorter util.KeySorter,
) error {
	if len(brokerChoices) == 0 {
		return ErrNoFeasibleChoice
	}

	brokerChoicesMap := map[int]struct{}{}
	for _, choice := range brokerChoices {
		brokerChoicesMap[choice] = struct{}{}
	}

	brokerCounts := map[int]int{}

	for _, choice := range brokerChoices {
		brokerCounts[choice] = 0
	}

	// Get counts for each feasible broker
	for p := 0; p < len(curr); p++ {
		replica := curr[p].Replicas[index]
		if _, ok := brokerChoicesMap[replica]; ok {
			brokerCounts[replica]++
		}
	}

	// Sort by count ascending, using index to break ties
	sortedBrokers := util.SortedKeysByValue(brokerCounts, true, keySorter)

	// Replace with the first feasible broker
	for _, broker := range sortedBrokers {
		if curr[partition].Index(broker) == -1 {
			curr[partition].Replicas[index] = broker
			return nil
		}
	}

	return ErrNoFeasibleChoice
}

func sortRemovalsByPositionFrequency(
	topic string,
	partitionChoices []int,
	curr []admin.PartitionAssignment,
	index int,
	keySorter util.KeySorter,
) error {
	if len(partitionChoices) == 0 {
		return ErrNoFeasibleChoice
	}

	brokerCounts := map[int]int{}

	for _, partition := range partitionChoices {
		replica := curr[partition].Replicas[index]
		if replica >= 0 {
			brokerCounts[replica]++
		}
	}

	// Sort by count descending, using index to break ties
	sortedBrokers := util.SortedKeysByValue(brokerCounts, false, keySorter)
	brokerRanks := map[int]int{}
	for s, broker := range sortedBrokers {
		brokerRanks[broker] = s
	}

	// Sort partition choices in-place
	sort.Slice(partitionChoices, func(a, b int) bool {
		aPartition := partitionChoices[a]
		bPartition := partitionChoices[b]

		aReplica := curr[aPartition].Replicas[index]
		bReplica := curr[bPartition].Replicas[index]

		return brokerRanks[aReplica] < brokerRanks[bReplica]
	})

	return nil
}
