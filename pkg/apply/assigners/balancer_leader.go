package assigners

import (
	"errors"
	"fmt"

	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/apply/pickers"
)

// BalancedLeaderAssigner is an Assigner that tries to ensure that the leaders
// of each partition are balanced across all of the broker racks. The general
// goals are to do this in a way that:
//
//  1. Does in-partition leader swaps whenever possible as opposed to changing
//     the set of brokers in a partition
//  2. Prevents particular brokers from being overrepresented among the leaders
//
// The algorithm currently used is as follows:
//
//   while not balanced:
//     find racks with fewest and most leaders (i.e., the overrepresented and underrepresented)
//     improve balance by doing a single leader replacement:
//       use the picker to rank the partitions that have an overrepresented leader
//       for each leader:
//         for each partition with the leader:
//           swap the leader with one of its followers if possible, then stop
//       otherwise, use the picker to replace the leader in the top-ranked partition with
//         a new broker from the target rack
type BalancedLeaderAssigner struct {
	brokers        []admin.BrokerInfo
	racks          []string
	brokerRacks    map[int]string
	brokersPerRack map[string][]int
	picker         pickers.Picker
}

var _ Assigner = (*BalancedLeaderAssigner)(nil)

// NewBalancedLeaderAssigner creates and returns a BalancedLeaderAssigner instance.
func NewBalancedLeaderAssigner(
	brokers []admin.BrokerInfo,
	picker pickers.Picker,
) *BalancedLeaderAssigner {
	return &BalancedLeaderAssigner{
		brokers:        brokers,
		picker:         picker,
		racks:          admin.DistinctRacks(brokers),
		brokerRacks:    admin.BrokerRacks(brokers),
		brokersPerRack: admin.BrokersPerRack(brokers),
	}
}

// Assign returns a new partition assignment according to the assigner-specific logic.
func (b *BalancedLeaderAssigner) Assign(
	topic string,
	curr []admin.PartitionAssignment,
) ([]admin.PartitionAssignment, error) {
	if err := admin.CheckAssignments(curr); err != nil {
		return nil, err
	}
	if len(curr)%len(b.racks) != 0 {
		return nil,
			fmt.Errorf(
				"Cannot balance leaders because the partition count is not a multiple of the number of racks",
			)
	}

	// First, copy current into desired
	desired := admin.CopyAssignments(curr)

	count := 0

	for {
		count++
		minRack, maxRack := b.minMaxRacks(desired)
		if minRack == maxRack {
			break
		}

		err := b.replaceLeader(topic, count, desired, maxRack, minRack)
		if err != nil {
			return desired, err
		}

		if count > 1000 {
			return nil, errors.New("Too many loops")
		}
	}

	return desired, nil
}

// minMaxRacks returns the racks with the fewest and most leaders. This
// is used as an input into the next step, where leaders are swapped around.
func (b *BalancedLeaderAssigner) minMaxRacks(
	curr []admin.PartitionAssignment,
) (string, string) {
	leaderRackCounts := map[string]int{}

	for _, rack := range b.racks {
		leaderRackCounts[rack] = 0
	}

	for _, assignment := range curr {
		leader := assignment.Replicas[0]

		leaderRack := b.brokerRacks[leader]
		leaderRackCounts[leaderRack]++
	}

	var minRack, maxRack string
	var minCount, maxCount int

	// Don't iterate over map to ensure ordering is consistent
	for _, rack := range b.racks {
		count := leaderRackCounts[rack]

		if minRack == "" {
			minRack = rack
			maxRack = rack
			minCount = count
			maxCount = count
		} else {
			if count < minCount {
				minRack = rack
				minCount = count
			}
			if count > maxCount {
				maxRack = rack
				maxCount = count
			}
		}
	}

	// If everything is balanced, make sure min and max racks are the same
	if minCount == maxCount {
		maxRack = minRack
	}

	return minRack, maxRack
}

func (b *BalancedLeaderAssigner) replaceLeader(
	topic string,
	count int,
	curr []admin.PartitionAssignment,
	fromRack string,
	toRack string,
) error {
	// First get the partitions that have a leader in the from rack
	fromPartitions := []int{}

	for _, assignment := range curr {
		leader := assignment.Replicas[0]

		if b.brokerRacks[leader] == fromRack {
			fromPartitions = append(fromPartitions, assignment.ID)
		}
	}

	b.picker.SortRemovals(
		topic,
		fromPartitions,
		curr,
		0,
	)

	// Go through all of the leaders in descending order.
	for _, partition := range fromPartitions {
		assignment := curr[partition]
		leader := assignment.Replicas[0]

		for r, replica := range assignment.Replicas[1:] {
			if b.brokerRacks[replica] == toRack {
				// We can swap within the same partition. Do this
				// and exit.
				curr[partition].Replicas[0] = replica
				curr[partition].Replicas[r+1] = leader
				return nil
			}
		}
	}

	// We could not do an in-partition swap, so use the picker to replace the leader
	// of the highest ranked partition with a leader in the target rack.
	return b.picker.PickNew(
		topic,
		b.brokersPerRack[toRack],
		curr,
		fromPartitions[0],
		0,
	)
}
