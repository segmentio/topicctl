package rebalancers

import (
	"fmt"
	"sort"

	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/apply/assigners"
	"github.com/segmentio/topicctl/pkg/apply/pickers"
	"github.com/segmentio/topicctl/pkg/config"
)

// FrequencyRebalancer is a Rebalancer that rebalances to achieve in-topic balance among
// all available brokers. The algorithm used is:
//
//   for each replica position index:
//     while true:
//       get counts for each broker in that position
//       partition the set of brokers into two sets, a "lower" one and an "upper one", based on
//         on the sorted frequency counts (with brokers to be removed treated as the highest
//         frequencies)
//       for each lower broker, upper broker combination:
//         try to replace the upper broker with the lower one
//         if replacement made, continue to next while loop iteration
//       if no replacement made, break out of while loop, continue to next partition index
//
// Replacements are made if:
//
//   1. The replacement improves the broker balance for the index OR
//         the replacement improves the broker balance for the topic as a whole
//     AND
//   2. The replacement is consistent with the placement strategy for the topic (e.g., balanced
//      leaders, in-rack, etc.)
//
// The picker passed in to the rebalancer is used to sort the partitions for each broker (if it
// appears more than once for the current index) and also to break ties when sorting and
// partitioning the brokers.
type FrequencyRebalancer struct {
	brokers         []admin.BrokerInfo
	picker          pickers.Picker
	placementConfig config.TopicPlacementConfig
}

var _ Rebalancer = (*FrequencyRebalancer)(nil)

// NewFrequencyRebalancer creates a new FrequencyRebalancer instance.
func NewFrequencyRebalancer(
	brokers []admin.BrokerInfo,
	picker pickers.Picker,
	placementConfig config.TopicPlacementConfig,
) *FrequencyRebalancer {
	return &FrequencyRebalancer{
		brokers:         brokers,
		picker:          picker,
		placementConfig: placementConfig,
	}
}

// Rebalance rebalances the argument partition assignments according to the algorithm
// described earlier.
func (f *FrequencyRebalancer) Rebalance(
	topic string,
	curr []admin.PartitionAssignment,
	brokersToRemove []int,
) ([]admin.PartitionAssignment, error) {
	ok, err := assigners.EvaluateAssignments(curr, f.brokers, f.placementConfig)
	if err != nil {
		return nil, err
	} else if !ok {
		return nil, fmt.Errorf("starting assignments on topic %s do not satisfy placement config - assignments: %#v", topic, curr)
	}

	desired := admin.CopyAssignments(curr)

	toRemoveMap := map[int]struct{}{}
	for _, brokerID := range brokersToRemove {
		toRemoveMap[brokerID] = struct{}{}
	}

	for i := 0; i < len(desired[0].Replicas); i++ {
		for {
			counts := f.brokerCounts(desired, i, topic, toRemoveMap)

			// Partition brokers into two sets, one with lowest counts and one with highest
			lowerCounts, upperCounts := partitionCounts(counts)
			swapMade := false

		outerLoop:
			for _, upperCount := range upperCounts {
				for _, lowerCount := range lowerCounts {
					lowerBroker := lowerCount.brokerID

					if f.shouldTryReplace(lowerCount, upperCount) {
						for _, upperPartition := range upperCount.partitions {
							swapMade = f.tryReplacement(desired, lowerBroker, upperPartition, i)
							if swapMade {
								break outerLoop
							}
						}
					}
				}
			}

			if !swapMade {
				// No swaps possible, continue to the next index
				break
			}
		}
	}

	for _, assignment := range desired {
		for _, brokerID := range assignment.Replicas {
			if _, ok := toRemoveMap[brokerID]; ok {
				return nil, fmt.Errorf(
					"Could not find a feasible replacement for broker %d",
					brokerID,
				)
			}
		}
	}

	return desired, nil
}

type brokerCount struct {
	brokerID    int
	indexCount  int
	totalCount  int
	partitions  []int
	toBeRemoved bool

	// score is used for breaking ties among brokers with the same count
	score int
}

func (b brokerCount) isSmaller(other brokerCount) bool {
	// Ensure that items to be removed are always last (i.e., considered to have the highest count)
	if !b.toBeRemoved && other.toBeRemoved {
		return true
	} else if b.toBeRemoved && !other.toBeRemoved {
		return false
	} else {
		// Then, within each toBeRemoved partition, by the count for this index, followed by the
		// total count for the topic, followed by the score
		return b.indexCount < other.indexCount ||
			(b.indexCount == other.indexCount && b.totalCount < other.totalCount) ||
			(b.indexCount == other.indexCount && b.totalCount == other.totalCount && b.score < other.score)
	}
}

func (f *FrequencyRebalancer) brokerCounts(
	curr []admin.PartitionAssignment,
	index int,
	topic string,
	toRemove map[int]struct{},
) []brokerCount {
	allPositionBrokerCounts := map[int]int{}
	indexPartitions := map[int][]int{}

	for _, assignment := range curr {
		for i, brokerID := range assignment.Replicas {
			allPositionBrokerCounts[brokerID]++

			if i == index {
				indexPartitions[brokerID] = append(
					indexPartitions[brokerID],
					assignment.ID,
				)
			}
		}
	}

	brokerCounts := []brokerCount{}

	for _, broker := range f.brokers {
		_, toBeRemoved := toRemove[broker.ID]

		brokerCounts = append(
			brokerCounts,
			brokerCount{
				brokerID:    broker.ID,
				indexCount:  len(indexPartitions[broker.ID]),
				totalCount:  allPositionBrokerCounts[broker.ID],
				partitions:  indexPartitions[broker.ID],
				toBeRemoved: toBeRemoved,
				score:       f.picker.ScoreBroker(topic, broker.ID, 0, index),
			},
		)
	}

	// Shuffle partition order for each broker
	for _, brokerCount := range brokerCounts {
		partitionScores := []int{}
		for _, partition := range brokerCount.partitions {
			partitionScores = append(
				partitionScores,
				f.picker.ScoreBroker(topic, brokerCount.brokerID, partition, index),
			)
		}

		sort.Slice(brokerCount.partitions, func(a, b int) bool {
			return partitionScores[a] < partitionScores[b]
		})
	}

	sort.Slice(brokerCounts, func(a, b int) bool {
		return brokerCounts[a].isSmaller(brokerCounts[b])
	})

	return brokerCounts
}

func (f *FrequencyRebalancer) shouldTryReplace(
	lowerCount brokerCount,
	higherCount brokerCount,
) bool {
	if higherCount.toBeRemoved && higherCount.indexCount > 0 {
		// If the higher item is to be removed, and has a positive index count, we should
		// definitely try to replace it
		return true
	} else if lowerCount.toBeRemoved {
		// Never try to insert a broker to be removed back into a partition
		return false
	} else if lowerCount.indexCount+1 < higherCount.indexCount {
		// Doing this replacement will strictly improve the in-index balance
		return true
	} else if lowerCount.indexCount+1 == higherCount.indexCount {
		// Doing this placement is neutral from an in-index perspective, look at the total topic
		if lowerCount.totalCount+1 < higherCount.totalCount {
			return true
		}
	}

	return false
}

func (f *FrequencyRebalancer) tryReplacement(
	curr []admin.PartitionAssignment,
	lowerBroker int,
	upperPartition int,
	index int,
) bool {
	upperBroker := curr[upperPartition].Replicas[index]

	// Try to put the lower count broker in the higher-count slot
	curr[upperPartition].Replicas[index] = lowerBroker

	// Check if the replacement is ok
	ok, err := assigners.EvaluateAssignments(curr, f.brokers, f.placementConfig)
	if ok && err == nil {
		return true
	}

	// If not, undo the replacement and return
	curr[upperPartition].Replicas[index] = upperBroker

	return false
}

func partitionCounts(brokerCounts []brokerCount) (
	[]brokerCount,
	[]brokerCount,
) {
	lower := []brokerCount{}
	var upper []brokerCount

	for b, brokerCount := range brokerCounts {
		if b >= len(brokerCounts)/2 && brokerCount.indexCount > 0 {
			// Put remainder of brokerCounts in reverse order in the upper slice
			upper = reverseCounts(brokerCounts[b:])
			break
		}
		lower = append(lower, brokerCount)
	}

	return lower, upper
}

func reverseCounts(counts []brokerCount) []brokerCount {
	reversed := []brokerCount{}

	for i := 0; i < len(counts); i++ {
		reversed = append(
			reversed,
			counts[len(counts)-1-i],
		)
	}

	return reversed
}
