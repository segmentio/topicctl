package assigners

import (
	"fmt"
	"github.com/efcloud/topicctl/pkg/admin"
	"github.com/efcloud/topicctl/pkg/apply/pickers"
	"sort"
)

// CrossRackAssigner is an assigner that ensures that the replicas of each
// partition are on different racks than each other. The algorithm is:
//
// https://segment.atlassian.net/browse/DRES-922?focusedCommentId=237288
//
// for each partition:
//   for each non-leader replica:
//     if replica is in same rack as leader:
//       change replica to a placeholder (-1)
//
// then:
//
// for each partition:
//   for each non-leader replica:
//     if replica is set to placeholder:
//       use picker to replace it with a broker in a different rack than the leader and any other replicas
//
// Note that this assigner doesn't make any leader changes. Thus, the assignments
// need to already be leader balanced before we make the changes with this assigner.
type CrossRackAssigner struct {
	brokers        []admin.BrokerInfo
	brokerRacks    map[int]string
	brokersPerRack map[string][]int
	picker         pickers.Picker
}

var _ Assigner = (*CrossRackAssigner)(nil)

// NewCrossRackAssigner creates and returns a CrossRackAssigner instance.
func NewCrossRackAssigner(
	brokers []admin.BrokerInfo,
	picker pickers.Picker,
) *CrossRackAssigner {
	return &CrossRackAssigner{
		brokers:        brokers,
		brokerRacks:    admin.BrokerRacks(brokers),
		brokersPerRack: admin.BrokersPerRack(brokers),
		picker:         picker,
	}
}

// Assign returns a new partition assignment according to the assigner-specific logic.
func (s *CrossRackAssigner) Assign(
	topic string,
	curr []admin.PartitionAssignment,
) ([]admin.PartitionAssignment, error) {
	if err := admin.CheckAssignments(curr); err != nil {
		return nil, err
	}

	// Check to make sure that the number of racks is >= number of replicas.
	// Otherwise, we won't be able to find a feasible assignment.
	if len(s.brokersPerRack) < len(curr[0].Replicas) {
		return nil, fmt.Errorf("Do not have enough racks for cross-rack placement")
	}

	desired := admin.CopyAssignments(curr)

	// First, null-out any replicas that are in the wrong rack, and record the racks we keep
	usedRacksPerPartition := make([]map[string]bool, len(curr))
	for index, assignment := range desired {
		usedRacks := make(map[string]bool, len(curr))
		for r, replica := range assignment.Replicas {
			replicaRack := s.brokerRacks[replica]
			if _, used := usedRacks[replicaRack]; used {
				// Rack has already been seen, null it out since we need to replace it
				desired[index].Replicas[r] = -1
			} else {
				// First time using this rack for this partition
				usedRacks[replicaRack] = true
			}
		}
		usedRacksPerPartition[index] = usedRacks
	}

	// Which racks did we not use yet?
	availableRacksPerPartition := make([][]string, 0, len(curr))
	for _, usedRacks := range usedRacksPerPartition {
		availableRacks := make(map[string]bool)
		for _, rack := range s.brokerRacks {
			if _, used := usedRacks[rack]; !used {
				availableRacks[rack] = true
			}
		}
		sortedRacks := make([]string, 0, len(availableRacks))
		for r := range availableRacks {
			sortedRacks = append(sortedRacks, r)
		}
		sort.Strings(sortedRacks)
		availableRacksPerPartition = append(availableRacksPerPartition, sortedRacks)
	}

	// Then, go back and replace all of the marked replicas with replicas from available racks
	for index, assignment := range desired {
		for r, replica := range assignment.Replicas {
			if replica == -1 {
				// Pop the 1st rack off and pick one of the brokers in that rack
				targetRack, remainingRacks := availableRacksPerPartition[index][0], availableRacksPerPartition[index][1:]
				availableRacksPerPartition[index] = remainingRacks
				targetBrokers := s.brokersPerRack[targetRack]
				err := s.picker.PickNew(
					topic,
					targetBrokers,
					desired,
					index,
					r,
				)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	return desired, nil
}
