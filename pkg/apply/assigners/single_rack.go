package assigners

import (
	"fmt"

	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/apply/pickers"
)

// SingleRackAssigner is an assigner that ensures that the replicas of each
// partition are in the same rack as the leader. The algorithm is:
//
// for each partition:
//
//	for each non-leader replica:
//	  if replica not in same rack as leader:
//	    change replica to a placeholder (-1)
//
// then:
//
// for each partition:
//
//	for each non-leader replica:
//	  if replica is set to placeholder:
//	    use picker to replace it with a broker in the target rack
//
// Note that this assigner doesn't make any leader changes. Thus, the assignments
// need to already be leader balanced before we make the changes with this assigner.
type SingleRackAssigner struct {
	brokers        []admin.BrokerInfo
	brokerRacks    map[int]string
	brokersPerRack map[string][]int
	picker         pickers.Picker
}

var _ Assigner = (*SingleRackAssigner)(nil)

// NewSingleRackAssigner creates and returns a SingleRackAssigner instance.
func NewSingleRackAssigner(
	brokers []admin.BrokerInfo,
	picker pickers.Picker,
) *SingleRackAssigner {
	return &SingleRackAssigner{
		brokers:        brokers,
		brokerRacks:    admin.BrokerRacks(brokers),
		brokersPerRack: admin.BrokersPerRack(brokers),
		picker:         picker,
	}
}

// Assign returns a new partition assignment according to the assigner-specific logic.
func (s *SingleRackAssigner) Assign(
	topic string,
	curr []admin.PartitionAssignment,
) ([]admin.PartitionAssignment, error) {
	if err := admin.CheckAssignments(curr); err != nil {
		return nil, err
	}

	// Check to make sure that the number of brokers in each rack is >= number of
	// replicas. Otherwise, we won't be able to find a feasible assignment.
	for rack, brokers := range s.brokersPerRack {
		if len(brokers) < len(curr[0].Replicas) {
			return nil, fmt.Errorf(
				"Rack %s does not have enough brokers for in-rack placement",
				rack,
			)
		}
	}

	desired := admin.CopyAssignments(curr)

	// First, null-out any replicas that are in the wrong rack
	for index, assignment := range desired {
		leader := assignment.Replicas[0]
		leaderRack := s.brokerRacks[leader]

		for r, replica := range assignment.Replicas {
			replicaRack := s.brokerRacks[replica]

			if replicaRack != leaderRack {
				desired[index].Replicas[r] = -1
			}
		}
	}

	// Then, go back and replace all of the marked replicas
	for index, assignment := range desired {
		leader := assignment.Replicas[0]
		leaderRack := s.brokerRacks[leader]

		for r, replica := range assignment.Replicas {
			if replica == -1 {
				err := s.picker.PickNew(
					topic,
					s.brokersPerRack[leaderRack],
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
