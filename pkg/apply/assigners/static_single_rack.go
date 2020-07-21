package assigners

import (
	"fmt"

	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/apply/pickers"
)

// StaticSingleRackAssigner is an Assigner that assigns replicas within a static rack per
// partition. This might be useful for cases where we need particular partitions in particular
// racks, but don't care about the per-replica placement (assuming that the rack is ok).
//
// The following algorithm is used:
//
// for each partition:
//   for each replica:
//     if replica not in the desired (static) rack:
//       change the replica to a placeholder (-1)
//
// then:
//
// for each partition:
//   for each replica:
//     if replica set to the placeholder:
//       use picker to pick a broker from the set of all brokers in the target rack
//
// In the case of ties, the lowest indexed broker is picked (if randomize is false) or
// a repeatably random choice (if randomize is true).
type StaticSingleRackAssigner struct {
	rackAssignments []string
	brokers         []admin.BrokerInfo
	brokerRacks     map[int]string
	brokersPerRack  map[string][]int
	picker          pickers.Picker
}

var _ Assigner = (*StaticSingleRackAssigner)(nil)

func NewStaticSingleRackAssigner(
	brokers []admin.BrokerInfo,
	rackAssignments []string,
	picker pickers.Picker,
) *StaticSingleRackAssigner {
	return &StaticSingleRackAssigner{
		rackAssignments: rackAssignments,
		brokers:         brokers,
		brokerRacks:     admin.BrokerRacks(brokers),
		brokersPerRack:  admin.BrokersPerRack(brokers),
		picker:          picker,
	}
}

func (s *StaticSingleRackAssigner) Assign(
	topic string,
	curr []admin.PartitionAssignment,
) ([]admin.PartitionAssignment, error) {
	if err := admin.CheckAssignments(curr); err != nil {
		return nil, err
	}

	// Check to make sure that the static assignments are valid racks and that there are enough
	// brokers per rack.
	for _, rack := range s.rackAssignments {
		rackCount := len(s.brokersPerRack[rack])

		if rackCount == 0 {
			return nil, fmt.Errorf("Could not find any brokers for rack %s", rack)
		} else if rackCount < len(curr[0].Replicas) {
			return nil, fmt.Errorf(
				"Rack %s does not have enough brokers for in-rack placement",
				rack,
			)
		}
	}

	desired := admin.CopyAssignments(curr)

	// First, null-out any replicas that are in the wrong rack
	for i := 0; i < len(desired); i++ {
		targetRack := s.rackAssignments[i]

		for j := 0; j < len(desired[i].Replicas); j++ {
			replica := desired[i].Replicas[j]

			if s.brokerRacks[replica] != targetRack {
				desired[i].Replicas[j] = -1
			}
		}
	}

	// Then, go back and replace all of the marked replicas
	for i := 0; i < len(desired); i++ {
		targetRack := s.rackAssignments[i]

		for j := 0; j < len(desired[i].Replicas); j++ {
			if desired[i].Replicas[j] == -1 {
				err := s.picker.PickNew(
					topic,
					s.brokersPerRack[targetRack],
					desired,
					i,
					j,
				)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	return desired, nil
}
