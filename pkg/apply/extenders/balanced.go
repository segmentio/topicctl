package extenders

import (
	"fmt"

	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/apply/pickers"
	log "github.com/sirupsen/logrus"
)

// BalancedExtender adds extra partition assignments in a "balanced" way. The current
// algorithm is:
//
// for each new partition:
//   set the leader rack to the next rack in the cycle
//   choose the leader using the picker
//   for each follower:
//     set the rack to either the same one as the leader (if inRack true) or the next one in the
//       cycle (if inRack false)
//     pick the follower using the picker
type BalancedExtender struct {
	brokers        []admin.BrokerInfo
	inRack         bool
	picker         pickers.Picker
	racks          []string
	brokerRacks    map[int]string
	brokersPerRack map[string][]int
}

var _ Extender = (*BalancedExtender)(nil)

// NewBalancedExtender returns a new BalancedExtender instance.
func NewBalancedExtender(
	brokers []admin.BrokerInfo,
	inRack bool,
	picker pickers.Picker,
) *BalancedExtender {
	return &BalancedExtender{
		brokers:        brokers,
		inRack:         inRack,
		picker:         picker,
		racks:          admin.DistinctRacks(brokers),
		brokerRacks:    admin.BrokerRacks(brokers),
		brokersPerRack: admin.BrokersPerRack(brokers),
	}
}

// Extend returns partition assignments for the extension of the argument topic.
func (b *BalancedExtender) Extend(
	topic string,
	curr []admin.PartitionAssignment,
	extraPartitions int,
) ([]admin.PartitionAssignment, error) {
	if extraPartitions%len(b.racks) != 0 {
		log.Warn("Extra partitions are not a multiple of the number of racks")
	}

	if b.inRack {
		// Check to make sure that the number of brokers in each rack is >= number of
		// replicas. Otherwise, we won't be able to find a feasible assignment.
		for rack, brokers := range b.brokersPerRack {
			if len(brokers) < len(curr[0].Replicas) {
				return nil, fmt.Errorf(
					"Rack %s does not have enough brokers for in-rack placement",
					rack,
				)
			}
		}
	}

	desired := admin.CopyAssignments(curr)

	for i := 0; i < extraPartitions; i++ {
		partitionID := i + len(curr)
		nextAssignment := admin.PartitionAssignment{
			ID:       partitionID,
			Replicas: []int{},
		}

		// Put in placeholders for replicas
		for j := 0; j < len(curr[0].Replicas); j++ {
			nextAssignment.Replicas = append(
				nextAssignment.Replicas,
				-1,
			)
		}

		desired = append(desired, nextAssignment)

		// Iterate over the positions of each of the replicas
		for j := 0; j < len(curr[0].Replicas); j++ {
			var nextRack string

			if b.inRack {
				nextRack = b.racks[i%len(b.racks)]
			} else {
				nextRack = b.racks[(i+j)%len(b.racks)]
			}

			err := b.picker.PickNew(
				topic,
				b.brokersPerRack[nextRack],
				desired,
				partitionID,
				j,
			)
			if err != nil {
				return nil, err
			}
		}

	}

	return desired, nil
}
