package rebalancers

import (
	"github.com/efcloud/topicctl/pkg/admin"
)

// Rebalancer is an interface for structs that figure out how to
// reassign replicas in existing topic partitions in order to ensure that all brokers
// are evenly represented. It also supports removing brokers (e.g., if they are to be removed
// from the cluster).
type Rebalancer interface {
	Rebalance(
		topic string,
		currAssignments []admin.PartitionAssignment,
		brokersToRemove []int,
	) ([]admin.PartitionAssignment, error)
}
