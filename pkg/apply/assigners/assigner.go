package assigners

import (
	"github.com/efcloud/topicctl/pkg/admin"
)

// Assigner is an interface for structs that figure out how to
// reassign replicas in existing topic partitions.
type Assigner interface {
	Assign(
		topic string,
		currAssignments []admin.PartitionAssignment,
	) ([]admin.PartitionAssignment, error)
}
