package extenders

import "github.com/segmentio/topicctl/pkg/admin"

// Extender is an interface for structs that determine how
// to add new partitions to an existing topic.
type Extender interface {
	Extend(
		topic string,
		currAssignments []admin.PartitionAssignment,
		newPartitions int,
	) ([]admin.PartitionAssignment, error)
}
