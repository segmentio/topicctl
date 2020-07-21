package extenders

import "github.com/segmentio/topicctl/pkg/admin"

// StaticExtender is an Extender that ignores the current state and assigns
// based on the value of the Assignments field. Generally intended for testing
// purposes.
type StaticExtender struct {
	Assignments []admin.PartitionAssignment
}

var _ Extender = (*StaticExtender)(nil)

func (s *StaticExtender) Extend(
	topic string,
	curr []admin.PartitionAssignment,
	newPartitions int,
) ([]admin.PartitionAssignment, error) {
	if err := admin.CheckAssignments(curr); err != nil {
		return nil, err
	}
	return s.Assignments, nil
}
