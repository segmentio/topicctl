package assigners

import "github.com/segmentio/topicctl/pkg/admin"

// StaticAssigner is an Assigner that ignores the current state and assigns
// based on the value of the Assignments field. Generally intended for
// testing purposes.
type StaticAssigner struct {
	Assignments []admin.PartitionAssignment
}

var _ Assigner = (*StaticAssigner)(nil)

func (s *StaticAssigner) Assign(
	topic string,
	curr []admin.PartitionAssignment,
) ([]admin.PartitionAssignment, error) {
	if err := admin.CheckAssignments(curr); err != nil {
		return nil, err
	}
	return s.Assignments, nil
}
