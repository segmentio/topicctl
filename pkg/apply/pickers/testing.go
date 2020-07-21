package pickers

import (
	"fmt"
	"testing"

	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pickNewTestCase struct {
	// Inputs
	topic         string
	brokerChoices []int
	curr          [][]int
	partition     int
	index         int

	description    string
	expectedChoice int
	expectedErr    bool
}

func (p pickNewTestCase) evaluate(t *testing.T, picker Picker) {
	currAssignments := admin.ReplicasToAssignments(p.curr)

	expectedAssignments := admin.ReplicasToAssignments(p.curr)
	expectedAssignments[p.partition].Replicas[p.index] = p.expectedChoice

	err := picker.PickNew(
		p.topic,
		p.brokerChoices,
		currAssignments,
		p.partition,
		p.index,
	)
	if p.expectedErr {
		assert.NotNil(t, err, p.description)
	} else {
		require.Nil(t, err, p.description)

		expectedReplicas, err := admin.AssignmentsToReplicas(expectedAssignments)
		require.Nil(t, err, p.description)

		updatedReplicas, err := admin.AssignmentsToReplicas(currAssignments)
		require.Nil(t, err, p.description)

		assert.Equal(
			t,
			expectedReplicas,
			updatedReplicas,
			p.description,
		)
	}
}

type sortRemovalsTestCase struct {
	// Inputs
	topic            string
	partitionChoices []int
	curr             [][]int
	index            int

	description      string
	expectedOrdering []int
	expectedErr      bool
}

func (s sortRemovalsTestCase) evaluate(t *testing.T, picker Picker) {
	currAssignments := admin.ReplicasToAssignments(s.curr)
	expectedAssignments := admin.ReplicasToAssignments(s.curr)

	updatedOrdering := util.CopyInts(s.partitionChoices)

	err := picker.SortRemovals(
		s.topic,
		updatedOrdering,
		currAssignments,
		s.index,
	)
	if s.expectedErr {
		assert.NotNil(t, err, s.description)
	} else {
		require.Nil(t, err, s.description)

		assert.Equal(
			t,
			s.expectedOrdering,
			updatedOrdering,
		)

		// Replicas should be unchanged
		expectedReplicas, err := admin.AssignmentsToReplicas(expectedAssignments)
		require.Nil(t, err, s.description)

		updatedReplicas, err := admin.AssignmentsToReplicas(currAssignments)
		require.Nil(t, err, s.description)

		assert.Equal(
			t,
			expectedReplicas,
			updatedReplicas,
			s.description,
		)
	}
}

func testBrokers(numBrokers int, numRacks int) []admin.BrokerInfo {
	brokers := []admin.BrokerInfo{}

	for b := 0; b < numBrokers; b++ {
		brokers = append(
			brokers,
			admin.BrokerInfo{
				ID:   b + 1,
				Rack: fmt.Sprintf("zone%d", (b%numRacks)+1),
			},
		)
	}

	return brokers
}
