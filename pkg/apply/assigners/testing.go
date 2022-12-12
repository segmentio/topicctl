package assigners

import (
	"fmt"
	"testing"

	"github.com/efcloud/topicctl/pkg/admin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type assignerTestCase struct {
	description string
	topic       string
	curr        [][]int
	expected    [][]int
	checker     func([]admin.PartitionAssignment) bool
	err         error
}

func (a assignerTestCase) evaluate(t *testing.T, assigner Assigner) {
	desired, err := assigner.Assign(
		a.topic,
		admin.ReplicasToAssignments(a.curr),
	)
	if a.err != nil {
		require.NotNil(t, err, a.description)
	} else {
		require.Nil(t, err, a.description)

		replicas, err := admin.AssignmentsToReplicas(desired)
		require.NoError(t, err)

		assert.NoError(t, admin.CheckAssignments(desired), a.description)
		assert.Equal(
			t,
			a.expected,
			replicas,
			a.description,
		)
		if a.checker != nil {
			assert.True(t, a.checker(desired), a.description)
		}
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
