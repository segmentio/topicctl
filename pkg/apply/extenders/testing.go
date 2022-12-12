package extenders

import (
	"fmt"
	"testing"

	"github.com/efcloud/topicctl/pkg/admin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type extenderTestCase struct {
	description     string
	topic           string
	curr            [][]int
	extraPartitions int
	expected        [][]int
	checker         func([]admin.PartitionAssignment) bool
	err             error
}

func (e extenderTestCase) evaluate(t *testing.T, extender Extender) {
	desired, err := extender.Extend(
		e.topic,
		admin.ReplicasToAssignments(e.curr),
		e.extraPartitions,
	)
	if e.err != nil {
		assert.Error(t, err, e.description)
	} else {
		replicas, err := admin.AssignmentsToReplicas(desired)
		require.Nil(t, err, e.description)

		assert.NoError(t, err, e.description)
		assert.NoError(t, admin.CheckAssignments(desired), e.description)
		assert.Equal(
			t,
			e.expected,
			replicas,
			e.description,
		)
		if e.checker != nil {
			assert.True(t, e.checker(desired), e.description)
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
