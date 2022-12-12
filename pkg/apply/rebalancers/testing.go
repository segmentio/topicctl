package rebalancers

import (
	"fmt"
	"testing"

	"github.com/efcloud/topicctl/pkg/admin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type rebalancerTestCase struct {
	description string
	topic       string
	curr        [][]int
	toRemove    []int
	expected    [][]int
	err         error
}

func (r rebalancerTestCase) evaluate(t *testing.T, rebalancer Rebalancer) {
	desired, err := rebalancer.Rebalance(
		r.topic,
		admin.ReplicasToAssignments(r.curr),
		r.toRemove,
	)
	if r.err != nil {
		require.NotNil(t, err, r.description)
	} else {
		require.Nil(t, err, r.description)

		replicas, err := admin.AssignmentsToReplicas(desired)
		require.NoError(t, err)

		assert.NoError(t, admin.CheckAssignments(desired), r.description)
		assert.Equal(
			t,
			r.expected,
			replicas,
			r.description,
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
