package admin

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBrokerHelpers(t *testing.T) {
	brokers := []BrokerInfo{
		{
			ID:   1,
			Rack: "rack1",
			Config: map[string]string{
				"leader.replication.throttled.rate": "1234",
			},
		},
		{
			ID:   2,
			Rack: "rack2",
			Config: map[string]string{
				"follower.replication.throttled.rate": "12345",
			},
		},
		{
			ID:   3,
			Rack: "rack1",
			Config: map[string]string{
				"leader.replication.throttled.rate":   "12345",
				"follower.replication.throttled.rate": "12345",
			},
		},
		{
			ID:   4,
			Rack: "rack2",
		},
		{
			ID:   5,
			Rack: "rack3",
		},
	}
	brokerIDs := BrokerIDs(brokers)
	throttledBrokerIDs := ThrottledBrokerIDs(brokers)
	brokerRacks := BrokerRacks(brokers)
	brokersPerRack := BrokersPerRack(brokers)
	brokerCountsPerRack := BrokerCountsPerRack(brokers)
	racks := DistinctRacks(brokers)

	assert.Equal(
		t,
		[]int{1, 2, 3, 4, 5},
		brokerIDs,
	)
	assert.Equal(
		t,
		[]int{1, 2, 3},
		throttledBrokerIDs,
	)
	assert.Equal(
		t,
		map[int]string{
			1: "rack1",
			2: "rack2",
			3: "rack1",
			4: "rack2",
			5: "rack3",
		},
		brokerRacks,
	)
	assert.Equal(
		t,
		map[string][]int{
			"rack1": {1, 3},
			"rack2": {2, 4},
			"rack3": {5},
		},
		brokersPerRack,
	)
	assert.Equal(
		t,
		map[string]int{
			"rack1": 2,
			"rack2": 2,
			"rack3": 1,
		},
		brokerCountsPerRack,
	)
	assert.Equal(
		t,
		[]string{"rack1", "rack2", "rack3"},
		racks,
	)
}

func TestTopicRackHelpers(t *testing.T) {
	testBrokers := []BrokerInfo{
		{
			ID:   1,
			Rack: "rack1",
		},
		{
			ID:   2,
			Rack: "rack2",
		},
		{
			ID:   3,
			Rack: "rack1",
		},
		{
			ID:   4,
			Rack: "rack2",
		},
		{
			ID:   5,
			Rack: "rack3",
		},
	}
	testTopic := TopicInfo{
		Config: map[string]string{
			"key":          "value",
			"retention.ms": "36000000",
		},
		Partitions: []PartitionInfo{
			{
				Topic:    "topic1",
				ID:       0,
				Leader:   1,
				Replicas: []int{1, 2, 5},
				ISR:      []int{1, 2},
			},
			{
				Topic:    "topic1",
				ID:       1,
				Leader:   2,
				Replicas: []int{2, 4},
				ISR:      []int{1, 2},
			},
			{
				Topic:    "topic1",
				ID:       2,
				Leader:   3,
				Replicas: []int{3, 5},
				ISR:      []int{1, 2},
			},
		},
	}

	assert.Equal(t, 10*time.Hour, testTopic.Retention())
	assert.Equal(t, 3, testTopic.MaxReplication())
	assert.Equal(t, 2, testTopic.MaxISR())
	assert.Equal(t, 3, MaxReplication([]TopicInfo{testTopic}))
	assert.True(t, HasLeaders([]TopicInfo{testTopic}))
	assert.Equal(t, []int{0, 1, 2}, testTopic.PartitionIDs())

	brokerRacks := BrokerRacks(testBrokers)
	minRacks, maxRacks, err := testTopic.RackCounts(brokerRacks)
	assert.Nil(t, err)
	assert.Equal(t, minRacks, 1)
	assert.Equal(t, maxRacks, 3)

	numRacks, err := testTopic.Partitions[0].NumRacks(brokerRacks)
	assert.Nil(t, err)
	assert.Equal(t, 3, numRacks)

	racks, err := testTopic.Partitions[0].Racks(brokerRacks)
	assert.Nil(t, err)
	assert.Equal(t, []string{"rack1", "rack2", "rack3"}, racks)
}

func TestTopicThrottleHelpers(t *testing.T) {
	topics := []TopicInfo{
		{
			Name: "topic1",
			Config: map[string]string{
				"leader.replication.throttled.replicas": "1:2,3:4",
			},
		},
		{
			Name: "topic2",
			Config: map[string]string{
				"follower.replication.throttled.replicas": "1:2,3:4",
			},
		},
		{
			Name: "topic3",
			Config: map[string]string{
				"leader.replication.throttled.replicas":   "1:2,3:4",
				"follower.replication.throttled.replicas": "1:2,3:4",
			},
		},
		{
			Name:   "topic4",
			Config: map[string]string{},
		},
	}
	throttledTopicNames := ThrottledTopicNames(topics)
	assert.Equal(
		t,
		[]string{
			"topic1",
			"topic2",
			"topic3",
		},
		throttledTopicNames,
	)

}

func TestTopicSyncHelpers(t *testing.T) {
	testTopicInSync := TopicInfo{
		Partitions: []PartitionInfo{
			{
				Topic:    "topic1",
				ID:       0,
				Leader:   1,
				Replicas: []int{1, 2, 5},
				ISR:      []int{5, 2, 1},
			},
			{
				Topic:    "topic1",
				ID:       1,
				Leader:   2,
				Replicas: []int{2, 4},
				ISR:      []int{2, 4},
			},
			{
				Topic:    "topic1",
				ID:       2,
				Leader:   3,
				Replicas: []int{3, 5},
				ISR:      []int{5, 3},
			},
		},
	}
	assert.True(t, testTopicInSync.AllReplicasInSync())
	assert.Equal(t, []PartitionInfo{}, testTopicInSync.OutOfSyncPartitions(nil))
	assert.True(t, testTopicInSync.AllLeadersCorrect())
	assert.Equal(t, []PartitionInfo{}, testTopicInSync.WrongLeaderPartitions(nil))

	testTopicOutOfSync := TopicInfo{
		Partitions: []PartitionInfo{
			{
				Topic:    "topic1",
				ID:       0,
				Leader:   1,
				Replicas: []int{1, 2, 5, 6},
				ISR:      []int{5, 2, 1},
			},
			{
				Topic:    "topic1",
				ID:       1,
				Leader:   2,
				Replicas: []int{2, 4},
				ISR:      []int{2, 4},
			},
			{
				Topic:    "topic1",
				ID:       2,
				Leader:   2,
				Replicas: []int{3, 2},
				ISR:      []int{5, 3},
			},
		},
	}
	assert.False(t, testTopicOutOfSync.AllReplicasInSync())
	assert.Equal(
		t,
		[]PartitionInfo{
			testTopicOutOfSync.Partitions[0],
			testTopicOutOfSync.Partitions[2],
		},
		testTopicOutOfSync.OutOfSyncPartitions(nil),
	)
	assert.Equal(
		t,
		[]PartitionInfo{
			testTopicOutOfSync.Partitions[2],
		},
		testTopicOutOfSync.OutOfSyncPartitions([]int{1, 2, 3}),
	)
	assert.False(t, testTopicOutOfSync.AllLeadersCorrect())
	assert.Equal(
		t,
		[]PartitionInfo{
			testTopicOutOfSync.Partitions[2],
		},
		testTopicOutOfSync.WrongLeaderPartitions(nil),
	)
	assert.Equal(
		t,
		[]PartitionInfo{},
		testTopicOutOfSync.WrongLeaderPartitions([]int{1}),
	)
}

func TestPartitionAssignmentHelpers(t *testing.T) {
	testTopic := TopicInfo{
		Config: map[string]string{
			"key":          "value",
			"retention.ms": "36000000",
		},
		Partitions: []PartitionInfo{
			{
				Topic:    "topic1",
				ID:       0,
				Leader:   1,
				Replicas: []int{1, 2, 5},
			},
			{
				Topic:    "topic1",
				ID:       1,
				Leader:   2,
				Replicas: []int{2, 4},
			},
			{
				Topic:    "topic1",
				ID:       2,
				Leader:   3,
				Replicas: []int{3, 5},
			},
		},
	}
	result := testTopic.ToAssignments()
	expectedAssignments := []PartitionAssignment{
		{
			ID:       0,
			Replicas: []int{1, 2, 5},
		},
		{
			ID:       1,
			Replicas: []int{2, 4},
		},
		{
			ID:       2,
			Replicas: []int{3, 5},
		},
	}

	assert.Equal(
		t,
		expectedAssignments,
		result,
	)
	replicas, err := AssignmentsToReplicas(result)
	assert.Nil(t, err)

	assert.Equal(
		t,
		[][]int{
			{1, 2, 5},
			{2, 4},
			{3, 5},
		},
		replicas,
	)
	assert.Equal(
		t,
		expectedAssignments,
		ReplicasToAssignments(replicas),
	)

	assignment := PartitionAssignment{
		ID:       0,
		Replicas: []int{1, 2, 5},
	}
	assert.Equal(t, 1, assignment.Index(2))
	assert.Equal(t, -1, assignment.Index(3))
	assert.Equal(
		t,
		map[string]struct{}{
			"rack1": {},
			"rack2": {},
			"rack3": {},
		},
		assignment.DistinctRacks(
			map[int]string{
				1: "rack1",
				2: "rack2",
				3: "rack2",
				4: "rack1",
				5: "rack3",
			},
		),
	)

	copied := assignment.Copy()
	assert.Equal(t, assignment, copied)
	// Slices are not shared
	copied.Replicas[0] = 8
	assert.NotEqual(t, assignment, copied)
}

func TestMaxPartitionsHelpers(t *testing.T) {
	assert.Equal(
		t,
		map[int]int{
			1: 1,
			2: 2,
			3: 2,
			4: 2,
			5: 1,
		},
		MaxPartitionsPerBroker(
			ReplicasToAssignments([][]int{
				{
					1, 2, 3,
				},
				{
					3, 4, 5,
				},
			}),
			ReplicasToAssignments([][]int{
				{
					1, 2, 4,
				},
				{
					4, 5, 2,
				},
			}),
		),
	)

	assert.Equal(
		t,
		map[int]int{
			1: 2,
			2: 2,
			3: 3,
			4: 1,
			5: 1,
		},
		MaxPartitionsPerBroker(
			ReplicasToAssignments([][]int{
				{
					1, 2, 3,
				},
				{
					3, 4, 2,
				},
				{
					1, 5, 3,
				},
			}),
		),
	)
}

func TestPartitionComparisonHelpers(t *testing.T) {
	assert.True(
		t,
		SameBrokers(
			PartitionAssignment{
				Replicas: []int{1, 2, 3},
			},
			PartitionAssignment{
				Replicas: []int{3, 2, 1},
			},
		),
	)
	assert.False(
		t,
		SameBrokers(
			PartitionAssignment{
				Replicas: []int{1, 2, 3},
			},
			PartitionAssignment{
				Replicas: []int{3, 2, 4},
			},
		),
	)

	assert.Equal(
		t,
		[]PartitionAssignment{
			{
				ID:       2,
				Replicas: []int{1, 2, 3},
			},
			{
				ID:       3,
				Replicas: []int{4, 5, 6},
			},
		},
		AssignmentsToUpdate(
			[]PartitionAssignment{
				{
					ID:       1,
					Replicas: []int{1, 2, 3},
				},
				{
					ID:       2,
					Replicas: []int{3, 4, 5},
				},
				{
					ID:       3,
					Replicas: []int{4, 6, 5},
				},
			},
			[]PartitionAssignment{
				{
					ID:       1,
					Replicas: []int{1, 2, 3},
				},
				{
					ID:       2,
					Replicas: []int{1, 2, 3},
				},
				{
					ID:       3,
					Replicas: []int{4, 5, 6},
				},
			},
		),
	)
}

func TestDiffHelpers(t *testing.T) {
	curr := []PartitionAssignment{
		// Invert order to test sorting
		{
			ID:       2,
			Replicas: []int{1, 2, 3},
		},
		{
			ID:       1,
			Replicas: []int{1, 2, 3},
		},
		{
			ID:       3,
			Replicas: []int{4, 5, 6},
		},
		{
			ID:       4,
			Replicas: []int{6, 7, 8},
		},
	}
	desired := []PartitionAssignment{
		{
			ID:       1,
			Replicas: []int{1, 2, 3},
		},
		{
			ID:       2,
			Replicas: []int{1, 2, 4},
		},
		{
			ID:       3,
			Replicas: []int{5, 4, 6},
		},
		{
			ID:       4,
			Replicas: []int{8, 7, 2},
		},
		{
			ID:       5,
			Replicas: []int{1, 3, 4},
		},
	}

	assert.Equal(
		t,
		[]AssignmentDiff{
			{
				PartitionID: 1,
				Old:         curr[1],
				New:         desired[0],
			},
			{
				PartitionID: 2,
				Old:         curr[0],
				New:         desired[1],
			},
			{
				PartitionID: 3,
				Old:         curr[2],
				New:         desired[2],
			},
			{
				PartitionID: 4,
				Old:         curr[3],
				New:         desired[3],
			},
			{
				PartitionID: 5,
				Old:         PartitionAssignment{},
				New:         desired[4],
			},
		},
		AssignmentDiffs(curr, desired),
	)

	assert.Equal(
		t,
		[]PartitionAssignment{
			{
				ID:       2,
				Replicas: []int{1, 2, 4},
			},
			{
				ID:       3,
				Replicas: []int{5, 4, 6},
			},
			{
				ID:       4,
				Replicas: []int{8, 7, 2},
			},
			{
				ID:       5,
				Replicas: []int{1, 3, 4},
			},
		},
		AssignmentsToUpdate(curr, desired),
	)
	assert.Equal(
		t,
		[]int{3, 4},
		NewLeaderPartitions(curr, desired),
	)
}
