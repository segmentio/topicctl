package admin

import (
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartitionThrottles(t *testing.T) {
	type partitionThrottleTestCase struct {
		description               string
		curr                      []PartitionAssignment
		desired                   []PartitionAssignment
		expectedLeaderThrottles   []PartitionThrottle
		expectedFollowerThrottles []PartitionThrottle
		expectedConfigEntries     []kafka.ConfigEntry
	}

	testCases := []partitionThrottleTestCase{
		{
			description: "migrations",
			curr: []PartitionAssignment{
				{
					ID:       3,
					Replicas: []int{1, 2, 3},
				},
				{
					ID:       5,
					Replicas: []int{1, 5, 6},
				},
			},
			desired: []PartitionAssignment{
				{
					ID:       3,
					Replicas: []int{1, 7, 3},
				},
				{
					ID:       5,
					Replicas: []int{8, 10, 9},
				},
			},
			expectedLeaderThrottles: []PartitionThrottle{
				{
					Partition: 3,
					Broker:    1,
				},
				{
					Partition: 3,
					Broker:    2,
				},
				{
					Partition: 3,
					Broker:    3,
				},
				{
					Partition: 5,
					Broker:    1,
				},
				{
					Partition: 5,
					Broker:    5,
				},
				{
					Partition: 5,
					Broker:    6,
				},
			},
			expectedFollowerThrottles: []PartitionThrottle{
				{
					Partition: 3,
					Broker:    7,
				},
				{
					Partition: 5,
					Broker:    8,
				},
				{
					Partition: 5,
					Broker:    9,
				},
				{
					Partition: 5,
					Broker:    10,
				},
			},
			expectedConfigEntries: []kafka.ConfigEntry{
				{
					ConfigName:  "leader.replication.throttled.replicas",
					ConfigValue: "3:1,3:2,3:3,5:1,5:5,5:6",
				},
				{
					ConfigName:  "follower.replication.throttled.replicas",
					ConfigValue: "3:7,5:8,5:9,5:10",
				},
			},
		},
		{
			description: "mixed migrations and leader changes",
			curr: []PartitionAssignment{
				{
					ID:       3,
					Replicas: []int{1, 2, 3},
				},
				{
					ID:       5,
					Replicas: []int{1, 5, 6},
				},
			},
			desired: []PartitionAssignment{
				{
					ID:       3,
					Replicas: []int{2, 3, 1},
				},
				{
					ID:       5,
					Replicas: []int{1, 6, 7},
				},
			},
			expectedLeaderThrottles: []PartitionThrottle{
				{
					Partition: 5,
					Broker:    1,
				},
				{
					Partition: 5,
					Broker:    5,
				},
				{
					Partition: 5,
					Broker:    6,
				},
			},
			expectedFollowerThrottles: []PartitionThrottle{
				{
					Partition: 5,
					Broker:    7,
				},
			},
			expectedConfigEntries: []kafka.ConfigEntry{
				{
					ConfigName:  "leader.replication.throttled.replicas",
					ConfigValue: "5:1,5:5,5:6",
				},
				{
					ConfigName:  "follower.replication.throttled.replicas",
					ConfigValue: "5:7",
				},
			},
		},
		{
			description: "leader changes only",
			curr: []PartitionAssignment{
				{
					ID:       3,
					Replicas: []int{1, 2, 3},
				},
			},
			desired: []PartitionAssignment{
				{
					ID:       3,
					Replicas: []int{2, 3, 1},
				},
			},
			expectedLeaderThrottles:   []PartitionThrottle{},
			expectedFollowerThrottles: []PartitionThrottle{},
			expectedConfigEntries:     []kafka.ConfigEntry{},
		},
	}

	for _, testCase := range testCases {
		leaderThrottles := LeaderPartitionThrottles(testCase.curr, testCase.desired)
		assert.Equal(
			t,
			testCase.expectedLeaderThrottles,
			leaderThrottles,
			testCase.description,
		)

		followerThrottles := FollowerPartitionThrottles(testCase.curr, testCase.desired)
		assert.Equal(
			t,
			testCase.expectedFollowerThrottles,
			followerThrottles,
			testCase.description,
		)

		assert.Equal(
			t,
			testCase.expectedConfigEntries,
			PartitionThrottleConfigEntries(leaderThrottles, followerThrottles),
		)
	}
}

func TestBrokerThrottles(t *testing.T) {
	leaderThrottles := []PartitionThrottle{
		{
			Partition: 3,
			Broker:    1,
		},
		{
			Partition: 3,
			Broker:    2,
		},
		{
			Partition: 3,
			Broker:    3,
		},
	}
	followerThrottles := []PartitionThrottle{
		{
			Partition: 3,
			Broker:    1,
		},
		{
			Partition: 5,
			Broker:    2,
		},
		{
			Partition: 5,
			Broker:    7,
		},
	}
	assert.Equal(
		t,
		[]BrokerThrottle{
			{
				Broker:        1,
				ThrottleBytes: 12345,
			},
			{
				Broker:        2,
				ThrottleBytes: 12345,
			},
			{
				Broker:        3,
				ThrottleBytes: 12345,
			},
			{
				Broker:        7,
				ThrottleBytes: 12345,
			},
		},
		BrokerThrottles(
			leaderThrottles,
			followerThrottles,
			12345,
		),
	)
}

func TestBrokerThrottleConfigEntries(t *testing.T) {
	brokerThrottle := BrokerThrottle{
		Broker:        1,
		ThrottleBytes: 12345,
	}
	assert.Equal(
		t,
		[]kafka.ConfigEntry{
			{
				ConfigName:  "leader.replication.throttled.rate",
				ConfigValue: "12345",
			},
			{
				ConfigName:  "follower.replication.throttled.rate",
				ConfigValue: "12345",
			},
		},
		brokerThrottle.ConfigEntries(),
	)
}

func TestParseBrokerThrottles(t *testing.T) {
	brokers := []BrokerInfo{
		{
			ID: 1,
			Config: map[string]string{
				"leader.replication.throttled.rate": "123456",
			},
		},
		{
			ID: 2,
			Config: map[string]string{
				"leader.replication.throttled.rate":   "1234567",
				"follower.replication.throttled.rate": "12345689",
			},
		},
		{
			ID:     3,
			Config: nil,
		},
	}
	leaderThrottles, followerThrottles, err := ParseBrokerThrottles(brokers)
	require.NoError(t, err)
	assert.Equal(
		t,
		[]BrokerThrottle{
			{
				Broker:        1,
				ThrottleBytes: 123456,
			},
			{
				Broker:        2,
				ThrottleBytes: 1234567,
			},
		},
		leaderThrottles,
	)
	assert.Equal(
		t,
		[]BrokerThrottle{
			{
				Broker:        2,
				ThrottleBytes: 12345689,
			},
		},
		followerThrottles,
	)

	badBrokers := []BrokerInfo{
		{
			ID: 1,
			Config: map[string]string{
				"leader.replication.throttled.rate": "not-a-number",
			},
		},
	}
	_, _, err = ParseBrokerThrottles(badBrokers)
	assert.NotNil(t, err)
}

func TestParsePartitionThrottles(t *testing.T) {
	topic := TopicInfo{
		Name: "test-topic",
		Config: map[string]string{
			"leader.replication.throttled.replicas":   "1:2,1:4,5:6",
			"follower.replication.throttled.replicas": "3:4,5:6,7:8",
		},
	}
	leaderThrottles, followerThrottles, err := ParsePartitionThrottles(topic)
	require.NoError(t, err)
	assert.Equal(
		t,
		[]PartitionThrottle{
			{
				Partition: 1,
				Broker:    2,
			},
			{
				Partition: 1,
				Broker:    4,
			},
			{
				Partition: 5,
				Broker:    6,
			},
		},
		leaderThrottles,
	)
	assert.Equal(
		t,
		[]PartitionThrottle{
			{
				Partition: 3,
				Broker:    4,
			},
			{
				Partition: 5,
				Broker:    6,
			},
			{
				Partition: 7,
				Broker:    8,
			},
		},
		followerThrottles,
	)

	badTopic := TopicInfo{
		Name: "bad-topic",
		Config: map[string]string{
			"leader.replication.throttled.replicas": "5:not-a-number",
		},
	}
	_, _, err = ParsePartitionThrottles(badTopic)
	assert.NotNil(t, err)
}
