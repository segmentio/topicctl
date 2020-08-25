package admin

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/util"
)

// PartitionThrottle represents a throttle being applied to a single partition,
// broker combination.
type PartitionThrottle struct {
	Partition int
	Broker    int
}

func (p PartitionThrottle) String() string {
	return fmt.Sprintf("%d:%d", p.Partition, p.Broker)
}

func sortThrottles(throttles []PartitionThrottle) {
	sort.Slice(
		throttles, func(a, b int) bool {
			throttle1 := throttles[a]
			throttle2 := throttles[b]

			// Sort by partition, then broker
			return throttle1.Partition < throttle2.Partition ||
				(throttle1.Partition == throttle2.Partition &&
					throttle1.Broker < throttle2.Broker)
		},
	)
}

// PartitionThrottleConfigEntries generates the topic config entries
// for the provided leader and follower throttles.
func PartitionThrottleConfigEntries(
	leaderThrottles []PartitionThrottle,
	followerThrottles []PartitionThrottle,
) []kafka.ConfigEntry {
	leaderValues := []string{}
	for _, throttle := range leaderThrottles {
		leaderValues = append(
			leaderValues,
			throttle.String(),
		)
	}

	followerValues := []string{}
	for _, throttle := range followerThrottles {
		followerValues = append(
			followerValues,
			throttle.String(),
		)
	}

	// Don't set any entries if there are no throttles to apply
	if len(leaderValues) == 0 && len(followerValues) == 0 {
		return []kafka.ConfigEntry{}
	}

	// Set both the leader and follower keys to the same values
	return []kafka.ConfigEntry{
		{
			ConfigName:  LeaderReplicasThrottledKey,
			ConfigValue: strings.Join(leaderValues, ","),
		},
		{
			ConfigName:  FollowerReplicasThrottledKey,
			ConfigValue: strings.Join(followerValues, ","),
		},
	}
}

// BrokerThrottle represents a throttle being applied to a single broker.
type BrokerThrottle struct {
	Broker        int
	ThrottleBytes int64
}

// ConfigEntries returns the kafka config entries associated with this
// broker throttle.
func (b BrokerThrottle) ConfigEntries() []kafka.ConfigEntry {
	// Set both the leader and follower keys to the same values
	return []kafka.ConfigEntry{
		{
			ConfigName:  LeaderThrottledKey,
			ConfigValue: fmt.Sprintf("%d", b.ThrottleBytes),
		},
		{
			ConfigName:  FollowerThrottledKey,
			ConfigValue: fmt.Sprintf("%d", b.ThrottleBytes),
		},
	}
}

// LeaderPartitionThrottles returns a slice of PartitionThrottles that we should apply
// on the leader side.
//
// See https://kafka.apache.org/0101/documentation.html for discussion on how these
// should be applied.
func LeaderPartitionThrottles(
	curr []PartitionAssignment,
	desired []PartitionAssignment,
) []PartitionThrottle {
	throttles := []PartitionThrottle{}

	// Apply to the original replicas only (whether or not they are actual leaders).
	for a, currAssignment := range curr {
		desiredAssignment := desired[a]

		// Don't set any throttles if we're just rearranging replicas
		if util.SameElements(currAssignment.Replicas, desiredAssignment.Replicas) {
			continue
		}

		for _, currReplica := range currAssignment.Replicas {
			throttles = append(
				throttles,
				PartitionThrottle{
					Partition: currAssignment.ID,
					Broker:    currReplica,
				},
			)
		}
	}

	sortThrottles(throttles)
	return throttles
}

// FollowerPartitionThrottles returns a slice of PartitionThrottles that we should apply
// on the follower side.
//
// See https://kafka.apache.org/0101/documentation.html for discussion on how these
// should be applied.
func FollowerPartitionThrottles(
	curr []PartitionAssignment,
	desired []PartitionAssignment,
) []PartitionThrottle {
	throttles := []PartitionThrottle{}

	// Apply to any replicas in desired that are being moved to a new partition.
	for a, desiredAssignment := range desired {
		currAssignment := curr[a]

		// Don't set any throttles if we're just rearranging replicas
		if util.SameElements(currAssignment.Replicas, desiredAssignment.Replicas) {
			continue
		}

		for _, desiredReplica := range desiredAssignment.Replicas {
			if currAssignment.Index(desiredReplica) == -1 {
				throttles = append(
					throttles,
					PartitionThrottle{
						Partition: desiredAssignment.ID,
						Broker:    desiredReplica,
					},
				)
			}
		}
	}

	sortThrottles(throttles)
	return throttles
}

// BrokerThrottles returns a slice of BrokerThrottles that we should apply. It's currently
// just set from the union of the leader and follower brokers (matching the behavior of
// bin/kafka-reassign-partitions.sh).
func BrokerThrottles(
	leaderThrottles []PartitionThrottle,
	followerThrottles []PartitionThrottle,
	throttleBytes int64,
) []BrokerThrottle {
	distinctBrokers := map[int]struct{}{}

	for _, throttle := range leaderThrottles {
		distinctBrokers[throttle.Broker] = struct{}{}
	}
	for _, throttle := range followerThrottles {
		distinctBrokers[throttle.Broker] = struct{}{}
	}

	brokerThrottles := []BrokerThrottle{}

	for broker := range distinctBrokers {
		brokerThrottles = append(
			brokerThrottles,
			BrokerThrottle{
				Broker:        broker,
				ThrottleBytes: throttleBytes,
			},
		)
	}

	sort.Slice(
		brokerThrottles, func(a, b int) bool {
			return brokerThrottles[a].Broker < brokerThrottles[b].Broker
		},
	)
	return brokerThrottles
}

// ParseBrokerThrottles returns slices of the leader and follower throttles for the
// argument brokers.
func ParseBrokerThrottles(brokers []BrokerInfo) (
	[]BrokerThrottle,
	[]BrokerThrottle,
	error,
) {
	leaderThrottles := []BrokerThrottle{}
	followerThrottles := []BrokerThrottle{}

	for _, broker := range brokers {
		leaderRateStr, ok := broker.Config[LeaderThrottledKey]

		if ok {
			leaderRate, err := strconv.ParseInt(leaderRateStr, 10, 64)
			if err != nil {
				return nil, nil, err
			}
			leaderThrottles = append(
				leaderThrottles,
				BrokerThrottle{
					Broker:        broker.ID,
					ThrottleBytes: leaderRate,
				},
			)
		}

		followerRateStr, ok := broker.Config[FollowerThrottledKey]
		if ok {
			followerRate, err := strconv.ParseInt(followerRateStr, 10, 64)
			if err != nil {
				return nil, nil, err
			}
			followerThrottles = append(
				followerThrottles,
				BrokerThrottle{
					Broker:        broker.ID,
					ThrottleBytes: followerRate,
				},
			)
		}

	}

	return leaderThrottles, followerThrottles, nil
}

// ParsePartitionThrottles returns slices of the leader and follower partition
// throttles for the argument topic.
func ParsePartitionThrottles(topic TopicInfo) (
	[]PartitionThrottle,
	[]PartitionThrottle,
	error,
) {
	var leaderThrottles []PartitionThrottle
	var followerThrottles []PartitionThrottle
	var err error

	leaderThrottlesStr, ok := topic.Config[LeaderReplicasThrottledKey]
	if ok {
		leaderThrottles, err = ParsePartitionThrottleStr(leaderThrottlesStr)
		if err != nil {
			return nil, nil, err
		}
	}

	followerThrottlesStr, ok := topic.Config[FollowerReplicasThrottledKey]
	if ok {
		followerThrottles, err = ParsePartitionThrottleStr(followerThrottlesStr)
		if err != nil {
			return nil, nil, err
		}
	}

	return leaderThrottles, followerThrottles, nil
}

// ParsePartitionThrottleStr converts a throttle config string from zk into a slice
// of PartitionThrottle structs.
func ParsePartitionThrottleStr(valuesStr string) ([]PartitionThrottle, error) {
	throttles := []PartitionThrottle{}

	if valuesStr == "" {
		return throttles, nil
	}

	for _, pair := range strings.Split(valuesStr, ",") {
		components := strings.Split(pair, ":")
		if len(components) != 2 {
			return nil, fmt.Errorf("Invalid partition throttle format: %s", pair)
		}

		partitionID, err := strconv.ParseInt(components[0], 10, 32)
		if err != nil {
			return nil, err
		}
		brokerID, err := strconv.ParseInt(components[1], 10, 32)
		if err != nil {
			return nil, err
		}

		throttles = append(
			throttles,
			PartitionThrottle{
				Partition: int(partitionID),
				Broker:    int(brokerID),
			},
		)
	}

	return throttles, nil
}
