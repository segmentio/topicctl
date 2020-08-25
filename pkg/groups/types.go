package groups

import (
	"sort"
	"time"

	log "github.com/sirupsen/logrus"
)

// GroupCoordinator stores the coordinator broker for a single consumer group.
type GroupCoordinator struct {
	GroupID     string
	Coordinator int
}

// GroupDetails stores the state and members for a consumer group.
type GroupDetails struct {
	GroupID string
	State   string
	Members []MemberInfo
}

// TopicsMap returns a map of all the topics consumed by the current group.
func (g GroupDetails) TopicsMap() map[string]struct{} {
	topicsMap := map[string]struct{}{}

	for _, member := range g.Members {
		for _, topic := range member.Topics() {
			topicsMap[topic] = struct{}{}
		}
	}

	return topicsMap
}

// PartitionMembers returns the members for each partition in the argument topic.
func (g GroupDetails) PartitionMembers(topic string) map[int]MemberInfo {
	partitionsMap := map[int]MemberInfo{}

	for _, member := range g.Members {
		partitions := member.TopicPartitions[topic]
		if len(partitions) > 0 {
			for _, partition := range partitions {
				if _, ok := partitionsMap[partition]; ok {
					log.Warnf("Multiple members assigned to partition %d", partition)
				}

				partitionsMap[partition] = member
			}
		}
	}

	return partitionsMap
}

// MemberInfo stores information about a single consumer group member.
type MemberInfo struct {
	MemberID        string
	ClientID        string
	ClientHost      string
	TopicPartitions map[string][]int
}

// Topics returns a slice of all topics that the current MemberInfo is consuming from.
func (m MemberInfo) Topics() []string {
	topics := []string{}

	for topic := range m.TopicPartitions {
		topics = append(topics, topic)
	}

	sort.Slice(topics, func(a, b int) bool {
		return topics[a] < topics[b]
	})

	return topics
}

// MemberPartitionLag information about the lag for a single topic / partition / group member
// combination.
type MemberPartitionLag struct {
	Topic        string
	Partition    int
	MemberID     string
	NewestOffset int64
	NewestTime   time.Time
	MemberOffset int64
	MemberTime   time.Time
}

// OffsetLag returns the difference between the latest offset in the partition and the latest one
// committed by the group member.
func (m MemberPartitionLag) OffsetLag() int64 {
	return m.NewestOffset - m.MemberOffset
}

// TimeLag returns the time difference between the latest timestamp in the the partition and the
// timestamp in the latest message committed by the group member.
func (m MemberPartitionLag) TimeLag() time.Duration {
	return m.NewestTime.Sub(m.MemberTime)
}
