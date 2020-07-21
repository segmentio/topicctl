package groups

import (
	"sort"
	"time"

	log "github.com/sirupsen/logrus"
)

type GroupCoordinator struct {
	GroupID     string
	Coordinator int
}

type GroupDetails struct {
	GroupID string
	State   string
	Members []MemberInfo
}

func (g GroupDetails) TopicsMap() map[string]struct{} {
	topicsMap := map[string]struct{}{}

	for _, member := range g.Members {
		for _, topic := range member.Topics() {
			topicsMap[topic] = struct{}{}
		}
	}

	return topicsMap
}

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

type MemberInfo struct {
	MemberID        string
	ClientID        string
	ClientHost      string
	TopicPartitions map[string][]int
}

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

type MemberPartitionLag struct {
	Topic        string
	Partition    int
	MemberID     string
	NewestOffset int64
	NewestTime   time.Time
	MemberOffset int64
	MemberTime   time.Time
}

func (m MemberPartitionLag) OffsetLag() int64 {
	return m.NewestOffset - m.MemberOffset
}

func (m MemberPartitionLag) TimeLag() time.Duration {
	return m.NewestTime.Sub(m.MemberTime)
}
