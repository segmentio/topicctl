package groups

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/messages"
)

type Client struct {
	brokerAddr string
	client     *kafka.Client
}

func NewClient(brokerAddr string) *Client {
	return &Client{
		brokerAddr: brokerAddr,
		client:     kafka.NewClient(brokerAddr),
	}
}

func (c *Client) GetGroups(
	ctx context.Context,
) ([]GroupCoordinator, error) {
	kafkaGroupObjs, err := c.client.ListGroups(ctx)

	// Don't immediately fail if err is non-nil; instead, just process and return
	// whatever results are returned.

	groupCoordinators := []GroupCoordinator{}

	for _, kafkaGroupInfo := range kafkaGroupObjs {
		groupCoordinators = append(
			groupCoordinators,
			GroupCoordinator{
				GroupID:     kafkaGroupInfo.GroupID,
				Coordinator: kafkaGroupInfo.Coordinator,
			},
		)
	}

	sort.Slice(groupCoordinators, func(a, b int) bool {
		return groupCoordinators[a].GroupID < groupCoordinators[b].GroupID
	})

	return groupCoordinators, err
}

func (c *Client) GetGroupDetails(
	ctx context.Context,
	groupID string,
) (*GroupDetails, error) {
	kafkaGroupInfo, err := c.client.DescribeGroup(ctx, groupID)
	if err != nil {
		return nil, err
	}

	groupDetails := GroupDetails{
		GroupID: kafkaGroupInfo.GroupID,
		State:   kafkaGroupInfo.State,
		Members: []MemberInfo{},
	}
	for _, kafkaMember := range kafkaGroupInfo.Members {
		member := MemberInfo{
			MemberID:        kafkaMember.MemberID,
			ClientID:        kafkaMember.ClientID,
			ClientHost:      kafkaMember.ClientHost,
			TopicPartitions: map[string][]int{},
		}

		for _, assignments := range kafkaMember.MemberAssignments.Topics {
			partitions := []int{}

			for _, kafkaPartition := range assignments.Partitions {
				partitions = append(partitions, int(kafkaPartition))
			}

			sort.Slice(partitions, func(a, b int) bool {
				return partitions[a] < partitions[b]
			})

			member.TopicPartitions[assignments.Topic] = partitions
		}

		// Assignments might be missing, use the topic metata to fill in any blanks
		for _, topic := range kafkaMember.MemberMetadata.Topics {
			if _, ok := member.TopicPartitions[topic]; !ok {
				member.TopicPartitions[topic] = []int{}
			}
		}

		groupDetails.Members = append(groupDetails.Members, member)
	}

	return &groupDetails, nil
}

func (c *Client) GetMemberLags(
	ctx context.Context,
	topic string,
	groupID string,
) ([]MemberPartitionLag, error) {
	groupDetails, err := c.GetGroupDetails(ctx, groupID)
	if err != nil {
		return nil, err
	}

	if groupDetails.State == "Dead" {
		return nil, errors.New("Group state is dead; check that group ID is valid")
	}

	topicsMap := groupDetails.TopicsMap()
	if _, ok := topicsMap[topic]; !ok {
		return nil, fmt.Errorf(
			"No assignments found for topic %s in group %s",
			topic,
			groupID,
		)
	}

	partitionMembers := groupDetails.PartitionMembers(topic)

	offsets, err := c.client.ConsumerOffsets(
		ctx, kafka.TopicAndGroup{
			Topic:   topic,
			GroupId: groupID,
		},
	)
	if err != nil {
		return nil, err
	}

	bounds, err := messages.GetAllPartitionBounds(ctx, c.brokerAddr, topic, offsets)
	if err != nil {
		return nil, err
	}

	partitionLags := []MemberPartitionLag{}

	for _, bound := range bounds {
		partitionLag := MemberPartitionLag{
			Topic:        topic,
			Partition:    bound.Partition,
			MemberID:     partitionMembers[bound.Partition].MemberID,
			MemberOffset: offsets[bound.Partition],
			NewestOffset: bound.LastOffset,
			NewestTime:   bound.LastTime,
		}

		if bound.FirstOffset == offsets[bound.Partition] {
			partitionLag.MemberTime = bound.FirstTime
		}

		partitionLags = append(partitionLags, partitionLag)
	}

	return partitionLags, nil
}
