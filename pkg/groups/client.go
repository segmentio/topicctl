package groups

import (
	"context"
	"errors"
	"sort"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/messages"
)

// Client is a struct for getting information about consumer groups from a cluster.
type Client struct {
	brokerAddr string
	client     *kafka.Client
}

// NewClient creates and returns a new Client instance.
func NewClient(brokerAddr string) *Client {
	return &Client{
		brokerAddr: brokerAddr,
		client: &kafka.Client{
			Addr: kafka.TCP(brokerAddr),
		},
	}
}

// GetGroups fetches and returns information about all consumer groups in the cluster.
func (c *Client) GetGroups(
	ctx context.Context,
) ([]GroupCoordinator, error) {
	listGroupsResp, err := c.client.ListGroups(ctx, kafka.ListGroupsRequest{})

	// Don't immediately fail if err is non-nil; instead, just process and return
	// whatever results are returned.

	groupCoordinators := []GroupCoordinator{}

	for _, kafkaGroupInfo := range listGroupsResp.Groups {
		groupCoordinators = append(
			groupCoordinators,
			GroupCoordinator{
				GroupID:     kafkaGroupInfo.GroupID,
				Coordinator: int(kafkaGroupInfo.Coordinator),
			},
		)
	}

	sort.Slice(groupCoordinators, func(a, b int) bool {
		return groupCoordinators[a].GroupID < groupCoordinators[b].GroupID
	})

	return groupCoordinators, err
}

// GetGroupDetails returns the details (membership, etc.) for a single consumer group.
func (c *Client) GetGroupDetails(
	ctx context.Context,
	groupID string,
) (*GroupDetails, error) {
	describeGroupResponse, err := c.client.DescribeGroup(
		ctx,
		kafka.DescribeGroupRequest{GroupID: groupID},
	)
	if err != nil {
		return nil, err
	}

	groupDetails := GroupDetails{
		GroupID: describeGroupResponse.GroupID,
		State:   describeGroupResponse.GroupState,
		Members: []MemberInfo{},
	}
	for _, kafkaMember := range describeGroupResponse.Members {
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

// GetMemberLags returns the lag for each partition being consumed by the argument group in the
// argument topic.
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

// ResetOffsets updates the offsets for a given topic / group combination.
func (c *Client) ResetOffsets(
	ctx context.Context,
	topic string,
	groupID string,
	partitionOffsets map[int]int64,
) error {
	consumerGroup, err := kafka.NewConsumerGroup(
		kafka.ConsumerGroupConfig{
			ID:      groupID,
			Brokers: []string{c.brokerAddr},
			Topics:  []string{topic},
		},
	)
	if err != nil {
		return err
	}

	generation, err := consumerGroup.Next(ctx)
	if err != nil {
		return err
	}

	return generation.CommitOffsets(
		map[string]map[int]int64{
			topic: partitionOffsets,
		},
	)
}
