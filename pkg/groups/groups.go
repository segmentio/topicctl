package groups

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/messages"
	log "github.com/sirupsen/logrus"
)

// GetGroups fetches and returns information about all consumer groups in the cluster.
func GetGroups(
	ctx context.Context,
	connector *admin.Connector,
) ([]GroupCoordinator, error) {
	listGroupsResp, err := connector.KafkaClient.ListGroups(
		ctx,
		&kafka.ListGroupsRequest{},
	)

	// Don't immediately fail if err is non-nil; instead, just process and return
	// whatever results are returned.
	groupCoordinators := []GroupCoordinator{}

	for _, kafkaGroupInfo := range listGroupsResp.Groups {

		topicsList := []string{}
		topicsMap := map[string]bool{}

		describeGroupsRequest := kafka.DescribeGroupsRequest{
			GroupIDs: []string{kafkaGroupInfo.GroupID},
		}

		describeGroupsResponse, err := connector.KafkaClient.DescribeGroups(ctx, &describeGroupsRequest)
		if err != nil {
			log.Warnf("Cannot list topics for group :%s \n Error in describing group : %s", kafkaGroupInfo.GroupID, err)
		} else {
			if len(describeGroupsResponse.Groups) != 1 {
				log.Warnf("Cannot list topics for group :%s \n Unexpected response length: %d, from describeGroups", kafkaGroupInfo.GroupID, len(describeGroupsResponse.Groups))
			} else {
				groupMembers := describeGroupsResponse.Groups[0].Members
				for _, groupMember := range groupMembers {
					for _, topic := range groupMember.MemberMetadata.Topics {
						topicsMap[topic] = true
					}
				}

				for key := range topicsMap {
					topicsList = append(topicsList, key)
				}
				sort.Strings(topicsList)
			}
		}

		groupCoordinators = append(
			groupCoordinators,
			GroupCoordinator{
				GroupID:     kafkaGroupInfo.GroupID,
				Coordinator: int(kafkaGroupInfo.Coordinator),
				Topics:      topicsList,
			},
		)
	}

	sort.Slice(groupCoordinators, func(a, b int) bool {
		return groupCoordinators[a].GroupID < groupCoordinators[b].GroupID
	})

	return groupCoordinators, err
}

// GetGroupDetails returns the details (membership, etc.) for a single consumer group.
func GetGroupDetails(
	ctx context.Context,
	connector *admin.Connector,
	groupID string,
) (*GroupDetails, error) {
	req := kafka.DescribeGroupsRequest{
		GroupIDs: []string{groupID},
	}
	log.Debugf("DescribeGroups request: %+v", req)

	describeGroupsResponse, err := connector.KafkaClient.DescribeGroups(ctx, &req)
	if err != nil {
		return nil, err
	}
	log.Debugf("DescribeGroups response: %+v", describeGroupsResponse)

	if len(describeGroupsResponse.Groups) != 1 {
		return nil, fmt.Errorf("Unexpected response length from describeGroups")
	}
	group := describeGroupsResponse.Groups[0]

	groupDetails := GroupDetails{
		GroupID: group.GroupID,
		State:   group.GroupState,
		Members: []MemberInfo{},
	}
	for _, kafkaMember := range group.Members {
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
func GetMemberLags(
	ctx context.Context,
	connector *admin.Connector,
	topic string,
	groupID string,
) ([]MemberPartitionLag, error) {
	groupDetails, err := GetGroupDetails(ctx, connector, groupID)
	if err != nil {
		return nil, err
	}

	if groupDetails.State == "Dead" {
		return nil, errors.New("Group state is dead; check that group ID is valid")
	}

	partitionMembers := groupDetails.PartitionMembers(topic)

	offsets, err := connector.KafkaClient.ConsumerOffsets(
		ctx, kafka.TopicAndGroup{
			Topic:   topic,
			GroupId: groupID,
		},
	)
	log.Debugf("Received consumerOffsets: %+v", offsets)

	if err != nil {
		return nil, err
	}

	bounds, err := messages.GetAllPartitionBounds(ctx, connector, topic, offsets)
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
func ResetOffsets(
	ctx context.Context,
	connector *admin.Connector,
	topic string,
	groupID string,
	partitionOffsets map[int]int64,
) error {
	consumerGroup, err := kafka.NewConsumerGroup(
		kafka.ConsumerGroupConfig{
			ID:      groupID,
			Brokers: []string{connector.Config.BrokerAddr},
			Topics:  []string{topic},
			Dialer:  connector.Dialer,
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

// GetEarliestorLatestOffset gets earliest/latest offset for a given topic partition for resetting offsets of consumer group
func GetEarliestOrLatestOffset(
	ctx context.Context,
	connector *admin.Connector,
	topic string,
	strategy string,
	partition int,
) (int64, error) {
	if strategy == EarliestResetOffsetsStrategy {
		partitionBound, err := messages.GetPartitionBounds(ctx, connector, topic, partition, 0)
		if err != nil {
			return 0, err
		}
		return partitionBound.FirstOffset, nil
	} else if strategy == LatestResetOffsetsStrategy {
		partitionBound, err := messages.GetPartitionBounds(ctx, connector, topic, partition, 0)
		if err != nil {
			return 0, err
		}
		return partitionBound.LastOffset, nil
	}
	return 0, errors.New("Invalid reset offset strategy provided.")
}
