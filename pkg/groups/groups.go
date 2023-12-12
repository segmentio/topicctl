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
func GetGroupDetails(ctx context.Context, connector *admin.Connector, groupID string) (*GroupDetails, error) {
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

// GetMemberLagsInput configures a call to [GetMemberLags].
type GetMemberLagsInput struct {
	GroupID string
	Topic   string

	// FullRange will make fetched partition ranges accurate
	// from the partition's perspective, ignoring consumer group:
	// this has the downside of potentially making MemberTime inaccurate.
	FullRange bool
}

// GetMemberLags returns the lag for each partition on the given topic,
// being consumed by the given group in the argument topic.
func GetMemberLags(ctx context.Context, connector *admin.Connector, input *GetMemberLagsInput) ([]MemberPartitionLag, error) {
	groupDetails, err := GetGroupDetails(ctx, connector, input.GroupID)
	if err != nil {
		return nil, err
	}

	if groupDetails.State == "Dead" {
		return nil, errors.New("Group state is dead; check that group ID is valid")
	}

	partitionMembers := groupDetails.PartitionMembers(input.Topic)

	offsetInput := kafka.TopicAndGroup{
		GroupId: input.GroupID,
		Topic:   input.Topic,
	}

	offsets, err := connector.KafkaClient.ConsumerOffsets(ctx, offsetInput)
	if err != nil {
		return nil, err
	}

	boundsOffsetsInput := offsets
	if input.FullRange {
		boundsOffsetsInput = nil
	}

	bounds, err := messages.GetAllPartitionBounds(ctx, connector, input.Topic, boundsOffsetsInput)
	if err != nil {
		return nil, err
	}

	partitionLags := make([]MemberPartitionLag, len(bounds))

	for i, bound := range bounds {
		lag := &partitionLags[i]
		*lag = MemberPartitionLag{
			Topic:        input.Topic,
			Partition:    bound.Partition,
			MemberID:     partitionMembers[bound.Partition].MemberID,
			OldestOffset: bound.FirstOffset,
			NewestOffset: bound.LastOffset,
			MemberOffset: offsets[bound.Partition],
			OldestTime:   bound.FirstTime,
			NewestTime:   bound.LastTime,
		}

		switch lag.MemberOffset {
		case bound.LastOffset:
			lag.MemberTime = bound.LastTime
		case bound.FirstOffset:
			lag.MemberTime = bound.FirstTime
		}
	}

	return partitionLags, nil
}

// DeleteOffsetsInput configures a call to [DeleteOffsets].
type DeleteOffsetsInput struct {
	GroupID    string
	Topic      string
	Partitions []int
}

// DeleteOffsets removes a consumer group's offsets
// on the given topic-partition combinations.
func DeleteOffsets(ctx context.Context, connector *admin.Connector, input *DeleteOffsetsInput) error {
	req := kafka.OffsetDeleteRequest{
		Addr:    connector.KafkaClient.Addr,
		GroupID: input.GroupID,
		Topics:  map[string][]int{input.Topic: input.Partitions},
	}

	resp, err := connector.KafkaClient.OffsetDelete(ctx, &req)
	if err != nil {
		return err
	}

	var errs []error

	for _, results := range resp.Topics {
		for _, result := range results {
			if result.Error != nil {
				errs = append(errs, result.Error)
			}
		}
	}

	return errors.Join(errs...)
}

// ResetOffsetsInput configures a call to [ResetOffsets].
type ResetOffsetsInput struct {
	GroupID          string
	Topic            string
	PartitionOffsets map[int]int64
}

// ResetOffsets updates the offsets for a given topic / group combination.
func ResetOffsets(ctx context.Context, connector *admin.Connector, input *ResetOffsetsInput) error {
	cfg := kafka.ConsumerGroupConfig{
		ID:      input.GroupID,
		Brokers: []string{connector.Config.BrokerAddr},
		Topics:  []string{input.Topic},
		Dialer:  connector.Dialer,
	}

	consumerGroup, err := kafka.NewConsumerGroup(cfg)
	if err != nil {
		return err
	}

	generation, err := consumerGroup.Next(ctx)
	if err != nil {
		return err
	}

	offsets := map[string]map[int]int64{
		input.Topic: input.PartitionOffsets,
	}

	return generation.CommitOffsets(offsets)
}

// GetEarliestOrLatestOffsetInput configures a call to [GetEarliestOrLatestOffset].
type GetEarliestOrLatestOffsetInput struct {
	Strategy  string
	Topic     string
	Partition int
}

// GetEarliestorLatestOffset gets earliest/latest offset
// for a given topic partition for resetting offsets of consumer group.
func GetEarliestOrLatestOffset(ctx context.Context, connector *admin.Connector, input *GetEarliestOrLatestOffsetInput) (int64, error) {
	if !isValidOffsetStrategy(input.Strategy) {
		return 0, errors.New("Invalid reset offset strategy provided.")
	}

	partitionBound, err := messages.GetPartitionBounds(ctx, connector, input.Topic, input.Partition, 0)
	if err != nil {
		return 0, err
	}

	switch input.Strategy {
	case EarliestResetOffsetsStrategy:
		return partitionBound.FirstOffset, nil
	case LatestResetOffsetsStrategy:
		return partitionBound.LastOffset, nil
	}

	panic("impossible")
}

func isValidOffsetStrategy(strategy string) bool {
	return strategy == EarliestResetOffsetsStrategy ||
		strategy == LatestResetOffsetsStrategy
}
