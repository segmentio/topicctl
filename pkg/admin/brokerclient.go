package admin

import (
	"context"
	"errors"
	"fmt"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/zk"
)

// BrokerAdminClient is a Client implementation that only uses broker APIs, without any
// zookeeper access.
type BrokerAdminClient struct {
	brokerAddr string
	client     *kafka.Client
	readOnly   bool
}

var _ Client = (*BrokerAdminClient)(nil)

func NewBrokerAdminClient(brokerAddr string, readOnly bool) *BrokerAdminClient {
	return &BrokerAdminClient{
		brokerAddr: brokerAddr,
		client: &kafka.Client{
			Addr: kafka.TCP(brokerAddr),
		},
		readOnly: readOnly,
	}
}

func (c *BrokerAdminClient) GetClusterID(ctx context.Context) (string, error) {
	resp, err := c.client.Metadata(ctx, &kafka.MetadataRequest{})
	if err != nil {
		return "", err
	}
	return resp.ClusterID, nil
}

func (c *BrokerAdminClient) GetBrokers(ctx context.Context, ids []int) (
	[]BrokerInfo,
	error,
) {
	resp, err := c.client.Metadata(ctx, &kafka.MetadataRequest{})
	if err != nil {
		return nil, err
	}

	brokerInfos := []BrokerInfo{}
	idsMap := map[int]struct{}{}
	for _, id := range ids {
		idsMap[id] = struct{}{}
	}

	for _, broker := range resp.Brokers {
		if _, ok := idsMap[broker.ID]; !ok && len(idsMap) > 0 {
			continue
		}

		brokerInfos = append(
			brokerInfos,
			BrokerInfo{
				ID:   broker.ID,
				Host: broker.Host,
				Port: int32(broker.Port),
				Rack: broker.Rack,
			},
		)
	}

	return brokerInfos, nil
}

func (c *BrokerAdminClient) GetBrokerIDs(ctx context.Context) ([]int, error) {
	resp, err := c.client.Metadata(ctx, &kafka.MetadataRequest{})
	if err != nil {
		return nil, err
	}
	brokerIDs := []int{}
	for _, broker := range resp.Brokers {
		brokerIDs = append(brokerIDs, broker.ID)
	}
	return brokerIDs, nil
}

func (c *BrokerAdminClient) GetBootstrapAddrs() []string {
	return []string{c.brokerAddr}
}

func (c *BrokerAdminClient) GetTopics(
	ctx context.Context,
	names []string,
	detailed bool,
) ([]TopicInfo, error) {
	resp, err := c.client.Metadata(ctx, &kafka.MetadataRequest{})
	if err != nil {
		return nil, err
	}

	topicInfos := []TopicInfo{}

	for _, topic := range resp.Topics {
		partitionInfos := []PartitionInfo{}

		for _, partition := range topic.Partitions {
			partitionInfos = append(
				partitionInfos,
				PartitionInfo{
					Topic:    topic.Name,
					ID:       partition.ID,
					Leader:   partition.Leader.ID,
					Replicas: brokerIDs(partition.Replicas),
					ISR:      brokerIDs(partition.Isr),
				},
			)
		}

		topicInfos = append(
			topicInfos,
			TopicInfo{
				Name:       topic.Name,
				Partitions: partitionInfos,
			},
		)
	}

	return topicInfos, nil
}

func (c *BrokerAdminClient) GetTopicNames(ctx context.Context) ([]string, error) {
	resp, err := c.client.Metadata(ctx, &kafka.MetadataRequest{})
	if err != nil {
		return nil, err
	}

	topicNames := []string{}
	for _, topic := range resp.Topics {
		topicNames = append(topicNames, topic.Name)
	}
	return topicNames, nil
}

func (c *BrokerAdminClient) GetTopic(
	ctx context.Context,
	name string,
	detailed bool,
) (TopicInfo, error) {
	resp, err := c.client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{name}})
	if err != nil {
		return TopicInfo{}, err
	}

	if len(resp.Topics) != 1 {
		return TopicInfo{},
			fmt.Errorf("Unexpected topic length response: %d", len(resp.Topics))
	}

	topic := resp.Topics[0]

	partitionInfos := []PartitionInfo{}

	for _, partition := range topic.Partitions {
		partitionInfos = append(
			partitionInfos,
			PartitionInfo{
				Topic:    topic.Name,
				ID:       partition.ID,
				Leader:   partition.Leader.ID,
				Replicas: brokerIDs(partition.Replicas),
				ISR:      brokerIDs(partition.Isr),
			},
		)
	}

	return TopicInfo{
		Name:       topic.Name,
		Partitions: partitionInfos,
	}, nil
}

func (c *BrokerAdminClient) GetBrokerPartitions(ctx context.Context, names []string) (
	[]PartitionInfo,
	error,
) {
	return nil, nil
}

func (c *BrokerAdminClient) UpdateTopicConfig(
	ctx context.Context,
	name string,
	configEntries []kafka.ConfigEntry,
	overwrite bool,
) ([]string, error) {
	return nil, nil
}

func (c *BrokerAdminClient) UpdateBrokerConfig(
	ctx context.Context,
	id int,
	configEntries []kafka.ConfigEntry,
	overwrite bool,
) ([]string, error) {
	return nil, nil
}

func (c *BrokerAdminClient) CreateTopic(
	ctx context.Context,
	config kafka.TopicConfig,
) error {
	if c.readOnly {
		return errors.New("Cannot create topic in read-only mode")
	}

	_, err := c.client.CreateTopics(
		ctx,
		&kafka.CreateTopicsRequest{
			Topics: []kafka.TopicConfig{config},
		},
	)
	return err
}

func (c *BrokerAdminClient) AssignPartitions(
	ctx context.Context,
	topic string,
	assignments []PartitionAssignment,
) error {
	return nil
}

func (c *BrokerAdminClient) AddPartitions(
	ctx context.Context,
	topic string,
	newAssignments []PartitionAssignment,
) error {
	return nil
}

func (c *BrokerAdminClient) RunLeaderElection(
	ctx context.Context,
	topic string,
	partitions []int,
) error {
	return nil
}

func (c *BrokerAdminClient) AcquireLock(ctx context.Context, path string) (
	zk.Lock,
	error,
) {
	return nil, nil
}

func (c *BrokerAdminClient) LockHeld(ctx context.Context, path string) (bool, error) {
	return false, nil
}

func (c *BrokerAdminClient) Close() error {
	return nil
}

func brokerIDs(brokers []kafka.Broker) []int {
	ids := []int{}
	for _, broker := range brokers {
		ids = append(ids, broker.ID)
	}
	return ids
}
