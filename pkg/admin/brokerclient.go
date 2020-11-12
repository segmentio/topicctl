package admin

import (
	"context"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/zk"
)

// BrokerAdminClient is a Client implementation that only uses broker APIs, without any
// zookeeper access.
type BrokerAdminClient struct {
}

var _ Client = (*BrokerAdminClient)(nil)

func (c *BrokerAdminClient) GetClusterID(ctx context.Context) (string, error) {
	return "", nil
}

func (c *BrokerAdminClient) GetBrokers(ctx context.Context, ids []int) (
	[]BrokerInfo,
	error,
) {
	return nil, nil
}

func (c *BrokerAdminClient) GetBrokerIDs(ctx context.Context) ([]int, error) {
	return nil, nil
}

func (c *BrokerAdminClient) GetBootstrapAddrs() []string {
	return nil
}

func (c *BrokerAdminClient) GetTopics(
	ctx context.Context,
	names []string,
	detailed bool,
) ([]TopicInfo, error) {
	return nil, nil
}

func (c *BrokerAdminClient) GetTopicNames(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (c *BrokerAdminClient) GetTopic(
	ctx context.Context,
	name string,
	detailed bool,
) (TopicInfo, error) {
	return TopicInfo{}, nil
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

func (c *BrokerAdminClient) GetControllerAddr(ctx context.Context) (string, error) {
	return "", nil
}

func (c *BrokerAdminClient) CreateTopic(
	ctx context.Context,
	config kafka.TopicConfig,
) error {
	return nil
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
