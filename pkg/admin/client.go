package admin

import (
	"context"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/zk"
)

// Client is an interface for
type Client interface {
	GetClusterID(ctx context.Context) (string, error)
	GetBrokers(ctx context.Context, ids []int) ([]BrokerInfo, error)
	GetBrokerIDs(ctx context.Context) ([]int, error)
	GetBootstrapAddrs() []string
	GetTopics(
		ctx context.Context,
		names []string,
		detailed bool,
	) ([]TopicInfo, error)
	GetTopicNames(ctx context.Context) ([]string, error)
	GetTopic(
		ctx context.Context,
		name string,
		detailed bool,
	) (TopicInfo, error)
	GetBrokerPartitions(ctx context.Context, names []string) ([]PartitionInfo, error)
	UpdateTopicConfig(
		ctx context.Context,
		name string,
		configEntries []kafka.ConfigEntry,
		overwrite bool,
	) ([]string, error)
	UpdateBrokerConfig(
		ctx context.Context,
		id int,
		configEntries []kafka.ConfigEntry,
		overwrite bool,
	) ([]string, error)
	GetControllerAddr(ctx context.Context) (string, error)
	CreateTopic(
		ctx context.Context,
		config kafka.TopicConfig,
	) error
	AssignPartitions(
		ctx context.Context,
		topic string,
		assignments []PartitionAssignment,
	) error
	AddPartitions(
		ctx context.Context,
		topic string,
		newAssignments []PartitionAssignment,
	) error
	RunLeaderElection(
		ctx context.Context,
		topic string,
		partitions []int,
	) error
	AcquireLock(ctx context.Context, path string) (zk.Lock, error)
	LockHeld(ctx context.Context, path string) (bool, error)
	Close() error
}
