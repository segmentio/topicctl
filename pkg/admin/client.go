package admin

import (
	"context"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/zk"
)

// Client is an interface for interacting with a cluster for administrative tasks.
type Client interface {
	// GetClusterID gets the ID of the cluster.
	GetClusterID(ctx context.Context) (string, error)

	// GetBrokers gets information about all brokers in the cluster.
	GetBrokers(ctx context.Context, ids []int) ([]BrokerInfo, error)

	// GetBrokerIDs get the IDs of all brokers in the cluster.
	GetBrokerIDs(ctx context.Context) ([]int, error)

	// GetConnector gets the Connector instance for this cluster.
	GetConnector() *Connector

	// GetTopics gets full information about each topic in the cluster.
	GetTopics(
		ctx context.Context,
		names []string,
		detailed bool,
	) ([]TopicInfo, error)

	// GetTopicNames gets just the names of each topic in the cluster.
	GetTopicNames(ctx context.Context) ([]string, error)

	// GetTopic gets the details of a single topic in the cluster.
	GetTopic(
		ctx context.Context,
		name string,
		detailed bool,
	) (TopicInfo, error)

	// UpdateTopicConfig updates the configuration for the argument topic. It returns the config
	// keys that were updated.
	UpdateTopicConfig(
		ctx context.Context,
		name string,
		configEntries []kafka.ConfigEntry,
		overwrite bool,
	) ([]string, error)

	// UpdateBrokerConfig updates the configuration for the argument broker. It returns the config
	// keys that were updated.
	UpdateBrokerConfig(
		ctx context.Context,
		id int,
		configEntries []kafka.ConfigEntry,
		overwrite bool,
	) ([]string, error)

	// CreateTopic creates a topic in the cluster.
	CreateTopic(
		ctx context.Context,
		config kafka.TopicConfig,
	) error

	// AssignPartitions sets the replica broker IDs for one or more partitions in a topic.
	AssignPartitions(
		ctx context.Context,
		topic string,
		assignments []PartitionAssignment,
	) error

	// AddPartitions extends a topic by adding one or more new partitions to it.
	AddPartitions(
		ctx context.Context,
		topic string,
		newAssignments []PartitionAssignment,
	) error

	// RunLeaderElection triggers a leader election for one or more partitions in a topic.
	RunLeaderElection(
		ctx context.Context,
		topic string,
		partitions []int,
	) error

	// AcquireLock acquires a lock that can be used to prevent simultaneous changes to a topic.
	AcquireLock(ctx context.Context, path string) (zk.Lock, error)

	// LockHeld returns whether a lock is currently held for the given path.
	LockHeld(ctx context.Context, path string) (bool, error)

	// GetSupportedFeatures gets the features supported by the cluster for this client.
	GetSupportedFeatures() SupportedFeatures

	// Close closes the client.
	Close() error
}
