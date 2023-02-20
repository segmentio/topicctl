package admin

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/efcloud/topicctl/pkg/util"
	"github.com/efcloud/topicctl/pkg/zk"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

const (
	defaultTimeout = 30 * time.Second

	// Used for filtering out default configs
	configSourceUnknown                    int8 = 0
	configSourceTopicConfig                int8 = 1
	configSourceDynamicBrokerConfig        int8 = 2
	configSourceDynamicDefaultBrokerConfig int8 = 3
	configSourceStaticBrokerConfig         int8 = 4
	configSourceDefaultConfig              int8 = 5
	configSourceDynamicBrokerLoggerConfig  int8 = 6

	sensitivePlaceholder = "SENSITIVE"
)

// BrokerAdminClient is a Client implementation that only uses broker APIs, without any
// zookeeper access.
type BrokerAdminClient struct {
	client            *kafka.Client
	connector         *Connector
	config            BrokerAdminClientConfig
	supportedFeatures SupportedFeatures
}

var _ Client = (*BrokerAdminClient)(nil)

// BrokerAdminClientConfig contains the configuration settings to construct a BrokerAdminClient
// instance.
type BrokerAdminClientConfig struct {
	ConnectorConfig
	ReadOnly          bool
	ExpectedClusterID string
}

// NewBrokerAdminClient constructs a new BrokerAdminClient instance.
func NewBrokerAdminClient(
	ctx context.Context,
	config BrokerAdminClientConfig,
) (*BrokerAdminClient, error) {
	connector, err := NewConnector(config.ConnectorConfig)
	if err != nil {
		return nil, err
	}
	client := connector.KafkaClient

	log.Debugf("Getting supported API versions")
	apiVersions, err := client.ApiVersions(ctx, &kafka.ApiVersionsRequest{})
	if err != nil {
		return nil, err
	}
	log.Debugf("Supported API versions: %+v", apiVersions)
	maxVersions := map[string]int{}
	for _, apiKey := range apiVersions.ApiKeys {
		maxVersions[apiKey.ApiName] = apiKey.MaxVersion
	}

	supportedFeatures := SupportedFeatures{
		// Broker-based client does not support locking yet
		Locks: false,
	}

	// If we have DescribeConfigs support, then we're good for reading (other needed APIs are
	// older).
	if _, ok := maxVersions["DescribeConfigs"]; ok {
		supportedFeatures.Reads = true
	} else {
		// Don't let users create client without basic read functionality.
		return nil, errors.New(
			"Kafka version too limited to support basic broker admin functionality; please use zk-based client.",
		)
	}

	// If we have ListPartitionReassignments support, then we're good for applying (other needed
	// APIs are older). This should be satisfied by versions >= 2.4.

	if _, ok := maxVersions["ListPartitionReassignments"]; ok {
		supportedFeatures.Applies = true
	}

	// If we have AlterClientQuotas support, then we're running a newer version of Kafka (>= 2.6),
	// that will provide the correct values for dynamic broker configs.
	if _, ok := maxVersions["AlterClientQuotas"]; ok {
		supportedFeatures.DynamicBrokerConfigs = true
	}
	log.Debugf("Supported features: %+v", supportedFeatures)

	adminClient := &BrokerAdminClient{
		client:            client,
		connector:         connector,
		config:            config,
		supportedFeatures: supportedFeatures,
	}

	if config.ExpectedClusterID != "" {
		log.Info("Checking cluster ID against version in cluster")
		clusterID, err := adminClient.GetClusterID(ctx)
		if err != nil {
			return nil, err
		}
		if clusterID != config.ExpectedClusterID {
			return nil, fmt.Errorf(
				"ID in cluster (%s) does not match expected one (%s)",
				clusterID,
				config.ExpectedClusterID,
			)
		}
	}

	return adminClient, nil
}

// GetClusterID gets the ID of the cluster.
func (c *BrokerAdminClient) GetClusterID(ctx context.Context) (string, error) {
	resp, err := c.getMetadata(ctx, nil)
	if err != nil {
		return "", err
	}
	return resp.ClusterID, nil
}

// GetBrokers gets information about all brokers in the cluster.
func (c *BrokerAdminClient) GetBrokers(ctx context.Context, ids []int) (
	[]BrokerInfo,
	error,
) {
	metadataResp, err := c.getMetadata(ctx, nil)
	if err != nil {
		return nil, err
	}

	brokerInfos := []BrokerInfo{}
	idsMap := map[int]struct{}{}
	for _, id := range ids {
		idsMap[id] = struct{}{}
	}

	configRequestResources := []kafka.DescribeConfigRequestResource{}
	brokerIDIndices := map[int]int{}

	for _, broker := range metadataResp.Brokers {
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
		brokerIDIndices[broker.ID] = len(brokerInfos) - 1

		configRequestResources = append(
			configRequestResources,
			kafka.DescribeConfigRequestResource{
				ResourceType: kafka.ResourceTypeBroker,
				ResourceName: fmt.Sprintf("%d", broker.ID),
			},
		)
	}

	configsReq := kafka.DescribeConfigsRequest{
		Resources: configRequestResources,
	}
	log.Debugf("DescribeConfigs request: %+v", configsReq)

	configsResp, err := c.client.DescribeConfigs(ctx, &configsReq)
	log.Debugf("DescribeConfigs response: %+v (%+v)", configsResp, err)
	if err != nil {
		return nil, err
	}

	for _, resource := range configsResp.Resources {
		brokerID, err := strconv.Atoi(resource.ResourceName)
		if err != nil {
			return nil, err
		}

		config := map[string]string{}
		for _, configEntry := range resource.ConfigEntries {
			if configEntry.IsDefault ||
				configEntry.ConfigSource == configSourceDefaultConfig ||
				configEntry.ConfigSource == configSourceStaticBrokerConfig ||
				configEntry.ConfigSource == configSourceDynamicDefaultBrokerConfig {
				// Skip over defaults
				continue
			}

			if configEntry.ConfigValue == "" && configEntry.IsSensitive {
				config[configEntry.ConfigName] = sensitivePlaceholder
			} else {
				config[configEntry.ConfigName] = configEntry.ConfigValue
			}
		}

		index := brokerIDIndices[brokerID]
		brokerInfos[index].Config = config
	}

	return brokerInfos, nil
}

// GetBrokerIDs get the IDs of all brokers in the cluster.
func (c *BrokerAdminClient) GetBrokerIDs(ctx context.Context) ([]int, error) {
	resp, err := c.getMetadata(ctx, nil)
	if err != nil {
		return nil, err
	}

	brokerIDs := []int{}
	for _, broker := range resp.Brokers {
		brokerIDs = append(brokerIDs, broker.ID)
	}
	return brokerIDs, nil
}

// GetConnector gets the Connector instance for this cluster.
func (c *BrokerAdminClient) GetConnector() *Connector {
	return c.connector
}

// GetTopics gets full information about each topic in the cluster.
func (c *BrokerAdminClient) GetTopics(
	ctx context.Context,
	names []string,
	detailed bool,
) ([]TopicInfo, error) {
	var topicNames []string
	if len(names) > 0 {
		topicNames = names
	}

	metadataResp, err := c.getMetadata(ctx, topicNames)
	if err != nil {
		return nil, err
	}

	topicInfos := []TopicInfo{}
	configRequestResources := []kafka.DescribeConfigRequestResource{}
	topicNameToIndex := map[string]int{}

	for t, topic := range metadataResp.Topics {
		if topic.Error != nil {
			if strings.Contains(topic.Error.Error(), "does not exist") {
				log.Debugf("Skipping over topic %s because it does not exist", topic.Name)
				continue
			}
			// Some other error
			return nil,
				fmt.Errorf("Error getting metadata for topic %s: %+v", topic.Name, topic.Error)
		}

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
		topicNameToIndex[topic.Name] = t

		configRequestResources = append(
			configRequestResources,
			kafka.DescribeConfigRequestResource{
				ResourceType: kafka.ResourceTypeTopic,
				ResourceName: topic.Name,
			},
		)

	}

	configsReq := kafka.DescribeConfigsRequest{
		Resources: configRequestResources,
	}
	log.Debugf("DescribeConfigs request: %+v", configsReq)

	configsResp, err := c.client.DescribeConfigs(ctx, &configsReq)
	log.Debugf("DescribeConfigs response: %+v (%+v)", configsResp, err)
	if err != nil {
		return nil, err
	}

	for _, resource := range configsResp.Resources {
		config := map[string]string{}
		for _, configEntry := range resource.ConfigEntries {
			if configEntry.IsDefault ||
				configEntry.ConfigSource == configSourceDefaultConfig ||
				configEntry.ConfigSource == configSourceStaticBrokerConfig {
				// Skip over defaults
				continue
			}

			if configEntry.ConfigValue == "" && configEntry.IsSensitive {
				config[configEntry.ConfigName] = sensitivePlaceholder
			} else {
				config[configEntry.ConfigName] = configEntry.ConfigValue
			}
		}

		index := topicNameToIndex[resource.ResourceName]
		topicInfos[index].Config = config
	}

	return topicInfos, nil
}

// GetTopicNames gets just the names of each topic in the cluster.
func (c *BrokerAdminClient) GetTopicNames(ctx context.Context) ([]string, error) {
	topicInfos, err := c.GetTopics(ctx, nil, false)
	if err != nil {
		return nil, err
	}

	topicNames := []string{}
	for _, topicInfo := range topicInfos {
		topicNames = append(topicNames, topicInfo.Name)
	}
	return topicNames, nil
}

// GetTopic gets the details of a single topic in the cluster.
func (c *BrokerAdminClient) GetTopic(
	ctx context.Context,
	name string,
	detailed bool,
) (TopicInfo, error) {
	topicInfos, err := c.GetTopics(
		ctx,
		[]string{name},
		detailed,
	)
	if err != nil {
		return TopicInfo{}, err

	}
	if len(topicInfos) == 0 {
		return TopicInfo{}, ErrTopicDoesNotExist
	}
	return topicInfos[0], nil
}

// UpdateTopicConfig updates the configuration for the argument topic. It returns the config
// keys that were updated.
func (c *BrokerAdminClient) UpdateTopicConfig(
	ctx context.Context,
	name string,
	configEntries []kafka.ConfigEntry,
	overwrite bool,
) ([]string, error) {
	if c.config.ReadOnly {
		return nil, errors.New("Cannot update topic config read-only mode")
	}

	req := kafka.IncrementalAlterConfigsRequest{
		Resources: []kafka.IncrementalAlterConfigsRequestResource{
			{
				ResourceType: kafka.ResourceTypeTopic,
				ResourceName: name,
				Configs:      configEntriesToAPIConfigs(configEntries),
			},
		},
	}
	log.Debugf("IncrementalAlterConfigs request: %+v", req)

	// TODO: Handle case where overwrite is false.
	resp, err := c.client.IncrementalAlterConfigs(ctx, &req)
	log.Debugf("IncrementalAlterConfigs response: %+v (%+v)", resp, err)
	if err != nil {
		return nil, err
	}
	if err = util.IncrementalAlterConfigsResponseResourcesError(resp.Resources); err != nil {
		return nil, err
	}

	updated := []string{}
	for _, entry := range configEntries {
		updated = append(updated, entry.ConfigName)
	}

	return updated, nil
}

// UpdateBrokerConfig updates the configuration for the argument broker.  It returns the config
// keys that were updated.
func (c *BrokerAdminClient) UpdateBrokerConfig(
	ctx context.Context,
	id int,
	configEntries []kafka.ConfigEntry,
	overwrite bool,
) ([]string, error) {
	if c.config.ReadOnly {
		return nil, errors.New("Cannot update broker config read-only mode")
	}

	req := kafka.IncrementalAlterConfigsRequest{
		Resources: []kafka.IncrementalAlterConfigsRequestResource{
			{
				ResourceType: kafka.ResourceTypeBroker,
				ResourceName: fmt.Sprintf("%d", id),
				Configs:      configEntriesToAPIConfigs(configEntries),
			},
		},
	}
	log.Debugf("IncrementalAlterConfigs request: %+v", req)

	// TODO: Handle case where overwrite is false.
	resp, err := c.client.IncrementalAlterConfigs(ctx, &req)
	log.Debugf("IncrementalAlterConfigs response: %+v (%+v)", resp, err)
	if err != nil {
		return nil, err
	}
	if err = util.IncrementalAlterConfigsResponseResourcesError(resp.Resources); err != nil {
		return nil, err
	}

	updated := []string{}
	for _, entry := range configEntries {
		updated = append(updated, entry.ConfigName)
	}

	return updated, nil
}

// CreateTopic creates a topic in the cluster.
func (c *BrokerAdminClient) CreateTopic(
	ctx context.Context,
	config kafka.TopicConfig,
) error {
	if c.config.ReadOnly {
		return errors.New("Cannot create topic in read-only mode")
	}

	req := kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{config},
	}
	log.Debugf("CreateTopics request: %+v", req)

	resp, err := c.client.CreateTopics(ctx, &req)
	log.Debugf("CreateTopics response: %+v (%+v)", resp, err)
	if err != nil {
		return err
	}
	if err = util.KafkaErrorsToErr(resp.Errors); err != nil {
		return err
	}
	return nil
}

// AssignPartitions sets the replica broker IDs for one or more partitions in a topic.
func (c *BrokerAdminClient) AssignPartitions(
	ctx context.Context,
	topic string,
	assignments []PartitionAssignment,
) error {
	if c.config.ReadOnly {
		return errors.New("Cannot assign partitions in read-only mode")
	}

	apiAssignments := []kafka.AlterPartitionReassignmentsRequestAssignment{}
	for _, assignment := range assignments {
		apiAssignment := kafka.AlterPartitionReassignmentsRequestAssignment{
			PartitionID: assignment.ID,
			BrokerIDs:   assignment.Replicas,
		}
		apiAssignments = append(apiAssignments, apiAssignment)
	}

	req := kafka.AlterPartitionReassignmentsRequest{
		Topic:       topic,
		Assignments: apiAssignments,
		Timeout:     defaultTimeout,
	}
	log.Debugf("AlterPartitionReassignments request: %+v", req)

	resp, err := c.client.AlterPartitionReassignments(ctx, &req)
	log.Debugf("AlterPartitionReassignments response: %+v (%+v)", resp, err)
	if err != nil {
		return err
	}
	if err = resp.Error; err != nil {
		return err
	}

	return err
}

// AddPartitions extends a topic by adding one or more new partitions to it.
func (c *BrokerAdminClient) AddPartitions(
	ctx context.Context,
	topic string,
	newAssignments []PartitionAssignment,
) error {
	if c.config.ReadOnly {
		return errors.New("Cannot add partitions in read-only mode")
	}

	topicInfo, err := c.GetTopic(ctx, topic, false)
	if err != nil {
		return err
	}

	topicPartitions := kafka.TopicPartitionsConfig{
		Name:  topic,
		Count: int32(len(newAssignments)) + int32(len(topicInfo.Partitions)),
	}
	partitionAssignments := []kafka.TopicPartitionAssignment{}

	for _, newAssignment := range newAssignments {
		brokerIDsInt32 := []int32{}
		for _, replica := range newAssignment.Replicas {
			brokerIDsInt32 = append(brokerIDsInt32, int32(replica))
		}

		partitionAssignments = append(
			partitionAssignments,
			kafka.TopicPartitionAssignment{
				BrokerIDs: brokerIDsInt32,
			},
		)
	}

	topicPartitions.TopicPartitionAssignments = partitionAssignments

	req := kafka.CreatePartitionsRequest{
		Topics: []kafka.TopicPartitionsConfig{topicPartitions},
	}
	log.Debugf("CreatePartitions request: %+v", req)

	resp, err := c.client.CreatePartitions(ctx, &req)
	log.Debugf("CreatePartitions response: %+v (%+v)", resp, err)
	if err != nil {
		return err
	}
	if err = util.KafkaErrorsToErr(resp.Errors); err != nil {
		return err
	}

	return err
}

// RunLeaderElection triggers a leader election for one or more partitions in a topic.
func (c *BrokerAdminClient) RunLeaderElection(
	ctx context.Context,
	topic string,
	partitions []int,
) error {
	if c.config.ReadOnly {
		return errors.New("Cannot run leader election in read-only mode")
	}

	req := kafka.ElectLeadersRequest{
		Topic:      topic,
		Partitions: partitions,
		Timeout:    defaultTimeout,
	}
	log.Debugf("ElectLeaders request: %+v", req)

	resp, err := c.client.ElectLeaders(ctx, &req)
	log.Debugf("ElectLeaders response: %+v (%+v)", resp, err)
	if err != nil {
		return err
	}
	if err = resp.Error; err != nil {
		return err
	}

	return err
}

// AcquireLock acquires a lock that can be used to prevent simultaneous changes to a topic.
// NOTE: Not implemented for broker-based clients.
func (c *BrokerAdminClient) AcquireLock(ctx context.Context, path string) (
	zk.Lock,
	error,
) {
	// Not implemented since we don't have access to zookeeper.
	return nil, nil
}

// LockHeld returns whether a lock is currently held for the given path.
// NOTE: Not implemented for broker-based clients.
func (c *BrokerAdminClient) LockHeld(ctx context.Context, path string) (bool, error) {
	// Not implemented since we don't have access to zookeeper.
	return false, nil
}

// GetSupportedFeatures gets the features supported by the cluster for this client.
func (c *BrokerAdminClient) GetSupportedFeatures() SupportedFeatures {
	return c.supportedFeatures
}

// Close closes the client.
func (c *BrokerAdminClient) Close() error {
	return nil
}

func (c *BrokerAdminClient) getMetadata(
	ctx context.Context,
	topics []string,
) (*kafka.MetadataResponse, error) {
	req := kafka.MetadataRequest{
		Topics: topics,
	}
	log.Debugf("Metadata request: %+v", req)
	resp, err := c.client.Metadata(ctx, &req)
	log.Debugf("Metadata response: %+v (%+v)", resp, err)

	return resp, err
}

func (c *BrokerAdminClient) getAPIVersions(ctx context.Context) (
	*kafka.ApiVersionsResponse,
	error,
) {
	req := kafka.ApiVersionsRequest{}
	log.Debugf("API versions request: %+v", req)
	resp, err := c.client.ApiVersions(ctx, &req)
	log.Debugf("API versions response: %+v (%+v)", resp, err)

	return resp, err
}

func brokerIDs(brokers []kafka.Broker) []int {
	ids := []int{}
	for _, broker := range brokers {
		ids = append(ids, broker.ID)
	}
	return ids
}

func configEntriesToAPIConfigs(
	configEntries []kafka.ConfigEntry,
) []kafka.IncrementalAlterConfigsRequestConfig {
	apiConfigs := []kafka.IncrementalAlterConfigsRequestConfig{}
	for _, entry := range configEntries {
		var op kafka.ConfigOperation

		if entry.ConfigValue == "" {
			op = kafka.ConfigOperationDelete
		} else {
			op = kafka.ConfigOperationSet
		}

		apiConfigs = append(
			apiConfigs,
			kafka.IncrementalAlterConfigsRequestConfig{
				Name:            entry.ConfigName,
				Value:           entry.ConfigValue,
				ConfigOperation: op,
			},
		)
	}

	return apiConfigs
}
