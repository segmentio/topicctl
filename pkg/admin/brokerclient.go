package admin

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/zk"
	log "github.com/sirupsen/logrus"
)

const (
	defaultTimeout = 5 * time.Second
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

type BrokerAdminClientConfig struct {
	ConnectorConfig
	ReadOnly bool
}

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
	apiVersions, err := client.ApiVersions(ctx, kafka.ApiVersionsRequest{})
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

	// If we have AlterPartitionReassignments support, then we're good for applying (other needed
	// APIs are older). This should be satisfied by versions >= 2.4.
	if _, ok := maxVersions["AlterPartitionReassignments"]; ok {
		supportedFeatures.Applies = true
	}

	// If we have AlterClientQuotas support, then we're running a newer version of Kafka (>= 2.6),
	// that will provide the correct values for dynamic broker configs.
	if _, ok := maxVersions["AlterClientQuotas"]; ok {
		supportedFeatures.DynamicBrokerConfigs = true
	}
	log.Debugf("Supported features: %+v", supportedFeatures)

	return &BrokerAdminClient{
		client:            client,
		connector:         connector,
		config:            config,
		supportedFeatures: supportedFeatures,
	}, nil
}

func (c *BrokerAdminClient) GetClusterID(ctx context.Context) (string, error) {
	resp, err := c.getMetadata(ctx, nil)
	if err != nil {
		return "", err
	}
	return resp.ClusterID, nil
}

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

	brokerIDs := []int{}
	brokerIDIndices := map[int]int{}

	for b, broker := range metadataResp.Brokers {
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
		brokerIDs = append(brokerIDs, broker.ID)
		brokerIDIndices[broker.ID] = b
	}

	configsReq := kafka.DescribeConfigsRequest{
		Brokers:         brokerIDs,
		IncludeDefaults: false,
	}
	log.Debugf("DescribeConfigs request: %+v", configsReq)

	configsResp, err := c.client.DescribeConfigs(ctx, configsReq)
	log.Debugf("DescribeConfigs response: %+v (%+v)", configsResp, err)
	if err != nil {
		return nil, err
	}

	for _, broker := range configsResp.Brokers {
		index := brokerIDIndices[broker.BrokerID]
		brokerInfos[index].Config = broker.Configs
	}

	return brokerInfos, nil
}

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

func (c *BrokerAdminClient) GetConnector() *Connector {
	return c.connector
}

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
	topicInfoNames := []string{}
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
		topicInfoNames = append(topicInfoNames, topic.Name)
		topicNameToIndex[topic.Name] = t
	}

	configsReq := kafka.DescribeConfigsRequest{
		Topics:          topicInfoNames,
		IncludeDefaults: false,
	}
	log.Debugf("DescribeConfigs request: %+v", configsReq)

	configsResp, err := c.client.DescribeConfigs(ctx, configsReq)
	log.Debugf("DescribeConfigs response: %+v (%+v)", configsResp, err)
	if err != nil {
		return nil, err
	}

	for _, topic := range configsResp.Topics {
		index := topicNameToIndex[topic.Topic]
		topicInfos[index].Config = topic.Configs
	}

	return topicInfos, nil
}

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

func (c *BrokerAdminClient) UpdateTopicConfig(
	ctx context.Context,
	name string,
	configEntries []kafka.ConfigEntry,
	overwrite bool,
) ([]string, error) {
	if c.config.ReadOnly {
		return nil, errors.New("Cannot update topic config read-only mode")
	}

	req := kafka.AlterTopicConfigsRequest{
		Topic:         name,
		ConfigEntries: configEntries,
	}
	log.Debugf("AlterTopicConfigs request: %+v", req)

	// TODO: Handle case where overwrite is false.
	resp, err := c.client.AlterTopicConfigs(ctx, req)
	log.Debugf("AlterTopicConfigs response: %+v (%+v)", resp, err)
	if err != nil {
		return nil, err
	}

	updated := []string{}
	for _, entry := range configEntries {
		updated = append(updated, entry.ConfigName)
	}

	return updated, nil
}

func (c *BrokerAdminClient) UpdateBrokerConfig(
	ctx context.Context,
	id int,
	configEntries []kafka.ConfigEntry,
	overwrite bool,
) ([]string, error) {
	if c.config.ReadOnly {
		return nil, errors.New("Cannot update broker config read-only mode")
	}

	req := kafka.AlterBrokerConfigsRequest{
		BrokerID:      id,
		ConfigEntries: configEntries,
	}
	log.Debugf("AlterBrokerConfigs request: %+v", req)

	// TODO: Handle case where overwrite is false.
	resp, err := c.client.AlterBrokerConfigs(ctx, req)
	log.Debugf("AlterBrokerConfigs response: %+v (%+v)", resp, err)
	if err != nil {
		return nil, err
	}

	updated := []string{}
	for _, entry := range configEntries {
		updated = append(updated, entry.ConfigName)
	}

	return updated, nil
}

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
	return err
}

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

	resp, err := c.client.AlterPartitionReassignments(ctx, req)
	log.Debugf("AlterPartitionReassignments response: %+v (%+v)", resp, err)
	return err
}

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

	partitions := []kafka.CreatePartitionsRequestPartition{}
	for _, newAssignment := range newAssignments {
		partitions = append(
			partitions,
			kafka.CreatePartitionsRequestPartition{
				BrokerIDs: newAssignment.Replicas,
			},
		)
	}

	req := kafka.CreatePartitionsRequest{
		Topic:         topic,
		NewPartitions: partitions,
		TotalCount:    len(partitions) + len(topicInfo.Partitions),
		Timeout:       defaultTimeout,
	}
	log.Debugf("CreatePartitions request: %+v", req)

	resp, err := c.client.CreatePartitions(ctx, req)
	log.Debugf("CreatePartitions response: %+v (%+v)", resp, err)

	return err
}

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

	resp, err := c.client.ElectLeaders(ctx, req)
	log.Debugf("ElectLeaders response: %+v (%+v)", resp, err)

	return err
}

func (c *BrokerAdminClient) AcquireLock(ctx context.Context, path string) (
	zk.Lock,
	error,
) {
	// Not implemented since we don't have access to zookeeper.
	return nil, nil
}

func (c *BrokerAdminClient) LockHeld(ctx context.Context, path string) (bool, error) {
	// Not implemented since we don't have access to zookeeper.
	return false, nil
}

func (c *BrokerAdminClient) GetSupportedFeatures() SupportedFeatures {
	return c.supportedFeatures
}

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
	resp, err := c.client.ApiVersions(ctx, req)
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
