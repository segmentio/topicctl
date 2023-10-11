package admin

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/util"
	"github.com/segmentio/topicctl/pkg/zk"
	log "github.com/sirupsen/logrus"
)

const (
	// Various paths in zookeeper
	assignmentPath    = "/admin/reassign_partitions"
	electionPath      = "/admin/preferred_replica_election"
	brokersPath       = "/brokers/ids"
	topicsPath        = "/brokers/topics"
	clusterIDPath     = "/cluster/id"
	brokerConfigsPath = "/config/brokers"
	configChangesPath = "/config/changes/config_change_"
	topicConfigsPath  = "/config/topics"

	// The maximum number of topics to fetch in parallel
	maxPoolSize = 20
)

var (
	// ErrTopicDoesNotExist is returned by admin functions when a topic that should exist
	// does not.
	ErrTopicDoesNotExist = errors.New("Topic does not exist")
)

// ZKAdminClient is a general client for interacting with a kafka cluster that assumes
// zookeeper access. Most interactions are done via the latter, but a few (e.g., creating topics or
// getting the controller address) are done via the broker API instead.
type ZKAdminClient struct {
	zkClient       zk.Client
	zkPrefix       string
	bootstrapAddrs []string
	Connector      *Connector
	sess           *session.Session
	readOnly       bool
}

var _ Client = (*ZKAdminClient)(nil)

// ZKAdminClientConfig contains all of the parameters necessary to create a kafka admin client.
type ZKAdminClientConfig struct {
	ZKAddrs           []string
	ZKPrefix          string
	BootstrapAddrs    []string
	ExpectedClusterID string
	Sess              *session.Session
	ReadOnly          bool
}

// NewZKAdminClient creates and returns a new Client instance.
func NewZKAdminClient(
	ctx context.Context,
	config ZKAdminClientConfig,
) (*ZKAdminClient, error) {
	zkClient, err := zk.NewPooledClient(
		config.ZKAddrs,
		time.Minute,
		&zk.DebugLogger{},
		10,
		config.ReadOnly,
	)
	if err != nil {
		return nil, err
	}

	zkPrefix := config.ZKPrefix

	// Normalize prefix
	if zkPrefix != "" {
		if !strings.HasPrefix(zkPrefix, "/") {
			zkPrefix = fmt.Sprintf("/%s", zkPrefix)
		}
		if strings.HasSuffix(zkPrefix, "/") {
			zkPrefix = zkPrefix[:len(zkPrefix)-1]
		}
	}

	client := &ZKAdminClient{
		zkClient: zkClient,
		zkPrefix: zkPrefix,
		sess:     config.Sess,
		readOnly: config.ReadOnly,
	}

	if config.ExpectedClusterID != "" {
		log.Info("Checking cluster ID against version in cluster")
		clusterID, err := client.GetClusterID(ctx)
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

	var bootstrapAddrs []string

	if len(config.BootstrapAddrs) == 0 {
		log.Debug("No bootstrap addresses provided, getting one from zookeeper")
		ids, err := client.GetBrokerIDs(ctx)
		if err != nil {
			return nil, err
		}
		// Just use the first ID
		brokers, err := client.GetBrokers(ctx, []int{ids[0]})
		if err != nil {
			return nil, err
		}
		bootstrapAddrs = []string{brokers[0].Addr()}
	} else {
		bootstrapAddrs = append(bootstrapAddrs, config.BootstrapAddrs...)
	}

	client.bootstrapAddrs = bootstrapAddrs
	client.Connector, err = NewConnector(
		ConnectorConfig{
			BrokerAddr: bootstrapAddrs[0],
		},
	)

	return client, nil
}

// GetClusterID gets the cluster ID from zookeeper. This ID is generated when the cluster is
// created and should be stable over the life of the cluster.
func (c *ZKAdminClient) GetClusterID(
	ctx context.Context,
) (string, error) {
	zkClusterIDPath := c.zNode(clusterIDPath)

	zkClusterIDObj := zkClusterID{}
	_, err := c.zkClient.GetJSON(ctx, zkClusterIDPath, &zkClusterIDObj)
	if err != nil {
		return "", err
	}

	return zkClusterIDObj.ID, nil
}

// GetBrokers gets information on one or more cluster brokers from zookeeper.
// If the argument ids is unset, then it fetches all brokers.
func (c *ZKAdminClient) GetBrokers(
	ctx context.Context,
	ids []int,
) ([]BrokerInfo, error) {
	// TODO (maybe): Use Kafka API instead of ZK to get broker info
	var brokerIDs []int
	var err error

	if len(ids) > 0 {
		brokerIDs = ids
	} else {
		brokerIDs, err = c.GetBrokerIDs(ctx)
		if err != nil {
			return nil, err
		}
	}

	brokers := []BrokerInfo{}
	brokerHosts := []string{}

	for _, id := range brokerIDs {
		zkBrokerInfo := zkBrokerInfo{}
		_, err = c.zkClient.GetJSON(
			ctx,
			c.zNode(brokersPath, fmt.Sprintf("%d", id)),
			&zkBrokerInfo,
		)
		if err != nil {
			return nil, err
		}

		zBrokerConfigPath := c.zNode(brokerConfigsPath, fmt.Sprintf("%d", id))
		zkBrokerConfig := zkBrokerConfig{}

		exists, _, err := c.zkClient.Exists(
			ctx,
			zBrokerConfigPath,
		)
		if err != nil {
			return nil, err
		}
		if exists {
			_, err = c.zkClient.GetJSON(
				ctx,
				zBrokerConfigPath,
				&zkBrokerConfig,
			)
			if err != nil {
				return nil, err
			}
		}

		epochMillis, err := strconv.ParseInt(
			zkBrokerInfo.TimestampStr,
			10,
			64,
		)
		if err != nil {
			return nil, err
		}

		brokerInfo := BrokerInfo{
			ID:        int(id),
			Endpoints: zkBrokerInfo.Endpoints,
			Host:      zkBrokerInfo.Host,
			Port:      zkBrokerInfo.Port,
			Rack:      zkBrokerInfo.Rack,
			Version:   zkBrokerInfo.Version,
			Timestamp: time.Unix(epochMillis/1000, 0),
			Config:    zkBrokerConfig.Config,
		}

		brokerHosts = append(
			brokerHosts,
			zkBrokerInfo.Host,
		)

		brokers = append(
			brokers,
			brokerInfo,
		)
	}

	instances, err := c.getInstances(ctx, brokerHosts)
	if err != nil {
		log.Debugf("Could not get instance info from EC2: %+v", err)
	}

	for b := 0; b < len(brokers); b++ {
		instance, ok := instances[brokers[b].Host]
		if !ok {
			continue
		}
		brokers[b].InstanceID = aws.StringValue(instance.InstanceId)
		brokers[b].InstanceType = aws.StringValue(instance.InstanceType)
		brokers[b].AvailabilityZone = aws.StringValue(
			instance.Placement.AvailabilityZone,
		)
	}

	sort.Slice(brokers, func(i, j int) bool {
		return brokers[i].ID < brokers[j].ID
	})

	return brokers, nil
}

// GetBrokerIDs returns a slice of all broker IDs.
func (c *ZKAdminClient) GetBrokerIDs(ctx context.Context) ([]int, error) {
	zPath := c.zNode(brokersPath)

	brokerIDStrs, _, err := c.zkClient.Children(ctx, zPath)
	if err != nil {
		return nil, fmt.Errorf(
			"Error getting children at path %s: %+v",
			zPath,
			err,
		)
	}

	brokerIDs := []int{}

	for _, idStr := range brokerIDStrs {
		id, err := strconv.ParseInt(idStr, 10, 32)
		if err != nil {
			return nil, err
		}
		brokerIDs = append(brokerIDs, int(id))
	}

	return brokerIDs, nil
}

// GetConnector returns the Connector instance associated with this client.
func (c *ZKAdminClient) GetConnector() *Connector {
	return c.Connector
}

// GetTopics gets information about one or more cluster topics from zookeeper.
// If the argument names is unset, then it fetches all topics. The detailed
// parameter determines whether the ISRs and leaders are fetched for each
// partition.
func (c *ZKAdminClient) GetTopics(
	ctx context.Context,
	names []string,
	detailed bool,
) ([]TopicInfo, error) {
	var topicNames []string
	var err error

	if len(names) > 0 {
		topicNames = names
	} else {
		topicNames, err = c.GetTopicNames(ctx)
		if err != nil {
			return nil, err
		}
	}

	log.Debugf(
		"Looking up %d topic names: %+v",
		len(topicNames),
		topicNames,
	)

	topics := []TopicInfo{}

	type topicReq struct {
		name     string
		detailed bool
	}

	type topicResp struct {
		topic TopicInfo
		err   error
	}

	// This operation can be slow if there are a lot of topics, distribute it out
	topicReqChan := make(chan topicReq, len(topicNames))
	topicRespChan := make(chan topicResp, len(topicNames))
	defer close(topicReqChan)

	for _, name := range topicNames {
		topicReqChan <- topicReq{
			name:     name,
			detailed: detailed,
		}
	}

	poolSize := minInt(len(topicNames), maxPoolSize)

	for i := 0; i < poolSize; i++ {
		go func() {
			for {
				topicReq, ok := <-topicReqChan
				if !ok {
					return
				}
				topic, err := c.getTopic(ctx, topicReq.name, topicReq.detailed)

				topicRespChan <- topicResp{
					topic: topic,
					err:   err,
				}
			}
		}()
	}

	for i := 0; i < len(topicNames); i++ {
		topicResp := <-topicRespChan
		if topicResp.err != nil {
			return nil, err
		}
		topics = append(topics, topicResp.topic)
	}

	sort.Slice(topics, func(i, j int) bool {
		return topics[i].Name < topics[j].Name
	})

	return topics, nil
}

// GetTopicNames gets all topic names from zookeeper.
func (c *ZKAdminClient) GetTopicNames(ctx context.Context) ([]string, error) {
	zPath := c.zNode(topicsPath)

	topicNames, _, err := c.zkClient.Children(ctx, zPath)
	if err != nil {
		return nil, fmt.Errorf(
			"Error getting children at path %s: %+v",
			zPath,
			err,
		)
	}
	return topicNames, nil
}

// GetTopic is a wrapper around GetTopics(...) for getting information about
// a single topic.
func (c *ZKAdminClient) GetTopic(
	ctx context.Context,
	name string,
	detailed bool,
) (TopicInfo, error) {
	// TODO (maybe): Use Kafka API instead of ZK to get topic info

	topics, err := c.GetTopics(ctx, []string{name}, detailed)
	if err != nil {
		if strings.Contains(err.Error(), "node does not exist") {
			return TopicInfo{}, ErrTopicDoesNotExist
		}
		return TopicInfo{}, err
	}

	if len(topics) != 1 {
		return TopicInfo{}, ErrTopicDoesNotExist
	}

	return topics[0], nil
}

func (c *ZKAdminClient) GetACLs(
	ctx context.Context,
	filter kafka.ACLFilter,
) ([]ACLInfo, error) {
	return nil, errors.New("ACLs not yet supported with zk access mode; omit zk addresses to fix.")
}

func (c *ZKAdminClient) CreateACLs(
	ctx context.Context,
	acls []kafka.ACLEntry,
) error {
	return errors.New("ACLs not yet supported with zk access mode; omit zk addresses to fix.")
}

func (c *ZKAdminClient) GetUsers(
	ctx context.Context,
	names []string,
) ([]UserInfo, error) {
	return nil, errors.New("Users not yet supported with zk access mode; omit zk addresses to fix.")
}

func (c *ZKAdminClient) CreateUser(
	ctx context.Context,
	user kafka.UserScramCredentialsUpsertion,
) error {
	return errors.New("Users not yet supported with zk access mode; omit zk addresses to fix.")
}

// UpdateTopicConfig updates the config JSON for a topic and sets a change
// notification so that the brokers are notified. If overwrite is true, then
// it will overwrite existing config entries.
//
// The function returns the list of keys that were modified. If overwrite is
// set to false, this can be used to determine the subset of entries
// that were already set.
func (c *ZKAdminClient) UpdateTopicConfig(
	ctx context.Context,
	name string,
	configEntries []kafka.ConfigEntry,
	overwrite bool,
) ([]string, error) {
	var updatedKeys []string
	if c.readOnly {
		return updatedKeys, errors.New("Cannot update topic in read-only mode")
	}
	log.Debugf("Updating config for topic %s", name)

	configMap := map[string]interface{}{}
	zPath := c.zNode(topicConfigsPath, name)

	stats, err := c.zkClient.GetJSON(
		ctx,
		zPath,
		&configMap,
	)
	if err != nil {
		return updatedKeys, err
	}

	updatedKeys, err = updateConfig(configMap, configEntries, overwrite)
	if err != nil {
		return updatedKeys, err
	}

	_, err = c.zkClient.SetJSON(ctx, zPath, configMap, stats.Version)
	if err != nil {
		return updatedKeys, err
	}

	changeObj := zkChangeNotification{
		Version:    2,
		EntityPath: fmt.Sprintf("topics/%s", name),
	}
	log.Debugf("Setting change notification: %+v", changeObj)
	return updatedKeys,
		c.zkClient.CreateJSON(ctx, c.zNode(configChangesPath), changeObj, true)
}

// UpdateBrokerConfig updates the config JSON for a cluster broker and
// sets a change notification so the cluster brokers are notified. If overwrite is
// true, then it will overwrite existing config entries.
//
// The function returns the list of keys that were modified. If overwrite is
// set to false, this can be used to determine the subset of entries
func (c *ZKAdminClient) UpdateBrokerConfig(
	ctx context.Context,
	id int,
	configEntries []kafka.ConfigEntry,
	overwrite bool,
) ([]string, error) {
	var updatedKeys []string
	if c.readOnly {
		return updatedKeys, errors.New("Cannot update topic in read-only mode")
	}
	log.Debugf("Updating config for broker %d", id)

	idStr := fmt.Sprintf("%d", id)

	// Broker configs parent might not already exist
	zBrokerRoot := c.zNode(brokerConfigsPath)

	exists, _, err := c.zkClient.Exists(ctx, zBrokerRoot)
	if err != nil {
		return nil, err
	}
	if !exists {
		log.Infof("Creating broker configs path: %s", zBrokerRoot)
		err := c.zkClient.Create(ctx, zBrokerRoot, nil, false)
		if err != nil {
			return updatedKeys, err
		}
	}

	// Broker config might not exist
	zBrokerPath := c.zNode(brokerConfigsPath, idStr)

	exists, _, err = c.zkClient.Exists(ctx, zBrokerPath)
	if err != nil {
		return nil, err
	}
	if !exists {
		zkBrokerConfigObj := zkBrokerConfig{
			Version: 1,
			Config:  map[string]string{},
		}

		log.Infof("Creating broker config at %s: %+v", zBrokerPath, zkBrokerConfigObj)
		err := c.zkClient.CreateJSON(ctx, zBrokerPath, zkBrokerConfigObj, false)
		if err != nil {
			return updatedKeys, err
		}
	}

	configMap := map[string]interface{}{}

	zPath := c.zNode(brokerConfigsPath, idStr)

	stats, err := c.zkClient.GetJSON(
		ctx,
		zPath,
		&configMap,
	)
	if err != nil {
		return updatedKeys, err
	}

	updatedKeys, err = updateConfig(configMap, configEntries, overwrite)
	if err != nil {
		return updatedKeys, err
	}

	_, err = c.zkClient.SetJSON(ctx, zPath, configMap, stats.Version)
	if err != nil {
		return updatedKeys, err
	}

	changeObj := zkChangeNotification{
		Version:    2,
		EntityPath: fmt.Sprintf("brokers/%s", idStr),
	}
	log.Debugf("Setting change notification: %+v", changeObj)
	return updatedKeys,
		c.zkClient.CreateJSON(ctx, c.zNode(configChangesPath), changeObj, true)
}

// CreateTopic creates a new topic with the argument config. It uses
// the topic creation API exposed on the controller broker.
func (c *ZKAdminClient) CreateTopic(
	ctx context.Context,
	config kafka.TopicConfig,
) error {
	if c.readOnly {
		return errors.New("Cannot create topic in read-only mode")
	}

	req := kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{config},
	}
	log.Debugf("Creating topic with config %+v", config)

	resp, err := c.Connector.KafkaClient.CreateTopics(ctx, &req)
	if err != nil {
		return err
	}
	if err = util.KafkaErrorsToErr(resp.Errors); err != nil {
		return err
	}

	return err
}

// AssignPartitions notifies the cluster to begin a partition reassignment.
// This should only be used for existing partitions; to create new partitions,
// use the AddPartitions method.
func (c *ZKAdminClient) AssignPartitions(
	ctx context.Context,
	topic string,
	assignments []PartitionAssignment,
) error {
	if c.readOnly {
		return errors.New("Cannot assign partitions in read-only mode")
	}

	zkAssignmentObj := zkAssignment{
		Version:    1,
		Partitions: []zkAssignmentPartition{},
	}
	for _, assignment := range assignments {
		zkAssignmentObj.Partitions = append(
			zkAssignmentObj.Partitions,
			zkAssignmentPartition{
				Topic:     topic,
				Partition: assignment.ID,
				Replicas:  util.CopyInts(assignment.Replicas),
			},
		)
	}

	zNode := c.zNode(assignmentPath)
	log.Infof(
		"Writing reassignment config to zk path %s: %+v",
		zNode,
		zkAssignmentObj,
	)

	return c.zkClient.CreateJSON(
		ctx,
		zNode,
		zkAssignmentObj,
		false,
	)
}

// AddPartitions adds one or more partitions to an existing topic. Unlike
// AssignPartitions, this directly updates the topic's partition config in
// zookeeper.
func (c *ZKAdminClient) AddPartitions(
	ctx context.Context,
	topic string,
	newAssignments []PartitionAssignment,
) error {
	if c.readOnly {
		return errors.New("Cannot add partitions in read-only mode")
	}

	// Use raw map[string]interface instead of struct to ensure we don't omit any
	// fields in existing config.
	topicInfo := map[string]interface{}{}
	zNode := c.zNode(topicsPath, topic)

	stats, err := c.zkClient.GetJSON(
		ctx,
		zNode,
		&topicInfo,
	)
	if err != nil {
		return err
	}

	rawPartitions, ok := topicInfo["partitions"]
	if !ok {
		return fmt.Errorf("Cannot find partitions in topic map: %+v", topicInfo)
	}
	partititions, ok := rawPartitions.(map[string]interface{})
	if !ok {
		return errors.New("Cannot convert partitions to map")
	}

	for _, assignment := range newAssignments {
		idStr := fmt.Sprintf("%d", assignment.ID)

		if _, ok := partititions[idStr]; ok {
			return fmt.Errorf("Partition %s already exists", idStr)
		}

		partititions[idStr] = assignment.Replicas
	}

	topicInfo["partitions"] = partititions

	log.Infof(
		"Overwriting config for topic %s with new version: %+v",
		topic,
		topicInfo,
	)

	_, err = c.zkClient.SetJSON(ctx, zNode, topicInfo, stats.Version)
	return err
}

// RunLeaderElection triggers a leader election for the argument
// topic and partitions.
func (c *ZKAdminClient) RunLeaderElection(
	ctx context.Context,
	topic string,
	partitions []int,
) error {
	if c.readOnly {
		return errors.New("Cannot run leader election in read-only mode")
	}

	zkElectionObj := zkElection{
		Version:    1,
		Partitions: []zkElectionTopicPartition{},
	}

	for _, partition := range partitions {
		zkElectionObj.Partitions = append(
			zkElectionObj.Partitions,
			zkElectionTopicPartition{
				Topic:     topic,
				Partition: partition,
			},
		)
	}

	zNode := c.zNode(electionPath)
	log.Infof(
		"Writing leader election config to zk path %s: %+v",
		zNode,
		zkElectionObj,
	)

	return c.zkClient.CreateJSON(
		ctx,
		zNode,
		zkElectionObj,
		false,
	)
}

// AcquireLock acquires and returns a lock from the underlying zookeeper client.
// The Unlock method should be called on the lock when it's safe to release.
func (c *ZKAdminClient) AcquireLock(
	ctx context.Context,
	path string,
) (zk.Lock, error) {
	return c.zkClient.AcquireLock(ctx, path)
}

// LockHeld determines whether the lock with the provided path is held (i.e., has children).
func (c *ZKAdminClient) LockHeld(
	ctx context.Context,
	path string,
) (bool, error) {
	exists, _, err := c.zkClient.Exists(ctx, path)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}

	children, _, err := c.zkClient.Children(ctx, path)
	if err != nil {
		return false, err
	}
	return len(children) > 0, nil
}

// GetSupportedFeatures returns the features that are supported by this client.
func (c *ZKAdminClient) GetSupportedFeatures() SupportedFeatures {
	// The zk-based client supports everything except for ACLs.
	// Zookeeper can support ACLs, topicctl just hasn't added support for it yet.
	return SupportedFeatures{
		Reads:                true,
		Applies:              true,
		Locks:                true,
		DynamicBrokerConfigs: true,
		ACLs:                 false,
		Users:                false,
	}
}

// Close closes the connections in the underlying zookeeper client.
func (c *ZKAdminClient) Close() error {
	return c.zkClient.Close()
}

// getControllerAddr gets the address of the cluster controller. This is needed
// for creating new topics and other operations.
func (c *ZKAdminClient) getControllerAddr(
	ctx context.Context,
) (string, error) {
	conn, err := kafka.DefaultDialer.DialContext(ctx, "tcp", c.bootstrapAddrs[0])
	if err != nil {
		return "", err
	}
	defer conn.Close()

	broker, err := conn.Controller()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%d", broker.Host, broker.Port), nil
}

func (c *ZKAdminClient) getTopic(
	ctx context.Context,
	name string,
	detailed bool,
) (TopicInfo, error) {
	log.Debugf("Getting info for topic %s", name)

	topicInfo := TopicInfo{
		Name:       name,
		Partitions: []PartitionInfo{},
	}
	zkTopicInfo := zkTopicInfo{}

	_, err := c.zkClient.GetJSON(
		ctx,
		c.zNode(topicsPath, name),
		&zkTopicInfo,
	)
	if err != nil {
		return topicInfo, err
	}

	topicInfo.Version = zkTopicInfo.Version

	zkTopicConfig := zkTopicConfig{}
	_, err = c.zkClient.GetJSON(
		ctx,
		c.zNode(topicConfigsPath, name),
		&zkTopicConfig,
	)
	if err != nil {
		return topicInfo, err
	}

	topicInfo.Config = zkTopicConfig.Config

	type partitionReq struct {
		id       int
		replicas []int
		detailed bool
	}

	type partitionResp struct {
		partition PartitionInfo
		err       error
	}

	// This operation can be slow if there are a lot of partitions, distribute it out
	partitionReqChan := make(chan partitionReq, len(zkTopicInfo.Partitions))
	partitionRespChan := make(chan partitionResp, len(zkTopicInfo.Partitions))

	for partitionIDStr, replicas := range zkTopicInfo.Partitions {
		partitionID, err := strconv.ParseInt(partitionIDStr, 10, 32)
		if err != nil {
			return topicInfo, err
		}

		partitionReqChan <- partitionReq{
			id:       int(partitionID),
			replicas: replicas,
			detailed: detailed,
		}
	}

	poolSize := minInt(len(zkTopicInfo.Partitions), maxPoolSize)

	for i := 0; i < poolSize; i++ {
		go func() {
			for {
				partitionReq, ok := <-partitionReqChan
				if !ok {
					return
				}
				partition, err := c.getPartition(
					ctx,
					partitionReq.id,
					name,
					partitionReq.detailed,
					partitionReq.replicas,
				)

				partitionRespChan <- partitionResp{
					partition: partition,
					err:       err,
				}
			}
		}()
	}

	for i := 0; i < len(zkTopicInfo.Partitions); i++ {
		partitionResp := <-partitionRespChan
		if partitionResp.err != nil {
			return topicInfo, err
		}
		topicInfo.Partitions = append(
			topicInfo.Partitions,
			partitionResp.partition,
		)
	}

	sort.Slice(topicInfo.Partitions, func(i, j int) bool {
		return topicInfo.Partitions[i].ID < topicInfo.Partitions[j].ID
	})

	return topicInfo, nil
}

func (c *ZKAdminClient) getPartition(
	ctx context.Context,
	id int,
	topicName string,
	detailed bool,
	replicas []int,
) (PartitionInfo, error) {
	partitionInfo := PartitionInfo{
		Topic:    topicName,
		ID:       int(id),
		Replicas: replicas,
	}

	if detailed {
		zkPartitionInfo := zkPartitionInfo{}
		_, err := c.zkClient.GetJSON(
			ctx,
			c.zNode(
				topicsPath,
				topicName,
				"partitions",
				fmt.Sprintf("%d", id),
				"state",
			),
			&zkPartitionInfo,
		)
		if err != nil {
			return partitionInfo, err
		}

		partitionInfo.ControllerEpoch = zkPartitionInfo.ControllerEpoch
		partitionInfo.ISR = zkPartitionInfo.ISR
		partitionInfo.Leader = zkPartitionInfo.Leader
		partitionInfo.ControllerEpoch = zkPartitionInfo.ControllerEpoch
		partitionInfo.LeaderEpoch = zkPartitionInfo.LeaderEpoch
		partitionInfo.Version = zkPartitionInfo.Version
	}

	return partitionInfo, nil
}

// assignmentInProgress returns whether the zk assignment node exists.
func (c *ZKAdminClient) assignmentInProgress(
	ctx context.Context,
) (bool, error) {
	exists, _, err := c.zkClient.Exists(
		ctx,
		c.zNode(assignmentPath),
	)
	return exists, err
}

// electionInProgress returns whether the election zk node is set.
func (c *ZKAdminClient) electionInProgress(
	ctx context.Context,
) (bool, error) {
	exists, _, err := c.zkClient.Exists(
		ctx,
		c.zNode(electionPath),
	)
	return exists, err
}

func (c *ZKAdminClient) zNode(elements ...string) string {
	joinedElements := filepath.Join(elements...)
	return filepath.Join("/", c.zkPrefix, joinedElements)
}

func (c *ZKAdminClient) getInstances(
	ctx context.Context,
	ips []string,
) (map[string]ec2.Instance, error) {
	instancesMap := map[string]ec2.Instance{}

	if c.sess == nil {
		return instancesMap, nil
	}

	ec2Client := ec2.New(c.sess)

	ipsMap := map[string]struct{}{}

	ipValues := []*string{}
	for _, ip := range ips {
		ipValues = append(ipValues, aws.String(ip))
		ipsMap[ip] = struct{}{}
	}

	resp, err := ec2Client.DescribeInstancesWithContext(
		ctx,
		&ec2.DescribeInstancesInput{
			Filters: []*ec2.Filter{
				{
					Name:   aws.String("private-ip-address"),
					Values: ipValues,
				},
			},
		},
	)
	if err != nil {
		return instancesMap, err
	}

	for _, reservation := range resp.Reservations {
		for _, instance := range reservation.Instances {
			for _, networkInterface := range instance.NetworkInterfaces {
				privateIP := aws.StringValue(networkInterface.PrivateIpAddress)

				if _, ok := ipsMap[privateIP]; ok {
					instancesMap[privateIP] = *instance
				}
			}
		}
	}

	return instancesMap, nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func updateConfig(
	configMap map[string]interface{},
	configEntries []kafka.ConfigEntry,
	overwrite bool,
) ([]string, error) {
	updatedKeys := []string{}

	configKVInterface, ok := configMap["config"]
	if !ok {
		return updatedKeys, errors.New("Could not find a config key for topic")
	}
	configKVMap, ok := configKVInterface.(map[string]interface{})
	if !ok {
		return updatedKeys, fmt.Errorf(
			"Could not convert config to map[string]interface: %+v",
			configKVInterface,
		)
	}

	for _, entry := range configEntries {
		currValue, ok := configKVMap[entry.ConfigName]

		if ok && !overwrite {
			log.Warnf(
				"Skipping over config key %s because already is set to %s and overwrite is false",
				entry.ConfigName,
				currValue,
			)
			continue
		}

		if entry.ConfigValue == "" {
			log.Debugf(
				"Removing config value for key %s: '%v'->'%s'",
				entry.ConfigName,
				currValue,
				entry.ConfigValue,
			)
			delete(configKVMap, entry.ConfigName)
		} else {
			log.Debugf(
				"Setting config value for key %s: '%v'->'%s'",
				entry.ConfigName,
				currValue,
				entry.ConfigValue,
			)
			configKVMap[entry.ConfigName] = entry.ConfigValue
		}

		updatedKeys = append(updatedKeys, entry.ConfigName)
	}

	configMap["config"] = configKVMap
	return updatedKeys, nil
}

func (c *ZKAdminClient) GetAllTopicsMetadata(
	ctx context.Context,
) (*kafka.MetadataResponse, error) {
	client := c.GetConnector().KafkaClient
	req := kafka.MetadataRequest{
		Topics: nil,
	}

	log.Debugf("Metadata request: %+v", req)
	metadata, err := client.Metadata(ctx, &req)
	if err != nil {
		return nil, fmt.Errorf("Error fetching all topics metadata: %+v", err)
	}

	return metadata, nil
}
