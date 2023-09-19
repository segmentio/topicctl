package admin

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/util"
	log "github.com/sirupsen/logrus"
)

const (
	// RetentionKey is the config key used for topic time retention.
	RetentionKey = "retention.ms"

	// LeaderThrottledKey is the config key for the leader throttle rate.
	LeaderThrottledKey = "leader.replication.throttled.rate"

	// FollowerThrottledKey is the config key for the follower throttle rate.
	FollowerThrottledKey = "follower.replication.throttled.rate"

	// LeaderReplicasThrottledKey is the config key for the list of leader replicas
	// that should be throttled.
	LeaderReplicasThrottledKey = "leader.replication.throttled.replicas"

	// FollowerReplicasThrottledKey is the config key for the list of follower replicas
	// that should be throttled.
	FollowerReplicasThrottledKey = "follower.replication.throttled.replicas"
)

// BrokerInfo represents the information stored about a broker in zookeeper.
type BrokerInfo struct {
	ID               int               `json:"id"`
	Endpoints        []string          `json:"endpoints"`
	Host             string            `json:"host"`
	Port             int32             `json:"port"`
	InstanceID       string            `json:"instanceID"`
	AvailabilityZone string            `json:"availabilityZone"`
	Rack             string            `json:"rack"`
	InstanceType     string            `json:"instanceType"`
	Version          int               `json:"version"`
	Timestamp        time.Time         `json:"timestamp"`
	Config           map[string]string `json:"config"`
}

// TopicInfo represents the information stored about a topic in zookeeper.
type TopicInfo struct {
	Name       string            `json:"name"`
	Config     map[string]string `json:"config"`
	Partitions []PartitionInfo   `json:"partitions"`
	Version    int               `json:"version"`
}

// PartitionInfo represents the information stored about a topic
// partition in zookeeper.
type PartitionInfo struct {
	Topic           string `json:"topic"`
	ID              int    `json:"ID"`
	Leader          int    `json:"leader"`
	Version         int    `json:"version"`
	Replicas        []int  `json:"replicas"`
	ISR             []int  `json:"isr"`
	ControllerEpoch int    `json:"controllerEpoch"`
	LeaderEpoch     int    `json:"leaderEpoch"`
}

// PartitionAssignment contains the actual or desired assignment of
// replicas in a topic partition.
type PartitionAssignment struct {
	ID       int   `json:"id"`
	Replicas []int `json:"replicas"`
}

// PartitionInfo represents the information stored about an ACL
// in zookeeper.
type ACLInfo struct {
	ResourceType   ResourceType      `json:"resourceType"`
	ResourceName   string            `json:"resourceName"`
	PatternType    PatternType       `json:"patternType"`
	Principal      string            `json:"principal"`
	Host           string            `json:"host"`
	Operation      ACLOperationType  `json:"operation"`
	PermissionType ACLPermissionType `json:"permissionType"`
}

// ResourceType presents the Kafka resource type.
// We need to subtype this to be able to define methods to
// satisfy the Value interface from Cobra so we can use it
// as a Cobra flag.
type ResourceType kafka.ResourceType

var resourceTypeMap = map[string]kafka.ResourceType{
	"any":             kafka.ResourceTypeAny,
	"topic":           kafka.ResourceTypeTopic,
	"group":           kafka.ResourceTypeGroup,
	"cluster":         kafka.ResourceTypeCluster,
	"transactionalid": kafka.ResourceTypeTransactionalID,
	"delegationtoken": kafka.ResourceTypeDelegationToken,
}

// String is used both by fmt.Print and by Cobra in help text.
func (r *ResourceType) String() string {
	switch kafka.ResourceType(*r) {
	case kafka.ResourceTypeAny:
		return "any"
	case kafka.ResourceTypeTopic:
		return "topic"
	case kafka.ResourceTypeGroup:
		return "group"
	case kafka.ResourceTypeCluster:
		return "cluster"
	case kafka.ResourceTypeTransactionalID:
		return "transactionalid"
	case kafka.ResourceTypeDelegationToken:
		return "delegationtoken"
	default:
		return "unknown"
	}
}

// Set is used by Cobra to set the value of a variable from a Cobra flag.
func (r *ResourceType) Set(v string) error {
	rt, ok := resourceTypeMap[strings.ToLower(v)]
	if !ok {
		return errors.New(`must be one of "any", "topic", "group", "cluster", "transactionalid", or "delegationtoken"`)
	}
	*r = ResourceType(rt)
	return nil
}

// Type is used by Cobra in help text.
func (r *ResourceType) Type() string {
	return "ResourceType"
}

// PatternType presents the Kafka pattern type.
// We need to subtype this to be able to define methods to
// satisfy the Value interface from Cobra so we can use it
// as a Cobra flag.
type PatternType kafka.PatternType

var patternTypeMap = map[string]kafka.PatternType{
	"any":      kafka.PatternTypeAny,
	"match":    kafka.PatternTypeMatch,
	"literal":  kafka.PatternTypeLiteral,
	"prefixed": kafka.PatternTypePrefixed,
}

// String is used both by fmt.Print and by Cobra in help text.
func (p *PatternType) String() string {
	switch kafka.PatternType(*p) {
	case kafka.PatternTypeAny:
		return "any"
	case kafka.PatternTypeMatch:
		return "match"
	case kafka.PatternTypeLiteral:
		return "literal"
	case kafka.PatternTypePrefixed:
		return "prefixed"
	default:
		return "unknown"
	}
}

// Set is used by Cobra to set the value of a variable from a Cobra flag.
func (p *PatternType) Set(v string) error {
	pt, ok := patternTypeMap[strings.ToLower(v)]
	if !ok {
		return errors.New(`must be one of "any", "match", "literal", or "prefixed"`)
	}
	*p = PatternType(pt)
	return nil
}

// Type is used by Cobra in help text.
func (r *PatternType) Type() string {
	return "PatternType"
}

// ACLOperationType presents the Kafka operation type.
// We need to subtype this to be able to define methods to
// satisfy the Value interface from Cobra so we can use it
// as a Cobra flag.
type ACLOperationType kafka.ACLOperationType

var aclOperationTypeMap = map[string]kafka.ACLOperationType{
	"any":             kafka.ACLOperationTypeAny,
	"all":             kafka.ACLOperationTypeAll,
	"read":            kafka.ACLOperationTypeRead,
	"write":           kafka.ACLOperationTypeWrite,
	"create":          kafka.ACLOperationTypeCreate,
	"delete":          kafka.ACLOperationTypeDelete,
	"alter":           kafka.ACLOperationTypeAlter,
	"describe":        kafka.ACLOperationTypeDescribe,
	"clusteraction":   kafka.ACLOperationTypeClusterAction,
	"describeconfigs": kafka.ACLOperationTypeDescribeConfigs,
	"alterconfigs":    kafka.ACLOperationTypeAlterConfigs,
	"idempotentwrite": kafka.ACLOperationTypeIdempotentWrite,
}

// String is used both by fmt.Print and by Cobra in help text.
func (o *ACLOperationType) String() string {
	switch kafka.ACLOperationType(*o) {
	case kafka.ACLOperationTypeAny:
		return "any"
	case kafka.ACLOperationTypeAll:
		return "all"
	case kafka.ACLOperationTypeRead:
		return "read"
	case kafka.ACLOperationTypeWrite:
		return "write"
	case kafka.ACLOperationTypeCreate:
		return "create"
	case kafka.ACLOperationTypeDelete:
		return "delete"
	case kafka.ACLOperationTypeAlter:
		return "alter"
	case kafka.ACLOperationTypeDescribe:
		return "describe"
	case kafka.ACLOperationTypeClusterAction:
		return "clusteraction"
	case kafka.ACLOperationTypeDescribeConfigs:
		return "describeconfigs"
	case kafka.ACLOperationTypeAlterConfigs:
		return "alterconfigs"
	case kafka.ACLOperationTypeIdempotentWrite:
		return "idempotentwrite"
	default:
		return "unknown"
	}
}

// Set is used by Cobra to set the value of a variable from a Cobra flag.
func (o *ACLOperationType) Set(v string) error {
	ot, ok := aclOperationTypeMap[strings.ToLower(v)]
	if !ok {
		return errors.New(`must be one of "any", "all", "read", "write", "create", "delete", "alter", "describe", "clusteraction", "describeconfigs", "alterconfigs" or "idempotentwrite"`)
	}
	*o = ACLOperationType(ot)
	return nil
}

// Type is used by Cobra in help text.
func (o *ACLOperationType) Type() string {
	return "ACLOperationType"
}

// ACLPermissionType presents the Kafka operation type.
// We need to subtype this to be able to define methods to
// satisfy the Value interface from Cobra so we can use it
// as a Cobra flag.
type ACLPermissionType kafka.ACLPermissionType

var aclPermissionTypeMap = map[string]kafka.ACLPermissionType{
	"any":   kafka.ACLPermissionTypeAny,
	"allow": kafka.ACLPermissionTypeAllow,
	"deny":  kafka.ACLPermissionTypeDeny,
}

// String is used both by fmt.Print and by Cobra in help text.
func (p *ACLPermissionType) String() string {
	switch kafka.ACLPermissionType(*p) {
	case kafka.ACLPermissionTypeAny:
		return "any"
	case kafka.ACLPermissionTypeDeny:
		return "deny"
	case kafka.ACLPermissionTypeAllow:
		return "allow"
	default:
		return "unknown"
	}
}

// Set is used by Cobra to set the value of a variable from a Cobra flag.
func (p *ACLPermissionType) Set(v string) error {
	pt, ok := aclPermissionTypeMap[strings.ToLower(v)]
	if !ok {
		return errors.New(`must be one of "any", "allow", or "deny"`)
	}
	*p = ACLPermissionType(pt)
	return nil
}

// Type is used by Cobra in help text.
func (p *ACLPermissionType) Type() string {
	return "ACLPermissionType"
	
// UserInfo represents the information stored about a user
// in zookeeper.
type UserInfo struct {
	Name            string
	CredentialInfos []kafka.DescribeUserScramCredentialsCredentialInfo
}

type zkClusterID struct {
	Version string `json:"version"`
	ID      string `json:"id"`
}

type zkBrokerInfo struct {
	Endpoints    []string `json:"endpoints"`
	Host         string   `json:"host"`
	Port         int32    `json:"port"`
	Rack         string   `json:"rack"`
	TimestampStr string   `json:"timestamp"`
	Version      int      `json:"version"`
}

type zkBrokerConfig struct {
	Version int               `json:"version"`
	Config  map[string]string `json:"config"`
}

type zkTopicInfo struct {
	Version    int              `json:"version"`
	Partitions map[string][]int `json:"partitions"`
}

type zkTopicConfig struct {
	Version int               `json:"version"`
	Config  map[string]string `json:"config"`
}

type zkPartitionInfo struct {
	Leader          int   `json:"leader"`
	Version         int   `json:"version"`
	ISR             []int `json:"isr"`
	ControllerEpoch int   `json:"controller_epoch"`
	LeaderEpoch     int   `json:"leader_epoch"`
}

type zkAssignment struct {
	Version    int                     `json:"version"`
	Partitions []zkAssignmentPartition `json:"partitions"`
}

type zkAssignmentPartition struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Replicas  []int  `json:"replicas"`
}

type zkElection struct {
	Version    int                        `json:"version"`
	Partitions []zkElectionTopicPartition `json:"partitions"`
}

type zkElectionTopicPartition struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
}

type zkChangeNotification struct {
	Version    int    `json:"version"`
	EntityPath string `json:"entity_path"`
}

type PartitionStatus string

const (
	Ok              PartitionStatus = "OK"
	Offline         PartitionStatus = "Offline"
	UnderReplicated PartitionStatus = "Under-replicated"
)

type PartitionLeaderState string

const (
	CorrectLeader PartitionLeaderState = "OK"
	WrongLeader   PartitionLeaderState = "Wrong"
)

type PartitionStatusInfo struct {
	Topic       string
	Partition   kafka.Partition
	Status      PartitionStatus
	LeaderState PartitionLeaderState
}

const (
	ListenerNotFoundError kafka.Error = 72
)

// Addr returns the address of the current BrokerInfo.
func (b BrokerInfo) Addr() string {
	return fmt.Sprintf("%s:%d", b.Host, b.Port)
}

// IsThrottled determines whether the broker has any throttles in its config.
func (b BrokerInfo) IsThrottled() bool {
	_, leaderOk := b.Config[LeaderThrottledKey]
	_, followerOk := b.Config[FollowerThrottledKey]

	return leaderOk || followerOk
}

// BrokerIDs returns a slice of the IDs of the argument brokers.
func BrokerIDs(brokers []BrokerInfo) []int {
	brokerIDs := []int{}

	for _, broker := range brokers {
		brokerIDs = append(brokerIDs, broker.ID)
	}

	return brokerIDs
}

// ThrottledBrokerIDs returns a slice of the IDs of the subset of
// argument brokers that have throttles on them.
func ThrottledBrokerIDs(brokers []BrokerInfo) []int {
	brokerIDs := []int{}

	for _, broker := range brokers {
		if broker.IsThrottled() {
			brokerIDs = append(brokerIDs, broker.ID)
		}
	}

	return brokerIDs
}

// BrokerRacks returns a mapping of broker ID -> rack.
func BrokerRacks(brokers []BrokerInfo) map[int]string {
	brokerRacks := map[int]string{}

	for _, broker := range brokers {
		brokerRacks[broker.ID] = broker.Rack
	}

	return brokerRacks
}

// BrokersPerRack returns a mapping of rack -> broker IDs.
func BrokersPerRack(brokers []BrokerInfo) map[string][]int {
	brokersPerRack := map[string][]int{}

	for _, broker := range brokers {
		rack := broker.Rack
		brokersPerRack[rack] = append(
			brokersPerRack[rack],
			broker.ID,
		)
	}

	return brokersPerRack
}

// BrokerCountsPerRack returns a mapping of rack -> number of brokers.
func BrokerCountsPerRack(brokers []BrokerInfo) map[string]int {
	brokersPerRack := BrokersPerRack(brokers)
	brokerCountsPerRack := map[string]int{}

	for rack, brokers := range brokersPerRack {
		brokerCountsPerRack[rack] = len(brokers)
	}

	return brokerCountsPerRack
}

// DistinctRacks returns a sorted slice of all the distinct racks in the cluster.
func DistinctRacks(brokers []BrokerInfo) []string {
	brokersPerRack := BrokersPerRack(brokers)

	racks := []string{}
	for rack := range brokersPerRack {
		racks = append(racks, rack)
	}

	sort.Slice(racks, func(a, b int) bool {
		return racks[a] < racks[b]
	})

	return racks
}

// LeadersPerRack returns a mapping of rack -> number of partitions with a leader
// in that rack.
func LeadersPerRack(brokers []BrokerInfo, topic TopicInfo) map[string]int {
	leadersPerRack := map[string]int{}

	brokerRacks := BrokerRacks(brokers)
	for _, partition := range topic.Partitions {
		leaderRack := brokerRacks[partition.Leader]
		leadersPerRack[leaderRack]++
	}

	return leadersPerRack
}

// Retention returns the retention duration implied by a topic config. If
// unset, it returns 0.
func (t TopicInfo) Retention() time.Duration {
	retentionStr, ok := t.Config[RetentionKey]
	if !ok {
		return 0
	}
	retention, err := strconv.ParseInt(retentionStr, 10, 64)
	if err != nil {
		return 0
	}

	return time.Duration(retention) * time.Millisecond
}

// PartitionIDs returns an ordered slice of partition IDs for a topic.
func (t TopicInfo) PartitionIDs() []int {
	ids := []int{}

	for _, partition := range t.Partitions {
		ids = append(ids, partition.ID)
	}

	return ids
}

// MaxReplication returns the maximum number of replicas across all partitions
// in a topic.
func (t TopicInfo) MaxReplication() int {
	maxReplication := 0

	for _, partition := range t.Partitions {
		if len(partition.Replicas) > maxReplication {
			maxReplication = len(partition.Replicas)
		}
	}

	return maxReplication
}

// MaxISR returns the maximum number of in-sync replicas across all partitions
// in a topic.
func (t TopicInfo) MaxISR() int {
	maxISR := 0

	for _, partition := range t.Partitions {
		if len(partition.ISR) > maxISR {
			maxISR = len(partition.ISR)
		}
	}

	return maxISR
}

// RackCounts returns the minimum and maximum distinct rack counts across
// all partitions in a topic.
func (t TopicInfo) RackCounts(brokerRacks map[int]string) (int, int, error) {
	var minRacks, maxRacks int

	for p, partition := range t.Partitions {
		numRacks, err := partition.NumRacks(brokerRacks)
		if err != nil {
			return 0, 0, err
		}

		if p == 0 {
			minRacks = numRacks
			maxRacks = numRacks
		} else {
			if numRacks < minRacks {
				minRacks = numRacks
			}
			if numRacks > maxRacks {
				maxRacks = numRacks
			}
		}
	}

	return minRacks, maxRacks, nil
}

// AllReplicasInSync returns whether all partitions have ISR == replicas
// (ignoring order).
func (t TopicInfo) AllReplicasInSync() bool {
	for _, partition := range t.Partitions {
		if !util.SameElements(partition.Replicas, partition.ISR) {
			return false
		}
	}

	return true
}

// OutOfSyncPartitions returns the partitions for which ISR != replicas
// (ignoring order).
func (t TopicInfo) OutOfSyncPartitions(subset []int) []PartitionInfo {
	outOfSync := []PartitionInfo{}

	subsetMap := map[int]struct{}{}
	for _, id := range subset {
		subsetMap[id] = struct{}{}
	}

	for _, partition := range t.Partitions {
		if _, ok := subsetMap[partition.ID]; len(subset) > 0 && !ok {
			continue
		}

		if !util.SameElements(partition.Replicas, partition.ISR) {
			outOfSync = append(outOfSync, partition)
		}
	}

	return outOfSync
}

// AllLeadersCorrect returns whether leader == replicas[0] for all partitions.
func (t TopicInfo) AllLeadersCorrect() bool {
	for _, partition := range t.Partitions {
		if partition.Leader != partition.Replicas[0] {
			return false
		}
	}

	return true
}

// WrongLeaderPartitions returns the partitions where leader != replicas[0].
func (t TopicInfo) WrongLeaderPartitions(subset []int) []PartitionInfo {
	wrongLeaders := []PartitionInfo{}

	subsetMap := map[int]struct{}{}
	for _, id := range subset {
		subsetMap[id] = struct{}{}
	}

	for _, partition := range t.Partitions {
		if _, ok := subsetMap[partition.ID]; len(subset) > 0 && !ok {
			continue
		}

		if partition.Leader != partition.Replicas[0] {
			wrongLeaders = append(wrongLeaders, partition)
		}
	}

	return wrongLeaders
}

// IsThrottled determines whether the topic has any throttles in its config.
func (t TopicInfo) IsThrottled() bool {
	_, leaderOk := t.Config[LeaderReplicasThrottledKey]
	_, followerOk := t.Config[FollowerReplicasThrottledKey]

	return leaderOk || followerOk
}

// MaxReplication returns the maximum amount of replication across all partitions
// in the argument topics.
func MaxReplication(topics []TopicInfo) int {
	maxReplication := 0
	for _, topic := range topics {
		topicReplication := topic.MaxReplication()
		if topicReplication > maxReplication {
			maxReplication = topicReplication
		}
	}

	return maxReplication
}

// HasLeaders returns whether at least one partition in the argument topics has
// a non-zero leader set. Used for formatting purposes.
func HasLeaders(topics []TopicInfo) bool {
	for _, topic := range topics {
		for _, partition := range topic.Partitions {
			if partition.Leader > 0 {
				return true
			}
		}
	}

	return false
}

// ThrottledTopicNames returns the names of topics in the argument slice that
// have throttles on them.
func ThrottledTopicNames(topics []TopicInfo) []string {
	throttledNames := []string{}

	for _, topic := range topics {
		if topic.IsThrottled() {
			throttledNames = append(throttledNames, topic.Name)
		}
	}

	return throttledNames
}

// Racks returns a slice of all racks for the partition replicas.
func (p PartitionInfo) Racks(brokerRacks map[int]string) ([]string, error) {
	racks := []string{}

	for _, brokerID := range p.Replicas {
		rack, ok := brokerRacks[brokerID]
		if !ok {
			return nil, fmt.Errorf("Unrecognized broker ID: %d", brokerID)
		}

		racks = append(racks, rack)
	}

	return racks, nil
}

// Racks returns a slice of all racks for the partition replicas.
func (p PartitionStatusInfo) Racks(brokerRacks map[int]string) []string {
	racks := []string{}

	for _, replica := range p.Partition.Replicas {
		rack, ok := brokerRacks[replica.ID]
		if ok {
			racks = append(racks, rack)
		}
	}

	return racks
}

// NumRacks returns the number of distinct racks in the partition.
func (p PartitionInfo) NumRacks(brokerRacks map[int]string) (int, error) {
	racksMap := map[string]struct{}{}

	for _, brokerID := range p.Replicas {
		rack, ok := brokerRacks[brokerID]
		if !ok {
			return 0, fmt.Errorf("Unrecognized broker ID: %d", brokerID)
		}

		racksMap[rack] = struct{}{}
	}

	return len(racksMap), nil
}

// ToAssignments converts a topic to a slice of partition assignments.
func (t TopicInfo) ToAssignments() []PartitionAssignment {
	assignments := []PartitionAssignment{}

	for _, partitionInfo := range t.Partitions {
		assignments = append(
			assignments,
			PartitionAssignment{
				ID:       partitionInfo.ID,
				Replicas: util.CopyInts(partitionInfo.Replicas),
			},
		)
	}

	return assignments
}

// PartitionIDs returns the IDs from the argument partitions.
func PartitionIDs(partitions []PartitionInfo) []int {
	ids := []int{}

	for _, partition := range partitions {
		ids = append(ids, partition.ID)
	}

	return ids
}

// Index returns the index of the argument replica, or -1 if it can't
// be found.
func (a PartitionAssignment) Index(replica int) int {
	for v, value := range a.Replicas {
		if value == replica {
			return v
		}
	}

	return -1
}

// Copy returns a deep copy of this PartitionAssignment.
func (a PartitionAssignment) Copy() PartitionAssignment {
	return PartitionAssignment{
		ID:       a.ID,
		Replicas: util.CopyInts(a.Replicas),
	}
}

// CopyAssignments returns a deep copy of the argument PartitionAssignment
// slice.
func CopyAssignments(
	curr []PartitionAssignment,
) []PartitionAssignment {
	copied := []PartitionAssignment{}

	for _, assignment := range curr {
		copied = append(copied, assignment.Copy())
	}

	return copied
}

// DistinctRacks returns a map of the distinct racks in this PartitionAssignment.
func (a PartitionAssignment) DistinctRacks(
	brokerRacks map[int]string,
) map[string]struct{} {
	racksMap := map[string]struct{}{}

	for _, replica := range a.Replicas {
		racksMap[brokerRacks[replica]] = struct{}{}
	}

	return racksMap
}

// CheckAssignments does some basic sanity checks on the assignments
// that are passed into an Assigner or extender so that we can fail early
// if something is obviously wrong.
func CheckAssignments(assignments []PartitionAssignment) error {
	if len(assignments) == 0 {
		return errors.New("Got zero-length slice")
	}

	var minReplicas, maxReplicas int

	for a, assignment := range assignments {
		numReplicas := len(assignment.Replicas)

		if a == 0 {
			minReplicas = numReplicas
			maxReplicas = numReplicas
		} else {
			if numReplicas < minReplicas {
				minReplicas = numReplicas
			}
			if numReplicas > maxReplicas {
				maxReplicas = numReplicas
			}
		}

		if a != assignment.ID {
			return errors.New("Slice elements not in order")
		}

		if hasRepeats(assignment) {
			return fmt.Errorf(
				"Found repeated partition in assignment: %+v",
				assignment,
			)
		}
	}

	if minReplicas != maxReplicas {
		return fmt.Errorf(
			"Partition replicas do not have consistent length (min: %d, max: %d)",
			minReplicas,
			maxReplicas,
		)
	}

	return nil
}

func hasRepeats(assignment PartitionAssignment) bool {
	replicasMap := map[int]struct{}{}

	for _, replica := range assignment.Replicas {
		if _, ok := replicasMap[replica]; ok {
			return true
		}

		replicasMap[replica] = struct{}{}
	}

	return false
}

// ReplicasToAssignments converts a slice of slices to a slice of PartitionAssignments,
// assuming that the argument slices are in partition order. Used for unit tests.
func ReplicasToAssignments(
	replicaSlices [][]int,
) []PartitionAssignment {
	assignments := []PartitionAssignment{}

	for p, replicas := range replicaSlices {
		assignments = append(
			assignments,
			PartitionAssignment{
				ID:       p,
				Replicas: util.CopyInts(replicas),
			},
		)
	}

	return assignments
}

// AssignmentsToReplicas is the inverse of ReplicasToAssignments. Used for unit
// tests.
func AssignmentsToReplicas(assignments []PartitionAssignment) ([][]int, error) {
	replicaSlices := [][]int{}

	for a, assignment := range assignments {
		if a != assignment.ID {
			return nil, errors.New("Assignments are not in order")
		}

		replicaSlices = append(
			replicaSlices,
			assignment.Replicas,
		)
	}

	return replicaSlices, nil
}

// MaxPartitionsPerBroker calculates the number of partitions that each broker may
// need to handle during a migration.
func MaxPartitionsPerBroker(
	allAssignments ...[]PartitionAssignment,
) map[int]int {
	assignmentsPerPartition := map[int][]PartitionAssignment{}

	for _, assignments := range allAssignments {
		for _, assignment := range assignments {
			assignmentsPerPartition[assignment.ID] = append(
				assignmentsPerPartition[assignment.ID],
				assignment,
			)
		}
	}

	partitionsPerBrokerMap := map[int]map[int]struct{}{}

	for partition, assignments := range assignmentsPerPartition {
		partitionsPerBrokerMap[partition] = map[int]struct{}{}
		for _, assignment := range assignments {
			for _, replica := range assignment.Replicas {
				partitionsPerBrokerMap[partition][replica] = struct{}{}
			}
		}
	}

	partitionsPerBroker := map[int]int{}

	for _, brokersMap := range partitionsPerBrokerMap {
		for broker := range brokersMap {
			partitionsPerBroker[broker]++
		}
	}

	return partitionsPerBroker
}

// SameBrokers returns whether two PartitionAssignments have the same brokers.
func SameBrokers(
	a PartitionAssignment,
	b PartitionAssignment,
) bool {
	return util.SameElements(a.Replicas, b.Replicas)
}

// AssignmentDiff represents the diff in a single partition reassignment.
type AssignmentDiff struct {
	PartitionID int
	Old         PartitionAssignment
	New         PartitionAssignment
}

// AssignmentDiffs returns the diffs implied by the argument current and
// desired PartitionAssignments. Used for displaying diffs to user.
func AssignmentDiffs(
	current []PartitionAssignment,
	desired []PartitionAssignment,
) []AssignmentDiff {
	diffsMap := map[int]AssignmentDiff{}

	for _, assignment := range current {
		diffsMap[assignment.ID] = AssignmentDiff{
			PartitionID: assignment.ID,
			Old:         assignment,
		}
	}

	for _, assignment := range desired {
		currDiff, ok := diffsMap[assignment.ID]
		if !ok {
			diffsMap[assignment.ID] = AssignmentDiff{
				PartitionID: assignment.ID,
				New:         assignment,
			}
		} else {
			diffsMap[assignment.ID] = AssignmentDiff{
				PartitionID: assignment.ID,
				Old:         currDiff.Old,
				New:         assignment,
			}
		}
	}

	partitionIDs := []int{}
	for partitionID := range diffsMap {
		partitionIDs = append(partitionIDs, partitionID)
	}
	sort.Slice(partitionIDs, func(a, b int) bool {
		return partitionIDs[a] < partitionIDs[b]
	})

	results := []AssignmentDiff{}

	for _, partitionID := range partitionIDs {
		results = append(results, diffsMap[partitionID])
	}

	return results
}

// AssignmentsToUpdate returns the subset of assignments that need to be
// updated given the current and desired states.
func AssignmentsToUpdate(
	current []PartitionAssignment,
	desired []PartitionAssignment,
) []PartitionAssignment {
	newAssignments := []PartitionAssignment{}
	diffs := AssignmentDiffs(current, desired)

	for _, diff := range diffs {
		if !reflect.DeepEqual(diff.Old, diff.New) {
			newAssignments = append(
				newAssignments,
				PartitionAssignment{
					ID:       diff.PartitionID,
					Replicas: util.CopyInts(diff.New.Replicas),
				},
			)
		}
	}

	return newAssignments
}

// NewLeaderPartitions returns the partition IDs which will have new leaders
// given the current and desired assignments.
func NewLeaderPartitions(
	current []PartitionAssignment,
	desired []PartitionAssignment) []int {
	newLeaderPartitions := []int{}
	diffs := AssignmentDiffs(current, desired)

	for _, diff := range diffs {
		if len(diff.Old.Replicas) > 0 &&
			len(diff.New.Replicas) > 0 &&
			diff.Old.Replicas[0] != diff.New.Replicas[0] {
			newLeaderPartitions = append(newLeaderPartitions, diff.PartitionID)
		}
	}

	return newLeaderPartitions
}

// Check if a string is valid PartitionStatus type
func StringToPartitionStatus(status string) (PartitionStatus, bool) {
	switch strings.ToLower(status) {
	case strings.ToLower(string(Ok)):
		return Ok, true
	case strings.ToLower(string(Offline)):
		return Offline, true
	case strings.ToLower(string(UnderReplicated)):
		return UnderReplicated, true
	default:
		return PartitionStatus(status), false
	}
}

// Set is used by Cobra to set the value of a variable from a Cobra flag.
func (p *PartitionStatus) Set(v string) error {
	ps, ok := StringToPartitionStatus(v)
	if !ok {
		return errors.New("Allowed values: ok, offline, under-replicated")
	}
	*p = ps
	return nil
}

// String is used by Cobra in help text.
func (p *PartitionStatus) String() string {
	return string(*p)
}

// Type is used by Cobra in help text.
func (p *PartitionStatus) Type() string {
	return "PartitionStatus"
}

// Get the partition status info for specified topics
func GetTopicsPartitionsStatusInfo(
	metadata *kafka.MetadataResponse,
	topics []string,
	status PartitionStatus,
) map[string][]PartitionStatusInfo {
	topicsPartitionsStatusInfo := make(map[string][]PartitionStatusInfo)

	filterTopics := GetValidTopicNamesFromMetadata(topics, metadata)
	for _, topicMetadata := range metadata.Topics {
		if topicMetadata.Error != nil {
			log.Errorf("Topic: %s metadata error: %v", topicMetadata.Name, topicMetadata.Error)
			continue
		}

		if len(topics) != 0 && !filterTopics[topicMetadata.Name] {
			continue
		}

		partitionsStatusInfo := []PartitionStatusInfo{}
		for _, partition := range topicMetadata.Partitions {
			partitionStatus := GetPartitionStatus(partition)
			log.Debugf("Topic: %s, Partition: %d status is: %v",
				topicMetadata.Name,
				partition.ID,
				partitionStatus,
			)

			// kafka-go metadata call fetches the partition broker ID as 0 for partitions
			// that are not found
			//
			// i.e
			// - if a replica is missing for a partition, we get broker id as 0
			// - if a isr is missing for a partition, we still get broker id as 0
			// - if a leader is missing for a partition, we still get broker id as 0 (this is offline partition)
			//
			// It can be confusing to kafka-go users since broker IDs can start from 0
			//
			// However, Burrow metadata call fetches missing partitions broker ID as -1
			//
			// For user readability, we will modify
			// - any ISR broker ID that does not have valid Host or Port from 0 to -1
			// - any Replica broker ID that does not have valid Host or Port from 0 to -1
			// - (Offline) Leader Broker ID that does not have a valid Host or Port from 0 to -1
			//
			switch partitionStatus {
			case Ok, UnderReplicated, Offline:
				if partitionStatus != Ok {
					if partitionStatus == Offline {
						partition.Leader.ID = -1
					}

					for i, _ := range partition.Isr {
						if partition.Isr[i].Host == "" && partition.Isr[i].Port == 0 {
							partition.Isr[i].ID = -1
						}
					}

					for i, _ := range partition.Replicas {
						if partition.Replicas[i].Host == "" && partition.Replicas[i].Port == 0 {
							partition.Replicas[i].ID = -1
						}
					}
				}

				leaderState := WrongLeader
				// check if preferred replica leader is the first valid replica ID
				firstReplicaID := -1
				for _, replica := range partition.Replicas {
					if replica.ID == -1 {
						continue
					}

					firstReplicaID = replica.ID
					break
				}

				if len(partition.Replicas) > 0 &&
					partitionStatus != Offline &&
					partition.Leader.ID == firstReplicaID {
					leaderState = CorrectLeader
				}

				if status == PartitionStatus("") || status == partitionStatus {
					partitionsStatusInfo = append(partitionsStatusInfo, PartitionStatusInfo{
						Topic:       topicMetadata.Name,
						Partition:   partition,
						Status:      partitionStatus,
						LeaderState: leaderState,
					})
				}
			default:
				log.Errorf("Unrecognized partition status: %v", partitionStatus)
			}
		}

		if len(partitionsStatusInfo) > 0 {
			topicsPartitionsStatusInfo[topicMetadata.Name] = partitionsStatusInfo
		}
	}

	return topicsPartitionsStatusInfo
}

// Get the partition status summary
func GetTopicsPartitionsStatusSummary(
	metadata *kafka.MetadataResponse,
	topics []string,
	status PartitionStatus,
) (map[string]map[PartitionStatus][]int, int, int, int) {
	okCount := 0
	offlineCount := 0
	underReplicatedCount := 0
	topicsPartitionsStatusSummary := make(map[string]map[PartitionStatus][]int)

	filterTopics := GetValidTopicNamesFromMetadata(topics, metadata)
	for _, topicMetadata := range metadata.Topics {

		if topicMetadata.Error != nil {
			log.Errorf("Topic: %s metadata error: %v", topicMetadata.Name, topicMetadata.Error)
			continue
		}

		if len(topics) != 0 && !filterTopics[topicMetadata.Name] {
			continue
		}

		_, exists := topicsPartitionsStatusSummary[topicMetadata.Name]
		if !exists {
			topicsPartitionsStatusSummary[topicMetadata.Name] = make(map[PartitionStatus][]int)

			if status == "" {
				topicsPartitionsStatusSummary[topicMetadata.Name][Ok] = []int{}
				topicsPartitionsStatusSummary[topicMetadata.Name][UnderReplicated] = []int{}
				topicsPartitionsStatusSummary[topicMetadata.Name][Offline] = []int{}
			} else {
				topicsPartitionsStatusSummary[topicMetadata.Name][status] = []int{}
			}
		}

		for _, partition := range topicMetadata.Partitions {
			partitionStatus := GetPartitionStatus(partition)

			if status == PartitionStatus("") || status == partitionStatus {
				switch partitionStatus {
				case Ok:
					okCount += 1
				case Offline:
					offlineCount += 1
				case UnderReplicated:
					underReplicatedCount += 1
				default:
					// unrecognized partition status
				}

				topicsPartitionsStatusSummary[topicMetadata.Name][partitionStatus] = append(
					topicsPartitionsStatusSummary[topicMetadata.Name][partitionStatus],
					partition.ID,
				)
			}
		}
	}

	return topicsPartitionsStatusSummary, okCount, offlineCount, underReplicatedCount
}

// Get the Partition Status
// - ok
// - offline
// - under-replicated
//
// NOTE: partition is
// 1. offline - if ListenerNotFound Error observed for leader partition
// 2. underreplicated - if number of isrs are lesser than the replicas
func GetPartitionStatus(partition kafka.Partition) PartitionStatus {
	if partition.Leader.Host == "" && partition.Leader.Port == 0 &&
		ListenerNotFoundError.Error() == partition.Error.Error() {
		return Offline
	} else if len(partition.Isr) < len(partition.Replicas) {
		return UnderReplicated
	}

	return Ok
}

// given an input of topics, returns topics that exist in the cluster
func GetValidTopicNamesFromMetadata(
	topics []string,
	metadata *kafka.MetadataResponse,
) map[string]bool {
	validTopics := make(map[string]bool)
	allTopicNamesSet := GetAllTopicNamesFromMetadata(metadata)

	for _, topic := range topics {
		_, exists := allTopicNamesSet[topic]
		if exists {
			validTopics[topic] = true
		} else {
			log.Errorf("Ignoring topic: %s. Not found in the kafka cluster", topic)
		}
	}

	return validTopics
}

func GetAllTopicNamesFromMetadata(
	metadata *kafka.MetadataResponse,
) map[string]bool {
	topicsSet := make(map[string]bool)

	for _, topicMetadata := range metadata.Topics {
		topicsSet[topicMetadata.Name] = true
	}

	return topicsSet
}
