package admin

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/segmentio/topicctl/pkg/util"
)

const (
	// ZK retention key
	RetentionKey = "retention.ms"

	// ZK config throttle keys
	LeaderThrottledKey           = "leader.replication.throttled.rate"
	FollowerThrottledKey         = "follower.replication.throttled.rate"
	LeaderReplicasThrottledKey   = "leader.replication.throttled.replicas"
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

// Brokers per rack returns a mapping of rack -> broker IDs.
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
