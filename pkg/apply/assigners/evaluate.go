package assigners

import (
	"fmt"
	"reflect"

	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/config"
)

// EvaluateAssignments determines whether the given assignments are consistent
// with the provided placement strategy.
func EvaluateAssignments(
	assignments []admin.PartitionAssignment,
	brokers []admin.BrokerInfo,
	placementConfig config.TopicPlacementConfig,
) (bool, error) {
	if err := admin.CheckAssignments(assignments); err != nil {
		return false, err
	}

	minRacks, maxRacks, leaderRackCounts := minMaxRacks(assignments, brokers)
	balanced := balancedLeaders(leaderRackCounts)

	switch placementConfig.Strategy {
	case config.PlacementStrategyAny:
		return true, nil
	case config.PlacementStrategyStatic:
		replicas, err := admin.AssignmentsToReplicas(assignments)
		if err != nil {
			return false, err
		}
		return reflect.DeepEqual(
			replicas,
			placementConfig.StaticAssignments,
		), nil
	case config.PlacementStrategyStaticInRack:
		if !(minRacks == 1 && maxRacks == 1) {
			return false, nil
		}
		if len(placementConfig.StaticRackAssignments) != len(assignments) {
			return false, nil
		}

		brokerRacks := admin.BrokerRacks(brokers)

		for a, assignment := range assignments {
			partitionRack := brokerRacks[assignment.Replicas[0]]
			expectedRack := placementConfig.StaticRackAssignments[a]
			if partitionRack != expectedRack {
				return false, nil
			}
		}

		return true, nil
	case config.PlacementStrategyBalancedLeaders:
		return balanced, nil
	case config.PlacementStrategyInRack:
		return minRacks == 1 && maxRacks == 1, nil
	case config.PlacementStrategyCrossRack:
		brokerRacks := admin.BrokerRacks(brokers)
		for _, assignment := range assignments {
			distinctRacks := assignment.DistinctRacks(brokerRacks)
			if len(assignment.Replicas) != len(distinctRacks) {
				return false, nil
			}
		}
		return true, nil
	default:
		return false, fmt.Errorf(
			"Unrecognized placementStrategy: %s",
			placementConfig.Strategy,
		)
	}
}

func balancedLeaders(leaderRackCounts map[string]int) bool {
	var minCount, maxCount int
	first := true

	for _, count := range leaderRackCounts {
		if first {
			minCount = count
			maxCount = count
			first = false
		} else {
			if count < minCount {
				minCount = count
			}
			if count > maxCount {
				maxCount = count
			}
		}
	}

	return minCount == maxCount
}

func minMaxRacks(
	assignments []admin.PartitionAssignment,
	brokers []admin.BrokerInfo,
) (int, int, map[string]int) {
	brokerRacks := admin.BrokerRacks(brokers)
	racks := admin.DistinctRacks(brokers)

	leaderRackCounts := map[string]int{}

	for _, rack := range racks {
		leaderRackCounts[rack] = 0
	}

	var minRacks, maxRacks int

	for a, assignment := range assignments {
		leader := assignment.Replicas[0]
		leaderRackCounts[brokerRacks[leader]]++

		racks := len(assignment.DistinctRacks(brokerRacks))

		if a == 0 {
			minRacks = racks
			maxRacks = racks
		} else {
			if racks < minRacks {
				minRacks = racks
			}
			if racks > maxRacks {
				maxRacks = racks
			}
		}
	}

	return minRacks, maxRacks, leaderRackCounts
}
