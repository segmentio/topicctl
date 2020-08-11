package check

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/config"
)

type CheckConfig struct {
	AdminClient   *admin.Client
	BrokerRacks   int
	ClusterConfig config.ClusterConfig
	CheckLeaders  bool
	TopicConfig   config.TopicConfig
}

// CheckTopic runs the topic check and returns a result. If there's a non-topic-specific error
// (e.g., cluster zk isn't reachable), then an error is returned.
func CheckTopic(ctx context.Context, config CheckConfig) (TopicCheckResults, error) {
	results := TopicCheckResults{}

	// Check existence
	results.AppendResult(
		TopicCheckResult{
			Name: CheckNameTopicExists,
		},
	)

	topicInfo, err := config.AdminClient.GetTopic(ctx, config.TopicConfig.Meta.Name, true)
	if err != nil {
		if err == admin.TopicDoesNotExistError {
			results.UpdateLastResult(false, "")
			return results, nil
		} else {
			return results, err
		}
	}
	results.UpdateLastResult(true, "")

	// Check retention
	results.AppendResult(
		TopicCheckResult{
			Name: CheckNameRetentionCorrect,
		},
	)
	if topicInfo.Retention() == time.Duration(config.TopicConfig.Spec.RetentionMinutes)*time.Minute {
		if len(topicInfo.Partitions) == config.TopicConfig.Spec.Partitions {
			results.UpdateLastResult(true, "")
		} else {
			results.UpdateLastResult(
				false,
				fmt.Sprintf(
					"expected %d min, observed %d min",
					config.TopicConfig.Spec.RetentionMinutes,
					int(topicInfo.Retention().Minutes()),
				),
			)
		}
	}

	// Check replication factor
	results.AppendResult(
		TopicCheckResult{
			Name: CheckNameReplicationFactorCorrect,
		},
	)
	replicationFactor := topicInfo.MaxISR()

	if replicationFactor == config.TopicConfig.Spec.ReplicationFactor {
		results.UpdateLastResult(true, "")
	} else {
		results.UpdateLastResult(
			false,
			fmt.Sprintf(
				"expected %d, observed %d",
				config.TopicConfig.Spec.ReplicationFactor,
				replicationFactor,
			),
		)
	}

	// Check partitions
	results.AppendResult(
		TopicCheckResult{
			Name: CheckNamePartitionCountCorrect,
		},
	)
	if len(topicInfo.Partitions) == config.TopicConfig.Spec.Partitions {
		results.UpdateLastResult(true, "")
	} else {
		results.UpdateLastResult(
			false,
			fmt.Sprintf(
				"expected %d, observed %d",
				config.TopicConfig.Spec.Partitions,
				len(topicInfo.Partitions),
			),
		)
	}

	// Check throttles
	results.AppendResult(
		TopicCheckResult{
			Name: CheckNameThrottlesClear,
		},
	)
	if !topicInfo.IsThrottled() {
		results.UpdateLastResult(true, "")
	} else {
		results.UpdateLastResult(
			false,
			"topic has existing throttles",
		)
	}

	// Check replicas in-sync
	results.AppendResult(
		TopicCheckResult{
			Name: CheckNameReplicasInSync,
		},
	)
	outOfSyncPartitions := topicInfo.OutOfSyncPartitions(nil)

	if len(outOfSyncPartitions) == 0 {
		results.UpdateLastResult(true, "")
	} else {
		results.UpdateLastResult(
			false,
			fmt.Sprintf(
				"%d/%d partitions have out-of-sync replicas",
				len(outOfSyncPartitions),
				len(topicInfo.Partitions),
			),
		)
	}

	// Check leaders
	if config.CheckLeaders {
		results.AppendResult(
			TopicCheckResult{
				Name: CheckNameLeadersCorrect,
			},
		)
		wrongLeaderPartitions := topicInfo.WrongLeaderPartitions(nil)

		if len(wrongLeaderPartitions) == 0 {
			results.UpdateLastResult(true, "")
		} else {
			results.UpdateLastResult(
				false,
				fmt.Sprintf(
					"%d/%d partitions have wrong leaders",
					len(wrongLeaderPartitions),
					len(topicInfo.Partitions),
				),
			)
		}
	}

	return results, nil
}
