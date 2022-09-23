package apply

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/hashicorp/go-multierror"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/apply/assigners"
	"github.com/segmentio/topicctl/pkg/apply/extenders"
	"github.com/segmentio/topicctl/pkg/apply/pickers"
	"github.com/segmentio/topicctl/pkg/apply/rebalancers"
	"github.com/segmentio/topicctl/pkg/config"
	"github.com/segmentio/topicctl/pkg/util"
	"github.com/segmentio/topicctl/pkg/zk"
	log "github.com/sirupsen/logrus"
)

// TopicApplierConfig contains the configuration for a TopicApplier struct.
type TopicApplierConfig struct {
	BrokerThrottleMBsOverride  int
	BrokersToRemove            []int
	ClusterConfig              config.ClusterConfig
	DryRun                     bool
	PartitionBatchSizeOverride int
	Rebalance                  bool
	AutoContinueRebalance      bool
	RetentionDropStepDuration  time.Duration
	SkipConfirm                bool
	SleepLoopDuration          time.Duration
	TopicConfig                config.TopicConfig
}

// TopicApplier executes an "apply" run on a topic by comparing the actual
// and desired configurations, and then updating the topic as necessary to
// align the two.
type TopicApplier struct {
	config      TopicApplierConfig
	adminClient admin.Client
	brokers     []admin.BrokerInfo

	// Pull out some fields for easier access
	clusterConfig config.ClusterConfig
	maxBatchSize  int
	throttleBytes int64
	topicConfig   config.TopicConfig
	topicName     string
}

// NewTopicApplier creates and returns a new TopicApplier instance.
func NewTopicApplier(
	ctx context.Context,
	adminClient admin.Client,
	applierConfig TopicApplierConfig,
) (*TopicApplier, error) {
	if !adminClient.GetSupportedFeatures().Applies {
		return nil,
			errors.New(
				"Admin client does not support features needed for apply; please use zk-based client instead.",
			)
	}

	brokers, err := adminClient.GetBrokers(ctx, nil)
	if err != nil {
		return nil, err
	}

	var maxBatchSize int
	if applierConfig.PartitionBatchSizeOverride > 0 {
		maxBatchSize = applierConfig.PartitionBatchSizeOverride
	} else {
		maxBatchSize = applierConfig.TopicConfig.Spec.MigrationConfig.PartitionBatchSize
	}

	// Set throttle from override (if set), then topic migration config (if set), then
	// cluster default (if set), otherwise hard-coded default.
	var throttleBytes int64
	if applierConfig.BrokerThrottleMBsOverride > 0 {
		throttleBytes = int64(applierConfig.BrokerThrottleMBsOverride) * 1000000
	} else if applierConfig.TopicConfig.Spec.MigrationConfig.ThrottleMB > 0 {
		throttleBytes = applierConfig.TopicConfig.Spec.MigrationConfig.ThrottleMB * 1000000
	} else if applierConfig.ClusterConfig.Spec.DefaultThrottleMB > 0 {
		throttleBytes = applierConfig.ClusterConfig.Spec.DefaultThrottleMB * 1000000
	} else {
		// Default to 120MB / sec
		throttleBytes = 120000000
	}

	return &TopicApplier{
		adminClient:   adminClient,
		config:        applierConfig,
		brokers:       brokers,
		clusterConfig: applierConfig.ClusterConfig,
		maxBatchSize:  maxBatchSize,
		throttleBytes: throttleBytes,
		topicConfig:   applierConfig.TopicConfig,
		topicName:     applierConfig.TopicConfig.Meta.Name,
	}, nil
}

// Apply runs a single "apply" run on the configured topic. The general flow is:
//
//  1. Validate configs
//  2. Acquire topic lock
//  3. Check if topic already exists
//  4. If new:
//     a. Create the topic
//     b. Update the placement in accordance with the configured strategy
//  5. If exists:
//     a. Check retention and update if needed
//     b. Check replication factor (can't be updated by topicctl)
//     c. Check partition count and extend if needed
//     d. Check partition placement and update/migrate if needed
//     e. Check partition leaders and update if needed
func (t *TopicApplier) Apply(ctx context.Context) error {
	log.Info("Validating configs...")
	brokerRacks := admin.DistinctRacks(t.brokers)

	if err := t.clusterConfig.Validate(); err != nil {
		return err
	}

	if err := t.topicConfig.Validate(len(brokerRacks)); err != nil {
		return err
	}
	if err := config.CheckConsistency(t.topicConfig, t.clusterConfig); err != nil {
		return err
	}

	log.Info("Checking if topic already exists...")

	topicInfo, err := t.adminClient.GetTopic(ctx, t.topicName, true)
	if err != nil {
		if err == admin.ErrTopicDoesNotExist {
			return t.applyNewTopic(ctx)
		}
		return err
	}

	return t.applyExistingTopic(ctx, topicInfo)
}

func (t *TopicApplier) applyNewTopic(ctx context.Context) error {
	newTopicConfig, err := t.topicConfig.ToNewTopicConfig()
	if err != nil {
		return err
	}

	if t.config.DryRun {
		log.Infof("Would create topic with config %+v", newTopicConfig)
		return nil
	}

	log.Infof(
		"It looks like this topic doesn't already exist. Will create it with this config:\n%s",
		FormatNewTopicConfig(newTopicConfig),
	)

	ok, _ := Confirm("OK to continue?", t.config.SkipConfirm)
	if !ok {
		return errors.New("Stopping because of user response")
	}

	log.Infof("Creating new topic with config %+v", newTopicConfig)

	err = t.adminClient.CreateTopic(
		ctx,
		newTopicConfig,
	)
	if err != nil {
		return err
	}

	// Just do a short sleep to ensure that zk is updated before we check
	if err := interruptableSleep(ctx, t.config.SleepLoopDuration/5); err != nil {
		return err
	}

	if err := t.updatePlacement(ctx, -1, true); err != nil {
		return err
	}

	if err := t.updateLeaders(ctx, -1); err != nil {
		return err
	}

	return nil
}

func (t *TopicApplier) applyExistingTopic(
	ctx context.Context,
	topicInfo admin.TopicInfo,
) error {
	log.Infof("Updating existing topic '%s'", t.topicName)

	if err := t.checkExistingState(ctx, topicInfo); err != nil {
		return err
	}

	if err := t.updateSettings(ctx, topicInfo); err != nil {
		return err
	}

	if err := t.updateReplication(ctx, topicInfo); err != nil {
		return err
	}

	if err := t.updatePartitions(ctx, topicInfo); err != nil {
		return err
	}

	if err := t.updatePlacement(
		ctx,
		t.maxBatchSize,
		false,
	); err != nil {
		return err
	}

	if err := t.updateLeaders(
		ctx,
		-1,
	); err != nil {
		return err
	}

	if t.config.Rebalance {
		if err := t.updateBalance(
			ctx,
			t.maxBatchSize,
		); err != nil {
			return err
		}

		if err := t.updateLeaders(
			ctx,
			-1,
		); err != nil {
			return err
		}
	}

	return nil
}

func (t *TopicApplier) checkExistingState(
	ctx context.Context,
	topicInfo admin.TopicInfo,
) error {
	log.Infof("Checking the existing state of the cluster, topic, and throttles...")

	lockHeld, err := t.clusterLockHeld(ctx)
	if err != nil {
		return err
	}
	if lockHeld {
		log.Warnf("The cluster lock is currently held, partition migrations might be locked")
	}

	outOfSync := topicInfo.OutOfSyncPartitions(nil)

	if len(outOfSync) == 0 {
		log.Info("All replicas are in-sync")

		if lockHeld {
			log.Info("Skipping checks of existing throttles because there might be a migration happening")
			return nil
		}

		if topicInfo.IsThrottled() {
			log.Infof(
				"It looks there are still throttles on the topic (config: %+v)",
				topicInfo.Config,
			)

			if t.config.DryRun {
				log.Infof("Skipping update because dryRun is set to true")
			} else {
				ok, err := Confirm("OK to remove these?", t.config.SkipConfirm)
				if err != nil {
					return err
				} else if !ok {
					log.Info("Skipping removal")
				}
				if err := t.removeThottles(ctx, true, nil); err != nil {
					return err
				}
			}
		}

		throttledBrokers := admin.ThrottledBrokerIDs(t.brokers)

		if len(throttledBrokers) > 0 {
			log.Infof("Found existing throttles on the following brokers: %+v", throttledBrokers)

			allTopics, err := t.adminClient.GetTopics(ctx, nil, false)
			if err != nil {
				return err
			}

			throttledTopics := []string{}

			for _, topic := range allTopics {
				if topic.IsThrottled() {
					throttledTopics = append(throttledTopics, topic.Name)
				}
			}

			if len(throttledTopics) > 0 {
				log.Infof(
					"Found throttles on the following topics: %+v; not removing broker throttles",
					throttledTopics,
				)
			} else {
				log.Info(
					"There are no topics with active throttles and no active runs of topicctl apply, so we can remove the broker throttles",
				)

				if t.config.DryRun {
					log.Infof("Skipping update because dryRun is set to true")
				} else {
					ok, err := Confirm("OK to remove broker throttles?", t.config.SkipConfirm)
					if err != nil {
						return err
					} else if !ok {
						log.Info("Skipping removal")
						return nil
					}
					if err := t.removeThottles(ctx, false, throttledBrokers); err != nil {
						return err
					}
				}
			}
		}
	} else {
		log.Warnf(
			"One or more replicas are not in-sync; there may be an ongoing migration:\n%s",
			admin.FormatTopicPartitions(outOfSync, t.brokers),
		)
	}

	return nil
}

func (t *TopicApplier) updateSettings(
	ctx context.Context,
	topicInfo admin.TopicInfo,
) error {
	log.Infof("Checking topic config settings...")

	topicSettings := t.topicConfig.Spec.Settings.Copy()
	if t.topicConfig.Spec.RetentionMinutes > 0 {
		topicSettings[admin.RetentionKey] = t.topicConfig.Spec.RetentionMinutes * 60000
	}

	diffKeys, missingKeys, err := topicSettings.ConfigMapDiffs(topicInfo.Config)
	if err != nil {
		return err
	}

	var retentionDropStepDuration time.Duration
	if t.config.RetentionDropStepDuration != 0 {
		retentionDropStepDuration = t.config.RetentionDropStepDuration
	} else {
		var err error
		retentionDropStepDuration, err = t.config.ClusterConfig.GetDefaultRetentionDropStepDuration()
		if err != nil {
			return err
		}
	}

	reduced, err := topicSettings.ReduceRetentionDrop(
		topicInfo.Config,
		retentionDropStepDuration,
	)
	if err != nil {
		return err
	}

	if len(diffKeys) > 0 {
		diffsTable, err := FormatSettingsDiff(topicSettings, topicInfo.Config, diffKeys)
		if err != nil {
			return err
		}

		log.Infof(
			"Found %d key(s) with different values:\n%s",
			len(diffKeys),
			diffsTable,
		)

		if reduced {
			log.Infof(
				strings.Join(
					[]string{
						"Note: Retention drop has been reduced to minimize cluster disruption.",
						"Re-run apply afterwards to keep dropping retention to configured value or run with --retention-drop-step-duration=0 to not do gradual step-down.",
					},
					" ",
				),
			)
		}

		if t.config.DryRun {
			log.Infof("Skipping update because dryRun is set to true")
			return nil
		}

		ok, _ := Confirm(
			"OK to update to the new values in the topic config?",
			t.config.SkipConfirm,
		)
		if !ok {
			return errors.New("Stopping because of user response")
		}
		log.Infof("OK, updating")

		configEntries, err := topicSettings.ToConfigEntries(diffKeys)
		if err != nil {
			return err
		}

		_, err = t.adminClient.UpdateTopicConfig(
			ctx,
			t.topicName,
			configEntries,
			true,
		)
		if err != nil {
			return err
		}
	}

	if len(missingKeys) > 0 {
		log.Warnf(
			"Found %d key(s) set in cluster but missing from config:\n%s\nThese will be left as-is.",
			len(missingKeys),
			FormatMissingKeys(topicInfo.Config, missingKeys),
		)
	}

	return nil
}

func (t *TopicApplier) updateReplication(
	ctx context.Context,
	topicInfo admin.TopicInfo,
) error {
	log.Infof("Checking replication...")

	currReplication := topicInfo.MaxISR()
	if currReplication != t.topicConfig.Spec.ReplicationFactor {
		return fmt.Errorf(
			"Replication in topic config (%d) is not equal to observed max ISR (%d); this cannot be resolved by topicctl",
			t.topicConfig.Spec.ReplicationFactor,
			currReplication,
		)
	}

	return nil
}

func (t *TopicApplier) updatePartitions(
	ctx context.Context,
	topicInfo admin.TopicInfo,
) error {
	log.Infof("Checking partition count...")

	currPartitions := len(topicInfo.Partitions)

	if currPartitions > t.topicConfig.Spec.Partitions {
		return fmt.Errorf(
			"Fewer partitions in topic config (%d) than observed (%d); this cannot be resolved by topicctl",
			t.topicConfig.Spec.Partitions,
			currPartitions,
		)
	} else if currPartitions < t.topicConfig.Spec.Partitions {
		lock, path, err := t.acquireClusterLock(ctx)
		if err != nil {
			return err
		}
		if lock != nil {
			defer func() {
				log.Infof("Releasing cluster lock: %s", path)
				lock.Unlock()
			}()
		}

		return t.updatePartitionsHelper(
			ctx,
			t.topicConfig.Spec.PlacementConfig.Strategy,
		)
	}

	return nil
}

func (t *TopicApplier) updatePartitionsHelper(
	ctx context.Context,
	desiredPlacement config.PlacementStrategy,
) error {
	topicInfo, err := t.adminClient.GetTopic(ctx, t.topicName, true)
	if err != nil {
		return err
	}
	currAssignments := topicInfo.ToAssignments()

	extraPartitions := t.topicConfig.Spec.Partitions - len(topicInfo.Partitions)
	log.Infof(
		"Trying to add %d additional partitions consistent with '%s' strategy",
		extraPartitions,
		desiredPlacement,
	)

	picker, err := t.getPicker(ctx)
	if err != nil {
		return err
	}

	var extender extenders.Extender

	switch desiredPlacement {
	case config.PlacementStrategyStatic:
		extender = &extenders.StaticExtender{
			Assignments: admin.ReplicasToAssignments(
				t.topicConfig.Spec.PlacementConfig.StaticAssignments,
			),
		}
	case config.PlacementStrategyInRack:
		extender = extenders.NewBalancedExtender(
			t.brokers,
			true,
			picker,
		)
	case config.PlacementStrategyBalancedLeaders, config.PlacementStrategyAny:
		extender = extenders.NewBalancedExtender(
			t.brokers,
			false,
			picker,
		)
	default:
		return fmt.Errorf("Cannot extend using strategy %s", desiredPlacement)
	}

	desiredAssignments, err := extender.Extend(
		t.topicName,
		currAssignments,
		extraPartitions,
	)
	if err != nil {
		return err
	}

	// Only consider the added partitions
	currAssignments = []admin.PartitionAssignment{}
	desiredAssignments = desiredAssignments[len(desiredAssignments)-extraPartitions:]

	log.Infof(
		"Here are the proposed diffs:\n%s",
		admin.FormatAssignentDiffs(
			currAssignments,
			desiredAssignments,
			t.brokers,
		),
	)

	if t.config.DryRun {
		log.Infof("Skipping update because dryRun is set to true")
		return nil
	}

	ok, _ := Confirm("OK to apply?", t.config.SkipConfirm)
	if !ok {
		return errors.New("Stopping because of user response")
	}

	err = t.updatePartitionsIteration(ctx, currAssignments, desiredAssignments, true)
	if err != nil {
		return err
	}

	topicInfo, err = t.adminClient.GetTopic(ctx, t.topicName, true)
	if err != nil {
		return err
	}

	log.Infof(
		"Update complete; new partition status:\n%s",
		admin.FormatTopicPartitions(topicInfo.Partitions, t.brokers),
	)

	return nil
}

func (t *TopicApplier) updatePlacement(
	ctx context.Context,
	batchSize int,
	newTopic bool,
) error {
	log.Infof("Checking partition placement...")

	desiredPlacement := t.topicConfig.Spec.PlacementConfig.Strategy
	topicInfo, err := t.adminClient.GetTopic(ctx, t.topicName, true)
	if err != nil {
		return err
	}
	currAssignments := topicInfo.ToAssignments()

	result, err := assigners.EvaluateAssignments(
		currAssignments,
		t.brokers,
		t.topicConfig.Spec.PlacementConfig,
	)
	if err != nil {
		return err
	}
	if result {
		log.Infof(
			"Partition placement already satisfies strategy '%s'",
			desiredPlacement,
		)
		return nil
	}

	if !newTopic {
		// New topics don't use throttles, so there's no reason to lock
		lock, path, err := t.acquireClusterLock(ctx)
		if err != nil {
			return err
		}
		if lock != nil {
			defer func() {
				log.Infof("Releasing cluster lock: %s", path)
				lock.Unlock()
			}()
		}
	}

	if batchSize < 0 {
		// Do all partitions at once
		batchSize = len(topicInfo.Partitions)
	}

	switch desiredPlacement {
	case config.PlacementStrategyStatic,
		config.PlacementStrategyStaticInRack,
		config.PlacementStrategyBalancedLeaders:
		return t.updatePlacementHelper(
			ctx,
			desiredPlacement,
			batchSize,
			newTopic,
		)
	case config.PlacementStrategyInRack, config.PlacementStrategyCrossRack:
		// If we want in-rack or cross-rack, first check that the leaders are balanced; we don't
		// block this, but we should at least warn the user before continuing.
		result, err = assigners.EvaluateAssignments(
			currAssignments,
			t.brokers,
			config.TopicPlacementConfig{
				Strategy: config.PlacementStrategyBalancedLeaders,
			},
		)
		if err != nil {
			return err
		}
		if !result {
			log.Infof(
				"Desired strategy is %s, but leaders aren't balanced. It is strongly suggested to do the latter first. "+
					"This can be done using the \"balanced-leaders\" strategy.",
				desiredPlacement,
			)

			ok, _ := Confirm(
				fmt.Sprintf("OK to apply %s despite having unbalanced leaders?", desiredPlacement),
				t.config.SkipConfirm || t.config.DryRun,
			)
			if !ok {
				return errors.New("Stopping because of user response")
			}
		}

		return t.updatePlacementHelper(
			ctx,
			desiredPlacement,
			batchSize,
			newTopic,
		)
	default:
		return fmt.Errorf("Cannot update using strategy %s", desiredPlacement)
	}
}

func (t *TopicApplier) updateBalance(
	ctx context.Context,
	batchSize int,
) error {
	log.Info("Running rebalance...")

	topicInfo, err := t.adminClient.GetTopic(ctx, t.topicName, true)
	if err != nil {
		return err
	}
	currAssignments := topicInfo.ToAssignments()

	// TODO: Make these parameters configurable?
	rebalancer := rebalancers.NewFrequencyRebalancer(
		t.brokers,
		pickers.NewRandomizedPicker(),
		t.topicConfig.Spec.PlacementConfig,
	)
	desiredAssignments, err := rebalancer.Rebalance(
		t.topicName,
		currAssignments,
		t.config.BrokersToRemove,
	)
	if err != nil {
		return err
	}

	assignmentsToUpdate := admin.AssignmentsToUpdate(
		currAssignments,
		desiredAssignments,
	)
	if len(assignmentsToUpdate) == 0 {
		return nil
	}

	return t.updatePlacementRunner(
		ctx,
		currAssignments,
		desiredAssignments,
		batchSize,
		false,
	)
}

func (t *TopicApplier) updatePlacementHelper(
	ctx context.Context,
	desiredPlacement config.PlacementStrategy,
	batchSize int,
	newTopic bool,
) error {
	log.Infof("Trying to get the partitions consistent with '%s'", desiredPlacement)

	var assigner assigners.Assigner

	topicInfo, err := t.adminClient.GetTopic(ctx, t.topicName, true)
	if err != nil {
		return err
	}
	currAssignments := topicInfo.ToAssignments()

	picker, err := t.getPicker(ctx)
	if err != nil {
		return err
	}

	switch desiredPlacement {
	case config.PlacementStrategyBalancedLeaders:
		assigner = assigners.NewBalancedLeaderAssigner(t.brokers, picker)
	case config.PlacementStrategyInRack:
		assigner = assigners.NewSingleRackAssigner(t.brokers, picker)
	case config.PlacementStrategyCrossRack:
		assigner = assigners.NewCrossRackAssigner(t.brokers, picker)
	case config.PlacementStrategyStatic:
		assigner = &assigners.StaticAssigner{
			Assignments: admin.ReplicasToAssignments(
				t.topicConfig.Spec.PlacementConfig.StaticAssignments,
			),
		}
	case config.PlacementStrategyStaticInRack:
		assigner = assigners.NewStaticSingleRackAssigner(
			t.brokers,
			t.topicConfig.Spec.PlacementConfig.StaticRackAssignments,
			picker,
		)
	default:
		return fmt.Errorf("Cannot update using strategy %s", desiredPlacement)
	}

	desiredAssignments, err := assigner.Assign(t.topicName, currAssignments)
	if err != nil {
		return err
	}

	return t.updatePlacementRunner(
		ctx,
		currAssignments,
		desiredAssignments,
		batchSize,
		newTopic,
	)
}

func (t *TopicApplier) updatePlacementRunner(
	ctx context.Context,
	currAssignments []admin.PartitionAssignment,
	desiredAssignments []admin.PartitionAssignment,
	batchSize int,
	newTopic bool,
) error {
	log.Infof(
		"Here are the proposed diffs:\n%s",
		admin.FormatAssignentDiffs(
			currAssignments,
			desiredAssignments,
			t.brokers,
		),
	)

	log.Infof(
		"Here are the number of partitions per broker now, during the migration, and after:\n%s",
		admin.FormatBrokerMaxPartitions(
			currAssignments,
			desiredAssignments,
			t.brokers,
		),
	)

	log.Infof(
		"They will be applied in batches of %d partitions each, with a throttle of %d bytes/sec (%d MB/sec)",
		batchSize,
		t.throttleBytes,
		t.throttleBytes/1000000,
	)

	if t.config.DryRun {
		log.Infof("Skipping update because dryRun is set to true")
		return nil
	}

	if t.config.AutoContinueRebalance {
		log.Warnf("Autocontinue flag detected, user will not be prompted each round")
	}

	ok, _ := Confirm("OK to apply?", t.config.SkipConfirm)
	if !ok {
		return errors.New("Stopping because of user response")
	}

	assignmentsToUpdate := admin.AssignmentsToUpdate(
		currAssignments,
		desiredAssignments,
	)

	currDiffAssignments := []admin.PartitionAssignment{}

	for _, diff := range assignmentsToUpdate {
		currDiffAssignments = append(
			currDiffAssignments,
			currAssignments[diff.ID],
		)
	}

	numRounds := (len(assignmentsToUpdate) + batchSize - 1) / batchSize // Ceil() with integer math
	roundScoreboard := color.New(color.FgYellow, color.Bold).SprintfFunc()
	for i, round := 0, 1; i < len(assignmentsToUpdate); i, round = i+batchSize, round+1 {
		end := i + batchSize

		if end > len(assignmentsToUpdate) {
			end = len(assignmentsToUpdate)
		}

		log.Infof(
			"Balancing round %s",
			roundScoreboard("%d of %d", round, numRounds),
		)

		err := t.updatePartitionsIteration(
			ctx,
			currDiffAssignments[i:end],
			assignmentsToUpdate[i:end],
			newTopic,
		)
		if err != nil {
			return err
		}

		if t.config.AutoContinueRebalance {
			log.Infof("Autocontinuing to next round")
		} else {
			ok, _ := Confirm("OK to continue?", t.config.SkipConfirm)
			if !ok {
				return errors.New("Stopping because of user response")
			}
		}
	}

	topicInfo, err := t.adminClient.GetTopic(ctx, t.topicName, true)
	if err != nil {
		return err
	}

	log.Infof(
		"Update complete; new partition status:\n%s",
		admin.FormatTopicPartitions(topicInfo.Partitions, t.brokers),
	)

	return nil
}

func (t *TopicApplier) updatePartitionsIteration(
	ctx context.Context,
	currAssignments []admin.PartitionAssignment,
	assignmentsToUpdate []admin.PartitionAssignment,
	newTopic bool,
) error {
	idsToUpdate := []int{}
	for _, assignment := range assignmentsToUpdate {
		idsToUpdate = append(idsToUpdate, assignment.ID)
	}

	log.Infof("Starting update iteration for partition(s) %+v", idsToUpdate)

	throttledTopic, throttledBrokers, err := t.applyThrottles(
		ctx,
		currAssignments,
		assignmentsToUpdate,
		newTopic,
	)
	if err != nil {
		return err
	}

	if len(currAssignments) > 0 {
		err = t.adminClient.AssignPartitions(
			ctx,
			t.topicName,
			assignmentsToUpdate,
		)
	} else {
		err = t.adminClient.AddPartitions(
			ctx,
			t.topicName,
			assignmentsToUpdate,
		)
	}

	if err != nil {
		return err
	}

	checkTimer := time.NewTicker(t.config.SleepLoopDuration)
	defer checkTimer.Stop()

	log.Info("Sleeping then entering check loop")

outerLoop:
	for {
		select {
		case <-checkTimer.C:
			log.Info("Checking if all partitions in topic are properly replicated...")

			topicInfo, err := t.adminClient.GetTopic(ctx, t.topicName, true)
			if err != nil {
				return err
			}
			notReady := []admin.PartitionInfo{}

			for _, assignment := range assignmentsToUpdate {
				if assignment.ID >= len(topicInfo.Partitions) {
					log.Infof("New partition %d not visible yet", assignment.ID)
					notReady = append(
						notReady,
						admin.PartitionInfo{
							Topic: t.topicName,
							ID:    assignment.ID,
						},
					)
					continue
				}

				partitionInfo := topicInfo.Partitions[assignment.ID]

				if !util.SameElements(partitionInfo.Replicas, partitionInfo.ISR) {
					log.Infof("Out of sync: %+v, %+v", partitionInfo.Replicas, partitionInfo.ISR)
					notReady = append(notReady, partitionInfo)
					continue
				}
				if !reflect.DeepEqual(partitionInfo.Replicas, assignment.Replicas) {
					log.Infof(
						"Wrong replicas: %+v, %+v",
						partitionInfo.Replicas,
						assignment.Replicas,
					)
					notReady = append(notReady, partitionInfo)
					continue
				}
			}

			if len(notReady) == 0 {
				log.Infof("Partition(s) %+v looks good, continuing", idsToUpdate)
				break outerLoop
			}
			log.Infof(">>> Not ready: %+v", notReady)

			log.Infof(
				"%d/%d partitions have not picked up the update and/or have out-of-sync replicas:\n%s",
				len(notReady),
				len(assignmentsToUpdate),
				admin.FormatTopicPartitions(notReady, t.brokers),
			)
			log.Infof("Sleeping for %s", t.config.SleepLoopDuration.String())
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Only remove throttles if apply was successful
	return t.removeThottles(ctx, throttledTopic, throttledBrokers)
}

func (t *TopicApplier) applyThrottles(
	ctx context.Context,
	currAssignments []admin.PartitionAssignment,
	assignmentsToUpdate []admin.PartitionAssignment,
	newTopic bool,
) (bool, []int, error) {
	if len(currAssignments) == 0 || newTopic {
		// We're just adding new partitions, no need to do any throttling
		return false, []int{}, nil
	}

	leaderThrottles := admin.LeaderPartitionThrottles(
		currAssignments,
		assignmentsToUpdate,
	)
	followerThrottles := admin.FollowerPartitionThrottles(
		currAssignments,
		assignmentsToUpdate,
	)
	brokerThrottles := admin.BrokerThrottles(
		leaderThrottles,
		followerThrottles,
		t.throttleBytes,
	)
	topicConfigEntries := admin.PartitionThrottleConfigEntries(
		leaderThrottles,
		followerThrottles,
	)

	var throttledTopic bool

	if len(topicConfigEntries) > 0 {
		log.Infof("Applying topic throttles: %+v", topicConfigEntries)
		_, err := t.adminClient.UpdateTopicConfig(
			ctx,
			t.topicName,
			topicConfigEntries,
			true,
		)
		if err != nil {
			return throttledTopic, nil, err
		}
		throttledTopic = true
	}

	throttledBrokers := []int{}

	for _, brokerThrottle := range brokerThrottles {
		log.Infof("Applying throttle to broker %d", brokerThrottle.Broker)
		updatedKeys, err := t.adminClient.UpdateBrokerConfig(
			ctx,
			brokerThrottle.Broker,
			brokerThrottle.ConfigEntries(),
			false,
		)
		if err != nil {
			return throttledTopic, throttledBrokers, err
		}

		if len(updatedKeys) > 0 {
			throttledBrokers = append(
				throttledBrokers,
				brokerThrottle.Broker,
			)
		}
	}

	return throttledTopic, throttledBrokers, nil
}

func (t *TopicApplier) removeThottles(
	ctx context.Context,
	throttledTopic bool,
	throttledBrokers []int,
) error {
	var err error

	if throttledTopic {
		// Clear out topic throttles
		log.Info("Removing topic throttles")
		_, topicErr := t.adminClient.UpdateTopicConfig(
			ctx,
			t.topicName,
			[]kafka.ConfigEntry{
				{
					ConfigName:  admin.LeaderReplicasThrottledKey,
					ConfigValue: "",
				},
				{
					ConfigName:  admin.FollowerReplicasThrottledKey,
					ConfigValue: "",
				},
			},
			true,
		)
		if topicErr != nil {
			log.Warnf(
				"Error removing topic throttle: %+v",
				topicErr,
			)
			err = multierror.Append(err, topicErr)
		}
	}

	for _, throttledBroker := range throttledBrokers {
		log.Infof("Removing throttle from broker %d", throttledBroker)
		_, brokerErr := t.adminClient.UpdateBrokerConfig(
			ctx,
			throttledBroker,
			[]kafka.ConfigEntry{
				{
					ConfigName:  admin.LeaderThrottledKey,
					ConfigValue: "",
				},
				{
					ConfigName:  admin.FollowerThrottledKey,
					ConfigValue: "",
				},
			},
			true,
		)
		if brokerErr != nil {
			log.Warnf(
				"Error removing throttle for broker %d: %+v",
				throttledBroker,
				brokerErr,
			)
			err = multierror.Append(err, brokerErr)
		}
	}

	return err
}

func (t *TopicApplier) updateLeaders(
	ctx context.Context,
	batchSize int,
) error {
	log.Infof("Checking leaders...")

	topicInfo, err := t.adminClient.GetTopic(ctx, t.topicName, true)
	if err != nil {
		return err
	}
	wrongLeaders := topicInfo.WrongLeaderPartitions(nil)

	if len(wrongLeaders) > 0 {
		if !topicInfo.AllReplicasInSync() {
			return errors.New("Replicas are not in-sync, please try again later")
		}

		lock, path, err := t.acquireTopicLock(ctx)
		if err != nil {
			return err
		}
		if lock != nil {
			defer func() {
				log.Infof("Releasing topic lock: %s", path)
				lock.Unlock()
			}()
		}

		log.Infof(
			"It looks like the following %d partitions have the wrong leaders:\n%s",
			len(wrongLeaders),
			admin.FormatTopicPartitions(wrongLeaders, t.brokers),
		)

		if t.config.DryRun {
			log.Infof("Skipping update because dryRun is set to true")
			return nil
		}

		if batchSize < 0 {
			// Do all partitions at once
			batchSize = len(wrongLeaders)
		}

		ok, _ := Confirm(
			fmt.Sprintf(
				"OK to run leader elections (in batches of %d partitions each) ?",
				batchSize,
			),
			t.config.SkipConfirm,
		)
		if !ok {
			return errors.New("Stopping because of user response")
		}

		partitionIDs := admin.PartitionIDs(wrongLeaders)

		for i := 0; i < len(partitionIDs); i += batchSize {
			end := i + batchSize

			if end > len(partitionIDs) {
				end = len(partitionIDs)
			}

			err := t.updateLeadersIteration(
				ctx,
				partitionIDs[i:end],
			)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (t *TopicApplier) updateLeadersIteration(
	ctx context.Context,
	electionPartitions []int,
) error {
	log.Infof("Running leader elections for partitions %+v", electionPartitions)

	err := t.adminClient.RunLeaderElection(
		ctx,
		t.topicName,
		electionPartitions,
	)
	if err != nil {
		return err
	}

	checkTimer := time.NewTicker(t.config.SleepLoopDuration)
	defer checkTimer.Stop()

	log.Info("Sleeping then entering check loop")

outerLoop:
	for {
		select {
		case <-checkTimer.C:
			log.Info("Checking if leader has been updated...")

			topicInfo, err := t.adminClient.GetTopic(ctx, t.topicName, true)
			if err != nil {
				return err
			}
			wrongLeaders := topicInfo.WrongLeaderPartitions(electionPartitions)

			if len(wrongLeaders) == 0 {
				log.Infof(
					"Leaders of partitions %+v looks good, continuing",
					electionPartitions,
				)
				break outerLoop
			}
			log.Infof(
				"%d/%d partitions updated have incorrect leaders:\n%s",
				len(wrongLeaders),
				len(electionPartitions),
				admin.FormatTopicPartitions(wrongLeaders, t.brokers),
			)

			log.Infof("Sleeping for %s", t.config.SleepLoopDuration.String())
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (t *TopicApplier) acquireClusterLock(ctx context.Context) (zk.Lock, string, error) {
	if t.config.DryRun || t.clusterConfig.Spec.ZKLockPath == "" {
		return nil, "", nil
	}

	lockPath := t.clusterLockPath()
	log.Infof("Acquiring cluster lock: %s", lockPath)
	lockCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	lock, err := t.adminClient.AcquireLock(lockCtx, lockPath)
	return lock, lockPath, err
}

func (t *TopicApplier) acquireTopicLock(ctx context.Context) (zk.Lock, string, error) {
	if t.config.DryRun || t.clusterConfig.Spec.ZKLockPath == "" {
		return nil, "", nil
	}

	lockPath := t.clusterLockPath()
	log.Infof("Acquiring topic lock: %s", lockPath)
	lockCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	lock, err := t.adminClient.AcquireLock(lockCtx, lockPath)
	return lock, lockPath, err
}

func (t *TopicApplier) clusterLockHeld(ctx context.Context) (bool, error) {
	return t.adminClient.LockHeld(ctx, t.clusterLockPath())
}

func (t *TopicApplier) clusterLockPath() string {
	return filepath.Join(
		t.clusterConfig.Spec.ZKLockPath,
		fmt.Sprintf(
			"%s-%s-%s",
			t.topicConfig.Meta.Cluster,
			t.topicConfig.Meta.Environment,
			t.topicConfig.Meta.Region,
		),
	)
}

func (t *TopicApplier) topicLockPath() string {
	return filepath.Join(
		t.clusterConfig.Spec.ZKLockPath,
		fmt.Sprintf(
			"%s-%s-%s-%s",
			t.topicConfig.Meta.Cluster,
			t.topicConfig.Meta.Environment,
			t.topicConfig.Meta.Region,
			t.topicName,
		),
	)
}

func (t *TopicApplier) getPicker(ctx context.Context) (pickers.Picker, error) {
	var picker pickers.Picker

	switch t.topicConfig.Spec.PlacementConfig.Picker {
	case config.PickerMethodClusterUse:
		log.Info("Getting all topics for ClusterUsePicker")

		topics, err := t.adminClient.GetTopics(ctx, nil, false)
		if err != nil {
			return nil, err
		}

		// Don't include the topic for this applier since the picker already considers
		// broker placement within the topic and, also, the placement might change during
		// the apply process.
		nonAppliedTopics := []admin.TopicInfo{}

		for _, topic := range topics {
			if topic.Name != t.topicName {
				nonAppliedTopics = append(
					nonAppliedTopics,
					topic,
				)
			}
		}

		picker = pickers.NewClusterUsePicker(t.brokers, nonAppliedTopics)
	case config.PickerMethodLowestIndex:
		picker = pickers.NewLowestIndexPicker()
	case config.PickerMethodRandomized:
		picker = pickers.NewRandomizedPicker()
	default:
		return nil, fmt.Errorf(
			"Unrecognized picker method: %s",
			t.topicConfig.Spec.PlacementConfig.Picker,
		)
	}

	return picker, nil
}

func interruptableSleep(ctx context.Context, duration time.Duration) error {
	log.Infof("Sleeping for %s", duration.String())

	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
