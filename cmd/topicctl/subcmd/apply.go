package subcmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/apply"
	"github.com/segmentio/topicctl/pkg/cli"
	"github.com/segmentio/topicctl/pkg/config"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var applyCmd = &cobra.Command{
	Use:     "apply [topic configs]",
	Short:   "apply one or more topic configs",
	Args:    cobra.MinimumNArgs(1),
	PreRunE: applyPreRun,
	RunE:    applyRun,
}

type applyCmdConfig struct {
	brokersToRemove              []int
	brokerThrottleMBsOverride    int
	dryRun                       bool
	jsonOutput                   bool
	partitionBatchSizeOverride   int
	pathPrefix                   string
	rebalance                    bool
	autoContinueRebalance        bool
	retentionDropStepDurationStr string
	skipConfirm                  bool
	ignoreFewerPartitionsError   bool
	destructive                  bool
	sleepLoopDuration            time.Duration
	failFast                     bool

	shared sharedOptions

	retentionDropStepDuration time.Duration
}

var applyConfig applyCmdConfig

type allChanges struct {
	NewTopics     []apply.NewChangesTracker    `json:"newTopics"`
	UpdatedTopics []apply.UpdateChangesTracker `json:"updatedTopics"`
	DryRun        bool                         `json:"dryRun"`
}

func init() {
	applyCmd.Flags().IntSliceVar(
		&applyConfig.brokersToRemove,
		"to-remove",
		[]int{},
		"Brokers to remove; only applies if rebalance is also set",
	)
	applyCmd.Flags().IntVar(
		&applyConfig.brokerThrottleMBsOverride,
		"broker-throttle-mb",
		0,
		"Broker throttle override (MB/sec)",
	)
	applyCmd.Flags().BoolVar(
		&applyConfig.dryRun,
		"dry-run",
		false,
		"Do a dry-run",
	)
	applyCmd.Flags().IntVar(
		&applyConfig.partitionBatchSizeOverride,
		"partition-batch-size",
		0,
		"Partition batch size override",
	)
	applyCmd.Flags().StringVar(
		&applyConfig.pathPrefix,
		"path-prefix",
		os.Getenv("TOPICCTL_APPLY_PATH_PREFIX"),
		"Prefix for topic config paths",
	)
	applyCmd.Flags().BoolVar(
		&applyConfig.rebalance,
		"rebalance",
		false,
		"Explicitly rebalance broker partition assignments",
	)
	applyCmd.Flags().BoolVar(
		&applyConfig.autoContinueRebalance,
		"auto-continue-rebalance",
		false,
		"Continue rebalancing without prompting (WARNING: user discretion advised)",
	)
	applyCmd.Flags().StringVar(
		&applyConfig.retentionDropStepDurationStr,
		"retention-drop-step-duration",
		"",
		"Amount of time to use for retention drop steps",
	)
	applyCmd.Flags().BoolVar(
		&applyConfig.skipConfirm,
		"skip-confirm",
		false,
		"Skip confirmation prompts during apply process",
	)
	applyCmd.Flags().BoolVar(
		&applyConfig.ignoreFewerPartitionsError,
		"ignore-fewer-partitions-error",
		false,
		"Don't return error when topic's config specifies fewer partitions than it currently has",
	)
	applyCmd.Flags().BoolVar(
		&applyConfig.destructive,
		"destructive",
		false,
		"Deletes topic settings from the broker if the settings are present on the broker but not in the config",
	)
	applyCmd.Flags().DurationVar(
		&applyConfig.sleepLoopDuration,
		"sleep-loop-duration",
		10*time.Second,
		"Amount of time to wait between partition checks",
	)
	applyCmd.Flags().BoolVar(
		&applyConfig.failFast,
		"fail-fast",
		true,
		"Fail upon the first error encountered during apply process",
	)
	applyCmd.Flags().BoolVar(
		&applyConfig.jsonOutput,
		"json-output",
		false,
		"Only logs changes as json objects to stdout",
	)

	addSharedConfigOnlyFlags(applyCmd, &applyConfig.shared)
	RootCmd.AddCommand(applyCmd)
}

func applyPreRun(cmd *cobra.Command, args []string) error {
	if applyConfig.retentionDropStepDurationStr != "" {
		var err error
		applyConfig.retentionDropStepDuration, err = time.ParseDuration(
			applyConfig.retentionDropStepDurationStr,
		)

		if err != nil {
			return err
		}
	}

	return nil
}

func appendError(aggregatedErr error, err error) error {
	if aggregatedErr == nil {
		return err
	}

	return fmt.Errorf("%v\n%v", aggregatedErr, err)
}

func applyRun(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	// Keep a cache of the admin clients with the cluster config path as the key
	adminClients := map[string]admin.Client{}
	// Keep track of any errors that occur during the apply process
	var errs error

	defer func() {
		for _, adminClient := range adminClients {
			adminClient.Close()
		}
	}()

	matchCount := 0

	for _, arg := range args {
		if applyConfig.pathPrefix != "" && !filepath.IsAbs(arg) {
			arg = filepath.Join(applyConfig.pathPrefix, arg)
		}

		matches, err := filepath.Glob(arg)
		if err != nil {
			return err
		}

		for _, match := range matches {
			matchCount++
			if err := applyTopic(ctx, match, adminClients); err != nil {
				if applyConfig.failFast {
					return err
				}
				errs = appendError(errs, err)
			}
		}
	}

	if matchCount == 0 {
		return fmt.Errorf("No topic configs match the provided args (%+v)", args)
	}

	return errs
}

// unpacks a NewOrUpdatedChanges object into allChanges' NewTopics and UpdatedTopics lists
func unpackChanges(currentChange *apply.NewOrUpdatedChanges, changeList allChanges) allChanges {
	// if no changes were made on this run just return
	if currentChange == nil {
		return changeList
	}

	if currentChange.NewChanges != nil {
		changeList.NewTopics = append(changeList.NewTopics, *currentChange.NewChanges)
	}
	if currentChange.UpdateChanges != nil {
		changeList.UpdatedTopics = append(changeList.UpdatedTopics, *currentChange.UpdateChanges)
	}
	return changeList
}

// prints JSON blob of changes being made to stdout
// returns the JSON blob as a map object
func printChanges(changes allChanges) (map[string]interface{}, error) {
	jsonChanges, err := json.Marshal(changes)
	if err != nil {
		return nil, err
	}
	//print json to stdout
	fmt.Printf("%s\n", jsonChanges)
	// return unmarshalled map
	changesMap := make(map[string]interface{})
	err = json.Unmarshal(jsonChanges, &changesMap)
	return changesMap, err
}

func applyTopic(
	ctx context.Context,
	topicConfigPath string,
	adminClients map[string]admin.Client,
) error {
	clusterConfigPath, err := clusterConfigForTopicApply(topicConfigPath)
	if err != nil {
		return err
	}

	topicConfigs, err := config.LoadTopicsFile(topicConfigPath)
	if err != nil {
		return err
	}

	clusterConfig, err := config.LoadClusterFile(clusterConfigPath, applyConfig.shared.expandEnv)
	if err != nil {
		return err
	}

	adminClient, ok := adminClients[clusterConfigPath]
	if !ok {
		adminClient, err = clusterConfig.NewAdminClient(
			ctx,
			nil,
			config.AdminClientOpts{
				ReadOnly:                  applyConfig.dryRun,
				UsernameOverride:          applyConfig.shared.saslUsername,
				PasswordOverride:          applyConfig.shared.saslPassword,
				SecretsManagerArnOverride: applyConfig.shared.saslSecretsManagerArn,
			},
		)
		if err != nil {
			return err
		}
		adminClients[clusterConfigPath] = adminClient
	}

	cliRunner := cli.NewCLIRunner(adminClient, log.Infof, false)

	// initialize changesMap and add dry run flag
	changesToBePrinted := allChanges{
		DryRun: applyConfig.dryRun,
	}

	for _, topicConfig := range topicConfigs {
		topicConfig.SetDefaults()
		log.Infof(
			"Processing topic %s in config %s with cluster config %s",
			topicConfig.Meta.Name,
			topicConfigPath,
			clusterConfigPath,
		)

		applierConfig := apply.TopicApplierConfig{
			BrokerThrottleMBsOverride:  applyConfig.brokerThrottleMBsOverride,
			BrokersToRemove:            applyConfig.brokersToRemove,
			ClusterConfig:              clusterConfig,
			DryRun:                     applyConfig.dryRun,
			JsonOutput:                 applyConfig.jsonOutput,
			PartitionBatchSizeOverride: applyConfig.partitionBatchSizeOverride,
			Rebalance:                  applyConfig.rebalance,
			AutoContinueRebalance:      applyConfig.autoContinueRebalance,
			RetentionDropStepDuration:  applyConfig.retentionDropStepDuration,
			SkipConfirm:                applyConfig.skipConfirm,
			IgnoreFewerPartitionsError: applyConfig.ignoreFewerPartitionsError,
			Destructive:                applyConfig.destructive,
			SleepLoopDuration:          applyConfig.sleepLoopDuration,
			TopicConfig:                topicConfig,
		}
		topicChanges, err := cliRunner.ApplyTopic(ctx, applierConfig)
		if err != nil {
			// if one of the steps after updateSettings errors when updating a topic,
			// we can be in a state where some (but not all) changes were applied
			// some topic creation errors also still create the topic
			changesToBePrinted = unpackChanges(topicChanges, changesToBePrinted)
			if changesToBePrinted.NewTopics != nil || changesToBePrinted.UpdatedTopics != nil {
				log.Error("Error detected while creating or updating a topic, the following changes were still made:")
				partialChanges, printErr := printChanges(changesToBePrinted)
				if printErr != nil {
					log.Error("Error printing partial JSON changes data")
				} else {
					log.Errorf("%#v", partialChanges)
				}
				return err
			}
		}
		changesToBePrinted = unpackChanges(topicChanges, changesToBePrinted)
	}

	// ensure we're not printing empty json if there's no changes to the topic
	if applyConfig.jsonOutput && (changesToBePrinted.NewTopics != nil || changesToBePrinted.UpdatedTopics != nil) {
		if _, err := printChanges(changesToBePrinted); err != nil {
			return err
		}
	}

	return nil
}

func clusterConfigForTopicApply(topicConfigPath string) (string, error) {
	if applyConfig.shared.clusterConfig != "" {
		return applyConfig.shared.clusterConfig, nil
	}

	return filepath.Abs(
		filepath.Join(
			filepath.Dir(topicConfigPath),
			"..",
			"cluster.yaml",
		),
	)
}
