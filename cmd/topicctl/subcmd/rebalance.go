package subcmd

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/apply"
	"github.com/segmentio/topicctl/pkg/cli"
	"github.com/segmentio/topicctl/pkg/config"
	"github.com/segmentio/topicctl/pkg/util"
	log "github.com/sirupsen/logrus"
)

var rebalanceCmd = &cobra.Command{
	Use:     "rebalance",
	Short:   "rebalance all topic configs for a kafka cluster",
	PreRunE: rebalancePreRun,
	RunE:    rebalanceRun,
}

type rebalanceCmdConfig struct {
	brokersToRemove            []int
	brokerThrottleMBsOverride  int
	dryRun                     bool
	partitionBatchSizeOverride int
	pathPrefix                 string
	sleepLoopDuration          time.Duration
	showProgressInterval       time.Duration

	shared sharedOptions
}

var rebalanceConfig rebalanceCmdConfig

func init() {
	rebalanceCmd.Flags().IntSliceVar(
		&rebalanceConfig.brokersToRemove,
		"to-remove",
		[]int{},
		"Brokers to remove; only applies if rebalance is also set",
	)
	rebalanceCmd.Flags().IntVar(
		&rebalanceConfig.brokerThrottleMBsOverride,
		"broker-throttle-mb",
		0,
		"Broker throttle override (MB/sec)",
	)
	rebalanceCmd.Flags().BoolVar(
		&rebalanceConfig.dryRun,
		"dry-run",
		false,
		"Do a dry-run",
	)
	rebalanceCmd.Flags().IntVar(
		&rebalanceConfig.partitionBatchSizeOverride,
		"partition-batch-size",
		0,
		"Partition batch size override",
	)
	rebalanceCmd.Flags().StringVar(
		&rebalanceConfig.pathPrefix,
		"path-prefix",
		os.Getenv("TOPICCTL_APPLY_PATH_PREFIX"),
		"Prefix for topic config paths",
	)
	rebalanceCmd.Flags().DurationVar(
		&rebalanceConfig.sleepLoopDuration,
		"sleep-loop-duration",
		10*time.Second,
		"Amount of time to wait between partition checks",
	)
	rebalanceCmd.Flags().DurationVar(
		&rebalanceConfig.showProgressInterval,
		"show-progress-interval",
		0*time.Second,
		"show progress during rebalance at intervals",
	)

	addSharedConfigOnlyFlags(rebalanceCmd, &rebalanceConfig.shared)
	RootCmd.AddCommand(rebalanceCmd)
}

func rebalancePreRun(cmd *cobra.Command, args []string) error {
	if rebalanceConfig.shared.clusterConfig == "" || rebalanceConfig.pathPrefix == "" {
		return fmt.Errorf("Must set args --cluster-config & --path-prefix (or) env variables TOPICCTL_CLUSTER_CONFIG & TOPICCTL_APPLY_PATH_PREFIX")
	}

	return nil
}

func rebalanceRun(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	rebalanceCtxMap, err := getRebalanceCtxMap(&rebalanceConfig)
	if err != nil {
		return err
	}
	ctx = context.WithValue(ctx, "progress", rebalanceCtxMap)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		// for an interrupt, cancel context and exit program to end all topic rebalances
		<-sigChan
		cancel()
		os.Exit(1)
	}()

	clusterConfigPath := rebalanceConfig.shared.clusterConfig
	topicConfigDir := rebalanceConfig.pathPrefix
	clusterConfig, err := config.LoadClusterFile(clusterConfigPath, rebalanceConfig.shared.expandEnv)
	if err != nil {
		return err
	}

	adminClient, err := clusterConfig.NewAdminClient(ctx,
		nil,
		rebalanceConfig.dryRun,
		rebalanceConfig.shared.saslUsername,
		rebalanceConfig.shared.saslPassword,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer adminClient.Close()

	// get all topic configs from path-prefix i.e topics folder
	// this folder should only have topic configs
	// recursive search is not performed in the path-prefix
	log.Infof("Getting all topic configs from path prefix (from root) %v", topicConfigDir)
	matches, err := filepath.Glob(topicConfigDir + "/**")
	if err != nil {
		return err
	}

	// iterate through each topic config and initiate rebalance
	topicConfigs := []config.TopicConfig{}
	topicErrorDict := make(map[string]error)
	for _, match := range matches {
		topicConfigs, err = config.LoadTopicsFile(match)
		if err != nil {
			return err
		}
		for _, topicConfig := range topicConfigs {
			log.Infof(
				"Rebalancing topic %s in config %s with cluster config %s",
				topicConfig.Meta.Name,
				match,
				clusterConfigPath,
			)

			topicErrorDict[topicConfig.Meta.Name] = nil
			rebalanceTopicProgressConfig := util.RebalanceTopicProgressConfig{
				TopicName:          topicConfig.Meta.Name,
				ClusterName:        clusterConfig.Meta.Name,
				ClusterEnvironment: clusterConfig.Meta.Environment,
				ToRemove:           rebalanceConfig.brokersToRemove,
				RebalanceError:     false,
			}
			if err := rebalanceApplyTopic(ctx, topicConfig, clusterConfig, adminClient); err != nil {
				topicErrorDict[topicConfig.Meta.Name] = err
				rebalanceTopicProgressConfig.RebalanceError = true
				log.Errorf("Ignoring topic %v for rebalance. Got error: %+v", topicConfig.Meta.Name, err)
			}

			// show topic final progress
			if rebalanceCtxMap.Enabled {
				progressStr, err := util.DictToStr(rebalanceTopicProgressConfig)
				if err != nil {
					log.Errorf("Got error: %+v", err)
				} else {
					log.Infof("Progress: %s", progressStr)
				}
			}
		}
	}

	// audit at the end of all topic rebalances
	successTopics := 0
	errorTopics := 0
	for thisTopicName, thisTopicError := range topicErrorDict {
		if thisTopicError != nil {
			errorTopics += 1
			log.Errorf("topic: %s failed with error: %v", thisTopicName, thisTopicError)
		} else {
			successTopics += 1
		}
	}
	log.Infof("Overall rebalance - success topics: %d, error topics: %d", successTopics, errorTopics)
	
	// show overall rebalance progress
	if rebalanceCtxMap.Enabled {
		progressStr, err := util.DictToStr(util.RebalanceProgressConfig{
			SuccessTopics: successTopics,
			ErrorTopics: errorTopics,
			ClusterName:        clusterConfig.Meta.Name,
			ClusterEnvironment: clusterConfig.Meta.Environment,
			ToRemove:           rebalanceConfig.brokersToRemove,
		})
		if err != nil {
			log.Errorf("Got error: %+v", err)
		} else {
			log.Infof("Progress: %s", progressStr)
		}
	}

	return nil
}

// Check whether a topic is a candidate for action rebalance
// - consistency of topic with cluster config
// - settings(partitions, retention time) of topic config with settings for topic in the cluster
func rebalanceTopicCheck(
	topicConfig config.TopicConfig,
	clusterConfig config.ClusterConfig,
	topicInfo admin.TopicInfo,
) error {
	// topic config should be same as the cluster config
	if err := config.CheckConsistency(topicConfig, clusterConfig); err != nil {
		return err
	}

	log.Infof("Check topic partitions...")
	if len(topicInfo.Partitions) != topicConfig.Spec.Partitions {
		return fmt.Errorf("Topic partitions in kafka does not match with topic config file")
	}

	log.Infof("Check topic retention.ms...")
	if topicInfo.Config["retention.ms"] != strconv.Itoa(topicConfig.Spec.RetentionMinutes*60000) {
		return fmt.Errorf("Topic retention in kafka does not match with topic config file")
	}

	return nil
}

// Perform rebalance on a topic. returns error if unsuccessful
// topic will not be rebalanced if
//   - topic config is inconsistent with cluster config (name, region, environment etc...)
//   - partitions of a topic in kafka cluster does not match with topic partition setting in topic config
//   - retention.ms of a topic in kafka cluster does not match with topic retentionMinutes setting in topic config
//
// to ensure there are no disruptions to kafka cluster
//
// NOTE: topic that is not present in kafka cluster will not be applied
func rebalanceApplyTopic(
	ctx context.Context,
	topicConfig config.TopicConfig,
	clusterConfig config.ClusterConfig,
	adminClient admin.Client,
) error {
	topicConfig.SetDefaults()
	topicInfo, err := adminClient.GetTopic(ctx, topicConfig.Meta.Name, true)
	if err != nil {
		if err == admin.ErrTopicDoesNotExist {
			return fmt.Errorf("Topic: %s does not exist in Kafka cluster", topicConfig.Meta.Name)
		}
		return err
	}
	log.Debugf("topicInfo from kafka: %+v", topicInfo)

	if err := rebalanceTopicCheck(topicConfig, clusterConfig, topicInfo); err != nil {
		return err
	}

	retentionDropStepDuration, err := clusterConfig.GetDefaultRetentionDropStepDuration()
	if err != nil {
		return err
	}

	applierConfig := apply.TopicApplierConfig{
		BrokerThrottleMBsOverride:  rebalanceConfig.brokerThrottleMBsOverride,
		BrokersToRemove:            rebalanceConfig.brokersToRemove,
		ClusterConfig:              clusterConfig,
		DryRun:                     rebalanceConfig.dryRun,
		PartitionBatchSizeOverride: rebalanceConfig.partitionBatchSizeOverride,
		Rebalance:                  true,                      // to enforce action: rebalance
		AutoContinueRebalance:      true,                      // to continue without prompts
		RetentionDropStepDuration:  retentionDropStepDuration, // not needed for rebalance
		SkipConfirm:                true,                      // to enforce action: rebalance
		SleepLoopDuration:          rebalanceConfig.sleepLoopDuration,
		TopicConfig:                topicConfig,
	}

	cliRunner := cli.NewCLIRunner(adminClient, log.Infof, false)
	if err := cliRunner.ApplyTopic(ctx, applierConfig); err != nil {
		return err
	}

	return nil
}

// build ctx map for rebalance progress
func getRebalanceCtxMap(rebalanceConfig *rebalanceCmdConfig) (util.RebalanceCtxMap, error) {
	rebalanceCtxMap := util.RebalanceCtxMap{
		Enabled: true,
		Interval: rebalanceConfig.showProgressInterval,
	}

	zeroDur, _ := time.ParseDuration("0s")
	if rebalanceConfig.showProgressInterval == zeroDur {
		rebalanceCtxMap.Enabled = false
		log.Infof("--progress-interval is 0s. Not showing progress...")
	} else if rebalanceConfig.showProgressInterval < zeroDur {
		return rebalanceCtxMap, fmt.Errorf("--show-progress-interval should be > 0s")
	}

	if rebalanceConfig.dryRun {
		rebalanceCtxMap.Enabled = false
		log.Infof("--dry-run enabled. Not showing progress...")
		return rebalanceCtxMap, nil
	}

	return rebalanceCtxMap, nil
}
