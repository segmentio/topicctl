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
	"github.com/segmentio/topicctl/pkg/util"
	"github.com/segmentio/topicctl/pkg/config"
	log "github.com/sirupsen/logrus"
)

var rebalanceCmd = &cobra.Command{
	Use:     "rebalance",
	Short:   "rebalance all topic configs for a kafka cluster",
	PreRunE: rebalancePreRun,
	RunE:    rebalanceRun,
}

type rebalanceCmdConfig struct {
	brokersToRemove              []int
	brokerThrottleMBsOverride    int
	dryRun                       bool
	partitionBatchSizeOverride   int
	pathPrefix                   string
	autoContinueRebalance        bool
	retentionDropStepDurationStr string
	skipConfirm                  bool
	sleepLoopDuration            time.Duration

	shared sharedOptions

	retentionDropStepDuration time.Duration
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
	rebalanceCmd.Flags().BoolVar(
		&rebalanceConfig.autoContinueRebalance,
		"auto-continue-rebalance",
		false,
		"Continue rebalancing without prompting (WARNING: user discretion advised)",
	)
	rebalanceCmd.Flags().StringVar(
		&rebalanceConfig.retentionDropStepDurationStr,
		"retention-drop-step-duration",
		"",
		"Amount of time to use for retention drop steps",
	)
	rebalanceCmd.Flags().BoolVar(
		&rebalanceConfig.skipConfirm,
		"skip-confirm",
		false,
		"Skip confirmation prompts during apply process",
	)
	rebalanceCmd.Flags().DurationVar(
		&rebalanceConfig.sleepLoopDuration,
		"sleep-loop-duration",
		10*time.Second,
		"Amount of time to wait between partition checks",
	)

	addSharedConfigOnlyFlags(rebalanceCmd, &rebalanceConfig.shared)
	RootCmd.AddCommand(rebalanceCmd)
}

func rebalancePreRun(cmd *cobra.Command, args []string) error {
	if rebalanceConfig.retentionDropStepDurationStr != "" {
		var err error
		rebalanceConfig.retentionDropStepDuration, err = time.ParseDuration(
			rebalanceConfig.retentionDropStepDurationStr,
		)

		if err != nil {
			return err
		}
	}

	// topicctl config layout for a kafka cluster (refer folder: topicctl/examples)
	//   cluster/
	//   cluster/cluster.yaml
	//   cluster/topics/
	//   cluster/topics/topic_1
	//   cluster/topics/topic_2
	//
	// there can be a situation where topics folder is not in cluster dir
	// hence we specify both --cluster-config and --path-prefix for action:rebalance
	// However, we check the path-prefix topic configs with cluster yaml for consistency before applying
	if rebalanceConfig.shared.clusterConfig == "" || rebalanceConfig.pathPrefix == "" {
		return fmt.Errorf("Must set args --cluster-config & --path-prefix (or) env variables TOPICCTL_CLUSTER_CONFIG & TOPICCTL_APPLY_PATH_PREFIX")
	}

	return nil
}

func rebalanceRun(cmd *cobra.Command, args []string) error {
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
	defer func() {
		for _, adminClient := range adminClients {
			adminClient.Close()
		}
	}()

	clusterConfigPath := rebalanceConfig.shared.clusterConfig
	topicConfigDir := rebalanceConfig.pathPrefix
	clusterConfig, err := config.LoadClusterFile(clusterConfigPath, applyConfig.shared.expandEnv)
	if err != nil {
		return err
	}
	log.Debugf("clusterConfig: %+v", clusterConfig)

	adminClient, ok := adminClients[clusterConfigPath]
	if !ok {
		adminClient, err = clusterConfig.NewAdminClient(
			ctx,
			nil,
			applyConfig.dryRun,
			applyConfig.shared.saslUsername,
			applyConfig.shared.saslPassword,
		)
		if err != nil {
			return err
		}
		adminClients[clusterConfigPath] = adminClient
	}

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
			log.Debugf("topicConfig from file: %+v", topicConfig)
			log.Infof(
				"Rebalancing topic %s in config %s with cluster config %s",
				topicConfig.Meta.Name,
				match,
				clusterConfigPath,
			)

			stop := make(chan bool)
			rebalanceMetricConfig := util.RebalanceMetricConfig{
				TopicName: topicConfig.Meta.Name,
				ClusterName: clusterConfig.Meta.Name,
				ClusterEnvironment: clusterConfig.Meta.Environment,
				ToRemove: rebalanceConfig.brokersToRemove,
				RebalanceStatus: "inprogress",
			}
			metricStr, err := util.MetricConfigStr(rebalanceMetricConfig)
			if err != nil {
				log.Errorf("Error: %+v", err)
			}
			go util.PrintMetrics(metricStr, stop)
			topicErrorDict[topicConfig.Meta.Name] = nil
			if err := rebalanceApplyTopic(ctx, topicConfig, clusterConfig, adminClient); err != nil {
				topicErrorDict[topicConfig.Meta.Name] = err
				rebalanceMetricConfig.RebalanceStatus = "error"
				log.Errorf("Ignoring topic %v for rebalance. Got error: %+v", topicConfig.Meta.Name, err)
			} else {
				rebalanceMetricConfig.RebalanceStatus = "success"
			}
			stop <- true
			metricStr, err = util.MetricConfigStr(rebalanceMetricConfig)
			if err != nil {
				log.Errorf("Error: %+v", err)
			}
			log.Infof("Metric: %s", metricStr)
		}
	}

	log.Infof("Rebalance error topics...")
	for thisTopicName, thisTopicError := range topicErrorDict {
		if thisTopicError != nil {
			log.Errorf("topic: %s failed with error: %v", thisTopicName, thisTopicError)
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

	applierConfig := apply.TopicApplierConfig{
		BrokerThrottleMBsOverride:  rebalanceConfig.brokerThrottleMBsOverride,
		BrokersToRemove:            rebalanceConfig.brokersToRemove,
		ClusterConfig:              clusterConfig,
		DryRun:                     rebalanceConfig.dryRun,
		PartitionBatchSizeOverride: rebalanceConfig.partitionBatchSizeOverride,
		Rebalance:                  true, // to enforce action: rebalance
		AutoContinueRebalance:      rebalanceConfig.autoContinueRebalance,
		RetentionDropStepDuration:  rebalanceConfig.retentionDropStepDuration,
		SkipConfirm:                rebalanceConfig.skipConfirm,
		SleepLoopDuration:          rebalanceConfig.sleepLoopDuration,
		TopicConfig:                topicConfig,
	}

	cliRunner := cli.NewCLIRunner(adminClient, log.Infof, false)
	if err := cliRunner.ApplyTopic(ctx, applierConfig); err != nil {
		return err
	}

	return nil
}
