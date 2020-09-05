package subcmd

import (
	"context"
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
	Use:   "apply [topic configs]",
	Short: "apply one or more topic configs",
	Args:  cobra.MinimumNArgs(1),
	RunE:  applyRun,
}

type applyCmdConfig struct {
	brokersToRemove            []int
	brokerThrottleMBsOverride  int
	clusterConfig              string
	dryRun                     bool
	partitionBatchSizeOverride int
	pathPrefix                 string
	rebalance                  bool
	retentionDropStepDuration  time.Duration
	skipConfirm                bool
	sleepLoopDuration          time.Duration
}

var applyConfig applyCmdConfig

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
	applyCmd.Flags().StringVar(
		&applyConfig.clusterConfig,
		"cluster-config",
		os.Getenv("TOPICCTL_CLUSTER_CONFIG"),
		"Cluster config path",
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
	applyCmd.Flags().DurationVar(
		&applyConfig.retentionDropStepDuration,
		"retention-drop-step-duration",
		10*time.Hour,
		"Amount of time to use for retention drop steps; set to 0 to remove limit",
	)
	applyCmd.Flags().BoolVar(
		&applyConfig.skipConfirm,
		"skip-confirm",
		false,
		"Skip confirmation prompts during apply process",
	)
	applyCmd.Flags().DurationVar(
		&applyConfig.sleepLoopDuration,
		"sleep-loop-duration",
		10*time.Second,
		"Amount of time to wait between partition checks",
	)

	RootCmd.AddCommand(applyCmd)
}

func applyRun(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	// Keep a cache of the admin clients with the cluster config path as the key
	adminClients := map[string]*admin.Client{}

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
				return err
			}
		}
	}

	if matchCount == 0 {
		return fmt.Errorf("No topic configs match the provided args (%+v)", args)
	}

	return nil
}

func applyTopic(
	ctx context.Context,
	topicConfigPath string,
	adminClients map[string]*admin.Client,
) error {
	clusterConfigPath, err := clusterConfigForTopicApply(topicConfigPath)
	if err != nil {
		return err
	}

	log.Infof(
		"Processing topic config %s with cluster config %s",
		topicConfigPath,
		clusterConfigPath,
	)

	topicConfig, err := config.LoadTopicFile(topicConfigPath)
	if err != nil {
		return err
	}
	topicConfig.SetDefaults()

	clusterConfig, err := config.LoadClusterFile(clusterConfigPath)
	if err != nil {
		return err
	}

	adminClient, ok := adminClients[clusterConfigPath]
	if !ok {
		adminClient, err = clusterConfig.NewAdminClient(ctx, nil, applyConfig.dryRun)
		if err != nil {
			return err
		}
		adminClients[clusterConfigPath] = adminClient
	}

	cliRunner := cli.NewCLIRunner(adminClient, log.Infof, false)

	applierConfig := apply.TopicApplierConfig{
		BrokerThrottleMBsOverride:  applyConfig.brokerThrottleMBsOverride,
		BrokersToRemove:            applyConfig.brokersToRemove,
		ClusterConfig:              clusterConfig,
		DryRun:                     applyConfig.dryRun,
		PartitionBatchSizeOverride: applyConfig.partitionBatchSizeOverride,
		Rebalance:                  applyConfig.rebalance,
		RetentionDropStepDuration:  applyConfig.retentionDropStepDuration,
		SkipConfirm:                applyConfig.skipConfirm,
		SleepLoopDuration:          applyConfig.sleepLoopDuration,
		TopicConfig:                topicConfig,
	}

	return cliRunner.ApplyTopic(ctx, applierConfig)
}

func clusterConfigForTopicApply(topicConfigPath string) (string, error) {
	if applyConfig.clusterConfig != "" {
		return applyConfig.clusterConfig, nil
	}

	return filepath.Abs(
		filepath.Join(
			filepath.Dir(topicConfigPath),
			"..",
			"cluster.yaml",
		),
	)
}
