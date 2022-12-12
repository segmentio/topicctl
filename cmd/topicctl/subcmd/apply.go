package subcmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/efcloud/topicctl/pkg/admin"
	"github.com/efcloud/topicctl/pkg/apply"
	"github.com/efcloud/topicctl/pkg/cli"
	"github.com/efcloud/topicctl/pkg/config"
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
	partitionBatchSizeOverride   int
	pathPrefix                   string
	rebalance                    bool
	autoContinueRebalance        bool
	retentionDropStepDurationStr string
	skipConfirm                  bool
	sleepLoopDuration            time.Duration

	shared sharedOptions

	retentionDropStepDuration time.Duration
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
	applyCmd.Flags().DurationVar(
		&applyConfig.sleepLoopDuration,
		"sleep-loop-duration",
		10*time.Second,
		"Amount of time to wait between partition checks",
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
			applyConfig.dryRun,
			applyConfig.shared.saslUsername,
			applyConfig.shared.saslPassword,
		)
		if err != nil {
			return err
		}
		adminClients[clusterConfigPath] = adminClient
	}

	cliRunner := cli.NewCLIRunner(adminClient, log.Infof, false)

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
			PartitionBatchSizeOverride: applyConfig.partitionBatchSizeOverride,
			Rebalance:                  applyConfig.rebalance,
			AutoContinueRebalance:      applyConfig.autoContinueRebalance,
			RetentionDropStepDuration:  applyConfig.retentionDropStepDuration,
			SkipConfirm:                applyConfig.skipConfirm,
			SleepLoopDuration:          applyConfig.sleepLoopDuration,
			TopicConfig:                topicConfig,
		}

		if err := cliRunner.ApplyTopic(ctx, applierConfig); err != nil {
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
