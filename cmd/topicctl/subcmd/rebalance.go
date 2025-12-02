package subcmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/spf13/cobra"

	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/apply"
	"github.com/segmentio/topicctl/pkg/cli"
	"github.com/segmentio/topicctl/pkg/config"
	"github.com/segmentio/topicctl/pkg/util"
	log "github.com/sirupsen/logrus"
)

var rebalanceCmd = &cobra.Command{
	Use:     "rebalance",
	Short:   "rebalance all topics for a given topic prefix path",
	PreRunE: rebalancePreRun,
	RunE:    rebalanceRun,
}

type rebalanceCmdConfig struct {
	brokersToRemove            []int
	brokerThrottleMBsOverride  int
	dryRun                     bool
	generateConfig             bool
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
		"Interval of time to show progress during rebalance",
	)
	rebalanceCmd.Flags().BoolVar(
		&rebalanceConfig.generateConfig,
		"generate-config",
		false,
		"Generate temporary config file(s) for the rebalance of configless topic(s)",
	)

	addSharedConfigOnlyFlags(rebalanceCmd, &rebalanceConfig.shared)
	RootCmd.AddCommand(rebalanceCmd)
}

func rebalancePreRun(cmd *cobra.Command, args []string) error {
	if rebalanceConfig.shared.clusterConfig == "" || rebalanceConfig.pathPrefix == "" {
		return fmt.Errorf("Requires args --cluster-config & --path-prefix (or) env variables TOPICCTL_CLUSTER_CONFIG & TOPICCTL_APPLY_PATH_PREFIX")
	}

	return nil
}

func rebalanceRun(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	rebalanceCtxStruct, err := getRebalanceCtxStruct(&rebalanceConfig)
	if err != nil {
		return err
	}
	ctx = context.WithValue(ctx, "progress", rebalanceCtxStruct)
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
		aws.Config{},
		config.AdminClientOpts{
			ReadOnly:                  rebalanceConfig.dryRun,
			UsernameOverride:          rebalanceConfig.shared.saslUsername,
			PasswordOverride:          rebalanceConfig.shared.saslPassword,
			SecretsManagerArnOverride: rebalanceConfig.shared.saslSecretsManagerArn,
		},
	)
	if err != nil {
		log.Fatal(err)
	}
	defer adminClient.Close()

	// get all topic configs from --path-prefix i.e topics folder
	// we perform a recursive on the --path-prefix because there can be nested directories with
	// more topics for the --cluster-config
	//
	// NOTE: a topic file is ignored for rebalance if
	// - a file is not a valid topic yaml file
	// - any topic config is not consistent with cluster config
	log.Infof("Getting all topic configs from path prefix %v", topicConfigDir)
	topicFiles, err := getAllFiles(topicConfigDir)
	if err != nil {
		return err
	}

	tmpDir := ""
	existingConfigFiles := make(map[string]struct{})
	if rebalanceConfig.generateConfig {
		// make set of existing files
		err := processTopicFiles(topicFiles, func(topicConfig config.TopicConfig, topicFile string) error {
			existingConfigFiles[topicConfig.Meta.Name] = struct{}{}
			return nil
		})
		if err != nil {
			return err
		}

		// create temp dir
		tmpDir = rebalanceConfig.pathPrefix
		if rebalanceConfig.pathPrefix[len(rebalanceConfig.pathPrefix) - 1] != '/' {
			tmpDir += "/"
		}
		tmpDir += "tmp/"
		err = os.Mkdir(tmpDir, 0755)
		if err != nil {
			return err
		}
		log.Infof("tmp output path: %v", tmpDir)

		// generate (bootstrap) config files
		cliRunner := cli.NewCLIRunner(adminClient, log.Infof, false)
		cliRunner.BootstrapTopics(
			ctx,
			[]string{},
			clusterConfig,
			".*",
			".^",
			tmpDir,
			false,
			false,
		)

		// re-invetory config files to take into account newly generated ones
		topicFiles, err = getAllFiles(topicConfigDir)
		if err != nil {
			return err
		}
	}

	// iterate through each topic config and initiate rebalance
	topicConfigs := []config.TopicConfig{}
	topicErrorDict := make(map[string]error)
	for _, topicFile := range topicFiles {
		// do not consider invalid topic yaml files for rebalance
		topicConfigs, err = config.LoadTopicsFile(topicFile)
		if err != nil {
			log.Errorf("Invalid topic yaml file: %s", topicFile)
			continue
		}

		for _, topicConfig := range topicConfigs {
			// topic config should be consistent with the cluster config
			if err := config.CheckConsistency(topicConfig.Meta, clusterConfig); err != nil {
				log.Errorf("topic file: %s inconsistent with cluster: %s", topicFile, clusterConfigPath)
				continue
			}

			log.Infof(
				"Rebalancing topic %s from config file %s with cluster config %s",
				topicConfig.Meta.Name,
				topicFile,
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
				log.Errorf("topic: %s rebalance failed with error: %v", topicConfig.Meta.Name, err)
			}

			// show topic final progress
			if rebalanceCtxStruct.Enabled {
				progressStr, err := util.StructToStr(rebalanceTopicProgressConfig)
				if err != nil {
					log.Errorf("progress struct to string error: %+v", err)
				} else {
					log.Infof("Rebalance Progress: %s", progressStr)
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
			log.Errorf("topic: %s rebalance failed with error: %v", thisTopicName, thisTopicError)
		} else {
			log.Infof("topic: %s rebalance is successful", thisTopicName)
			successTopics += 1
		}
	}

	// show overall rebalance summary report
	if rebalanceCtxStruct.Enabled {
		progressStr, err := util.StructToStr(util.RebalanceProgressConfig{
			SuccessTopics:      successTopics,
			ErrorTopics:        errorTopics,
			ClusterName:        clusterConfig.Meta.Name,
			ClusterEnvironment: clusterConfig.Meta.Environment,
			ToRemove:           rebalanceConfig.brokersToRemove,
		})
		if err != nil {
			log.Errorf("progress struct to string error: %+v", err)
		} else {
			log.Infof("Rebalance Progress: %s", progressStr)
		}
	}

	// clean up any generated config files
	if rebalanceConfig.generateConfig {
		err = os.RemoveAll(tmpDir)
		if err != nil {
			return err
		}
	}

	log.Infof("Rebalance complete! %d topics rebalanced successfully, %d topics had errors", successTopics, errorTopics)
	return nil
}

// Check whether a topic is a candidate for action rebalance
// settings(partitions, retention time) of topic config with settings for topic in the cluster
func rebalanceTopicCheck(
	topicConfig config.TopicConfig,
	topicInfo admin.TopicInfo,
) error {
	log.Debugf("Check topic partitions...")
	if len(topicInfo.Partitions) != topicConfig.Spec.Partitions {
		return fmt.Errorf("Topic partitions in kafka: %d does not match with topic config: %d",
			len(topicInfo.Partitions),
			topicConfig.Spec.Partitions,
		)
	}

	log.Debugf("Check topic retention.ms...")
	topicInfoRetentionMs := topicInfo.Config["retention.ms"]
	topicConfigRetentionMs := strconv.Itoa(topicConfig.Spec.RetentionMinutes * 60000)
	if topicInfoRetentionMs == "" {
		topicInfoRetentionMs = strconv.Itoa(0)
	}
	if topicInfoRetentionMs != topicConfigRetentionMs {
		return fmt.Errorf("Topic retention in kafka: %s does not match with topic config: %s",
			topicInfoRetentionMs,
			topicConfigRetentionMs,
		)
	}

	return nil
}

// Perform rebalance on a topic. returns error if unsuccessful
// topic will not be rebalanced if
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

	if err := rebalanceTopicCheck(topicConfig, topicInfo); err != nil {
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
func getRebalanceCtxStruct(rebalanceConfig *rebalanceCmdConfig) (util.RebalanceCtxStruct, error) {
	rebalanceCtxStruct := util.RebalanceCtxStruct{
		Enabled:  true,
		Interval: rebalanceConfig.showProgressInterval,
	}

	zeroDur, _ := time.ParseDuration("0s")
	if rebalanceConfig.showProgressInterval == zeroDur {
		rebalanceCtxStruct.Enabled = false
		log.Infof("--progress-interval is 0s. Not showing progress...")
	} else if rebalanceConfig.showProgressInterval < zeroDur {
		return rebalanceCtxStruct, fmt.Errorf("--show-progress-interval should be > 0s")
	}

	if rebalanceConfig.dryRun {
		rebalanceCtxStruct.Enabled = false
		log.Infof("--dry-run enabled. Not showing progress...")
		return rebalanceCtxStruct, nil
	}

	return rebalanceCtxStruct, nil
}

// get all files for a given dir path
func getAllFiles(dir string) ([]string, error) {
	var files []string

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			files = append(files, path)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return files, err
}

func processTopicFiles(topicFiles []string, operation func(topicConfig config.TopicConfig, topicFile string) error) error {
	for _, topicFile := range topicFiles {
		// do not consider invalid topic yaml files for rebalance
		topicConfigs, err := config.LoadTopicsFile(topicFile)
		if err != nil {
			log.Errorf("Invalid topic yaml file: %s", topicFile)
			continue
		}

		for _, topicConfig := range topicConfigs {
			err := operation(topicConfig, topicFile)
			if err != nil {
				return fmt.Errorf("error during operationg on config %d (%s): %w", 0, topicConfig.Meta.Name, err)
			}
		}
	}
	return nil
}

func topicConfigExists(topicFilepath string, existingFiles map[string]struct{}, name string) bool {
	configPath, _ := filepath.Split(topicFilepath)
	if strings.HasSuffix(configPath, "tmp/") {
		_, found := existingFiles[name]
		if found {
			return true
		}
	}
	return false
}
