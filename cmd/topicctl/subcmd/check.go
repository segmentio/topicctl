package subcmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/check"
	"github.com/segmentio/topicctl/pkg/cli"
	"github.com/segmentio/topicctl/pkg/config"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var checkCmd = &cobra.Command{
	Use:   "check [topic configs]",
	Short: "check that configs are valid and (optionally) match cluster state",
	RunE:  checkRun,
}

type checkCmdConfig struct {
	clusterConfig string
	checkLeaders  bool
	pathPrefix    string
	validateOnly  bool
}

var checkConfig checkCmdConfig

func init() {
	checkCmd.Flags().StringVar(
		&checkConfig.clusterConfig,
		"cluster-config",
		os.Getenv("TOPICCTL_CLUSTER_CONFIG"),
		"Cluster config",
	)
	checkCmd.Flags().StringVar(
		&checkConfig.pathPrefix,
		"path-prefix",
		os.Getenv("TOPICCTL_APPLY_PATH_PREFIX"),
		"Prefix for topic config paths",
	)
	checkCmd.Flags().BoolVar(
		&checkConfig.checkLeaders,
		"check-leaders",
		false,
		"Check leaders",
	)
	checkCmd.Flags().BoolVar(
		&checkConfig.validateOnly,
		"validate-only",
		false,
		"Validate configs only, without connecting to cluster",
	)

	RootCmd.AddCommand(checkCmd)
}

func checkRun(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	// Keep a cache of the admin clients with the cluster config path as the key
	adminClients := map[string]*admin.Client{}

	defer func() {
		for _, adminClient := range adminClients {
			adminClient.Close()
		}
	}()

	matchCount := 0
	okCount := 0

	for _, arg := range args {
		if checkConfig.pathPrefix != "" {
			arg = filepath.Join(checkConfig.pathPrefix, arg)
		}

		matches, err := filepath.Glob(arg)
		if err != nil {
			return err
		}

		for _, match := range matches {
			matchCount++

			ok, err := checkTopic(ctx, match, adminClients)
			if err != nil {
				return err
			}

			if ok {
				okCount++
			}
		}
	}

	if matchCount == 0 {
		return fmt.Errorf("No topic configs match the provided args (%+v)", args)
	} else if matchCount > okCount {
		return fmt.Errorf(
			"Check failed for %d/%d topic configs",
			matchCount-okCount,
			matchCount,
		)
	}

	return nil
}

func checkTopic(
	ctx context.Context,
	topicConfigPath string,
	adminClients map[string]*admin.Client,
) (bool, error) {
	clusterConfigPath, err := clusterConfigForTopicCheck(topicConfigPath)
	if err != nil {
		return false, err
	}

	log.Debugf(
		"Processing topic config %s with cluster config %s",
		topicConfigPath,
		clusterConfigPath,
	)

	topicConfig, err := config.LoadTopicFile(topicConfigPath)
	if err != nil {
		return false, err
	}
	topicConfig.SetDefaults()

	clusterConfig, err := config.LoadClusterFile(clusterConfigPath)
	if err != nil {
		return false, err
	}

	var adminClient *admin.Client

	if !checkConfig.validateOnly {
		var ok bool
		adminClient, ok = adminClients[clusterConfigPath]
		if !ok {
			adminClient, err = clusterConfig.NewAdminClient(ctx, nil, true)
			if err != nil {
				return false, err
			}
			adminClients[clusterConfigPath] = adminClient
		}
	}

	cliRunner := cli.NewCLIRunner(adminClient, log.Infof, false)
	topicCheckConfig := check.CheckConfig{
		AdminClient:   adminClient,
		CheckLeaders:  checkConfig.checkLeaders,
		ClusterConfig: clusterConfig,
		// TODO: Add support for broker rack verification.
		NumRacks:     -1,
		TopicConfig:  topicConfig,
		ValidateOnly: checkConfig.validateOnly,
	}
	return cliRunner.CheckTopic(
		ctx,
		topicCheckConfig,
	)
}

func clusterConfigForTopicCheck(topicConfigPath string) (string, error) {
	if checkConfig.clusterConfig != "" {
		return checkConfig.clusterConfig, nil
	}

	return filepath.Abs(
		filepath.Join(
			filepath.Dir(topicConfigPath),
			"..",
			"cluster.yaml",
		),
	)
}
