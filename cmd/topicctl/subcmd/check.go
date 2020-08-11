package subcmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/cli"
	"github.com/segmentio/topicctl/pkg/config"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var checkCmd = &cobra.Command{
	Use:   "check [topic configs]",
	Short: "check that configs match cluster state",
	RunE:  checkRun,
}

type checkCmdConfig struct {
	clusterConfig string
	matchRegexp   string
	excludeRegexp string
	outputDir     string
	overwrite     bool
}

var checkConfig checkCmdConfig

func init() {
	checkCmd.Flags().StringVar(
		&checkConfig.clusterConfig,
		"cluster-config",
		os.Getenv("TOPICCTL_CLUSTER_CONFIG"),
		"Cluster config",
	)

	RootCmd.AddCommand(checkCmd)
}

func checkRun(cmd *cobra.Command, args []string) error {
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
		if applyConfig.pathPrefix != "" {
			arg = filepath.Join(applyConfig.pathPrefix, arg)
		}

		matches, err := filepath.Glob(arg)
		if err != nil {
			return err
		}

		for _, match := range matches {
			matchCount++
			if err := checkTopic(ctx, match, adminClients); err != nil {
				return err
			}
		}
	}

	if matchCount == 0 {
		return fmt.Errorf("No topic configs match the provided args (%+v)", args)
	}

	return nil
}

func checkTopic(
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
		adminClient, err = clusterConfig.NewAdminClient(ctx, nil, true)
		if err != nil {
			return err
		}
		adminClients[clusterConfigPath] = adminClient
	}

	cliRunner := cli.NewCLIRunner(adminClient, log.Infof, false)
	return cliRunner.CheckTopic(ctx, topicConfig)
}
