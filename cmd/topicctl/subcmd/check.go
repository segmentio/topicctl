package subcmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
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
	checkLeaders bool
	pathPrefix   string
	validateOnly bool

	shared sharedOptions
}

var checkConfig checkCmdConfig

func init() {
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

	addSharedConfigOnlyFlags(checkCmd, &checkConfig.shared)
	RootCmd.AddCommand(checkCmd)
}

func checkRun(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	// Keep a cache of the admin clients with the cluster config path as the key
	adminClients := map[string]admin.Client{}

	defer func() {
		for _, adminClient := range adminClients {
			adminClient.Close()
		}
	}()

	matchCount := 0
	okCount := 0

	for _, arg := range args {
		if checkConfig.pathPrefix != "" && !filepath.IsAbs(arg) {
			arg = filepath.Join(checkConfig.pathPrefix, arg)
		}

		matches, err := filepath.Glob(arg)
		if err != nil {
			return err
		}

		for _, match := range matches {
			matchCount++

			ok, err := checkTopicFile(ctx, match, adminClients)
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

func checkTopicFile(
	ctx context.Context,
	topicConfigPath string,
	adminClients map[string]admin.Client,
) (bool, error) {
	clusterConfigPath, err := clusterConfigForTopicCheck(topicConfigPath)
	if err != nil {
		return false, err
	}

	clusterConfig, err := config.LoadClusterFile(clusterConfigPath, checkConfig.shared.expandEnv)
	if err != nil {
		return false, err
	}

	topicConfigs, err := config.LoadTopicsFile(topicConfigPath)
	if err != nil {
		return false, err
	}

	var adminClient admin.Client

	numRacks := -1

	if !checkConfig.validateOnly {
		var ok bool
		adminClient, ok = adminClients[clusterConfigPath]
		if !ok {
			adminClient, err = clusterConfig.NewAdminClient(
				ctx,
				aws.Config{},
				config.AdminClientOpts{
					ReadOnly:                  true,
					KafkaConnTimeout:          checkConfig.shared.connTimeout,
					UsernameOverride:          checkConfig.shared.saslUsername,
					PasswordOverride:          checkConfig.shared.saslPassword,
					SecretsManagerArnOverride: checkConfig.shared.saslSecretsManagerArn,
				},
			)
			if err != nil {
				return false, err
			}
			adminClients[clusterConfigPath] = adminClient
			numRacks, err = countRacks(ctx, adminClient)
			if err != nil {
				return false, err
			}
		}
	}

	cliRunner := cli.NewCLIRunner(adminClient, log.Infof, false)

	for _, topicConfig := range topicConfigs {
		topicConfig.SetDefaults()
		log.Debugf(
			"Processing topic %s in config %s with cluster config %s",
			topicConfig.Meta.Name,
			topicConfigPath,
			clusterConfigPath,
		)

		topicCheckConfig := check.CheckConfig{
			AdminClient:   adminClient,
			CheckLeaders:  checkConfig.checkLeaders,
			ClusterConfig: clusterConfig,
			NumRacks:      numRacks,
			TopicConfig:   topicConfig,
			ValidateOnly:  checkConfig.validateOnly,
		}
		result, err := cliRunner.CheckTopic(
			ctx,
			topicCheckConfig,
		)
		if !result || err != nil {
			return result, err
		}
	}

	return true, nil
}

func clusterConfigForTopicCheck(topicConfigPath string) (string, error) {
	if checkConfig.shared.clusterConfig != "" {
		return checkConfig.shared.clusterConfig, nil
	}

	return filepath.Abs(
		filepath.Join(
			filepath.Dir(topicConfigPath),
			"..",
			"cluster.yaml",
		),
	)
}

func countRacks(ctx context.Context, c admin.Client) (int, error) {
	ids, err := c.GetBrokerIDs(ctx)
	if err != nil {
		return 0, err
	}
	brokers, err := c.GetBrokers(ctx, ids)
	if err != nil {
		return 0, err
	}
	racks := make(map[string]struct{}, len(brokers))
	for i := range brokers {
		racks[brokers[i].Rack] = struct{}{}
	}
	return len(racks), nil
}
