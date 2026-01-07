package subcmd

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/segmentio/topicctl/pkg/cli"
	"github.com/segmentio/topicctl/pkg/config"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var bootstrapCmd = &cobra.Command{
	Use:   "bootstrap [topics]",
	Short: "bootstrap topic configs from existing topic(s) in a cluster",
	RunE:  bootstrapRun,
}

type bootstrapCmdConfig struct {
	matchRegexp   string
	excludeRegexp string
	outputDir     string
	overwrite     bool
	placementStrategy config.PlacementStrategy

	allowInternalTopics bool

	shared sharedOptions
}

var bootstrapConfig bootstrapCmdConfig

func init() {
	bootstrapCmd.Flags().StringVar(
		&bootstrapConfig.matchRegexp,
		"match",
		".*",
		"Match regexp",
	)
	bootstrapCmd.Flags().StringVar(
		&bootstrapConfig.excludeRegexp,
		"exclude",
		".^",
		"Exclude regexp",
	)
	bootstrapCmd.Flags().StringVarP(
		&bootstrapConfig.outputDir,
		"output",
		"o",
		"",
		"Output directory",
	)
	bootstrapCmd.Flags().BoolVar(
		&bootstrapConfig.overwrite,
		"overwrite",
		false,
		"Overwrite existing configs in output directory",
	)
	bootstrapCmd.Flags().BoolVar(
		&bootstrapConfig.allowInternalTopics,
		"allow-internal-topics",
		false,
		"Include topics that start with __ (typically these are internal topics)",
	)
	bootstrapCmd.Flags().StringVar(
		(*string)(&bootstrapConfig.placementStrategy),
		"placement-strategy",
		"cross-rack",
		"Provide a placementStrategy to overwrite the default value of cross-rack",
	)

	addSharedConfigOnlyFlags(bootstrapCmd, &bootstrapConfig.shared)
	bootstrapCmd.MarkFlagRequired("cluster-config")
	RootCmd.AddCommand(bootstrapCmd)
}

func bootstrapRun(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clusterConfig, err := config.LoadClusterFile(
		bootstrapConfig.shared.clusterConfig,
		bootstrapConfig.shared.expandEnv,
	)
	if err != nil {
		return err
	}
	adminClient, err := clusterConfig.NewAdminClient(
		ctx,
		aws.Config{},
		config.AdminClientOpts{
			ReadOnly:                  true,
			UsernameOverride:          bootstrapConfig.shared.saslUsername,
			PasswordOverride:          bootstrapConfig.shared.saslPassword,
			SecretsManagerArnOverride: bootstrapConfig.shared.saslSecretsManagerArn,
		},
	)
	if err != nil {
		return err
	}

	cliRunner := cli.NewCLIRunner(adminClient, log.Infof, false)
	return cliRunner.BootstrapTopics(
		ctx,
		args,
		clusterConfig,
		bootstrapConfig.matchRegexp,
		bootstrapConfig.excludeRegexp,
		bootstrapConfig.outputDir,
		bootstrapConfig.overwrite,
		bootstrapConfig.allowInternalTopics,
		bootstrapConfig.placementStrategy,
	)
}
