package subcmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/apply"
	"github.com/segmentio/topicctl/pkg/cli"
	"github.com/segmentio/topicctl/pkg/config"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var applyUserCmd = &cobra.Command{
	Use:   "apply-user [user configs]",
	Short: "apply one or more user configs",
	Args:  cobra.MinimumNArgs(1),
	RunE:  applyUserRun,
}

func init() {
	applyUserCmd.Flags().BoolVar(
		&applyConfig.dryRun,
		"dry-run",
		false,
		"Do a dry-run",
	)
	applyUserCmd.Flags().StringVar(
		&applyConfig.pathPrefix,
		"path-prefix",
		os.Getenv("TOPICCTL_USER_APPLY_PATH_PREFIX"),
		"Prefix for user config paths",
	)
	applyUserCmd.Flags().BoolVar(
		&applyConfig.skipConfirm,
		"skip-confirm",
		false,
		"Skip confirmation prompts during apply process",
	)

	addSharedConfigOnlyFlags(applyUserCmd, &applyConfig.shared)
	RootCmd.AddCommand(applyUserCmd)
}

func applyUserRun(cmd *cobra.Command, args []string) error {
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
			if err := applyUser(ctx, match, adminClients); err != nil {
				return err
			}
		}
	}

	if matchCount == 0 {
		return fmt.Errorf("No user configs match the provided args (%+v)", args)
	}

	return nil
}

func applyUser(
	ctx context.Context,
	userConfigPath string,
	adminClients map[string]admin.Client,
) error {
	clusterConfigPath, err := clusterConfigForUserApply(userConfigPath)
	if err != nil {
		return err
	}

	userConfigs, err := config.LoadUsersFile(userConfigPath)
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

	for _, userConfig := range userConfigs {
		// userConfig.SetDefaults()
		log.Infof(
			"Processing user %s in config %s with cluster config %s",
			userConfig.Meta.Name,
			userConfigPath,
			clusterConfigPath,
		)

		applierConfig := apply.UserApplierConfig{
			DryRun:        applyConfig.dryRun,
			SkipConfirm:   applyConfig.skipConfirm,
			UserConfig:    userConfig,
			ClusterConfig: clusterConfig,
		}

		if err := cliRunner.ApplyUser(ctx, applierConfig); err != nil {
			return err
		}
	}

	return nil
}

// TODO: move this into a util function shared between this and apply topic
func clusterConfigForUserApply(userConfigPath string) (string, error) {
	if applyConfig.shared.clusterConfig != "" {
		return applyConfig.shared.clusterConfig, nil
	}

	return filepath.Abs(
		filepath.Join(
			filepath.Dir(userConfigPath),
			"..",
			"cluster.yaml",
		),
	)
}
