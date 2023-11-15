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
	"github.com/segmentio/topicctl/pkg/create"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var createCmd = &cobra.Command{
	Use:               "create [resource type]",
	Short:             "creates one or more resources",
	PersistentPreRunE: createPreRun,
}

type createCmdConfig struct {
	dryRun      bool
	pathPrefix  string
	skipConfirm bool

	shared sharedOptions
}

var createConfig createCmdConfig

func init() {
	createCmd.Flags().BoolVar(
		&createConfig.dryRun,
		"dry-run",
		false,
		"Do a dry-run",
	)
	createCmd.Flags().StringVar(
		&createConfig.pathPrefix,
		"path-prefix",
		os.Getenv("TOPICCTL_ACL_PATH_PREFIX"),
		"Prefix for ACL config paths",
	)
	createCmd.Flags().BoolVar(
		&createConfig.skipConfirm,
		"skip-confirm",
		false,
		"Skip confirmation prompts during creation process",
	)

	addSharedFlags(createCmd, &createConfig.shared)
	createCmd.AddCommand(
		createACLsCmd(),
	)
	RootCmd.AddCommand(createCmd)
}

func createPreRun(cmd *cobra.Command, args []string) error {
	if err := RootCmd.PersistentPreRunE(cmd, args); err != nil {
		return err
	}
	return createConfig.shared.validate()
}

func createACLsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "acls [acl configs]",
		Short:   "creates ACLs from configuration files",
		Args:    cobra.MinimumNArgs(1),
		RunE:    createACLRun,
		PreRunE: createPreRun,
	}

	return cmd
}

func createACLRun(cmd *cobra.Command, args []string) error {
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
		if createConfig.pathPrefix != "" && !filepath.IsAbs(arg) {
			arg = filepath.Join(createConfig.pathPrefix, arg)
		}

		matches, err := filepath.Glob(arg)
		if err != nil {
			return err
		}

		for _, match := range matches {
			matchCount++
			if err := createACL(ctx, match, adminClients); err != nil {
				return err
			}
		}
	}

	if matchCount == 0 {
		return fmt.Errorf("No ACL configs match the provided args (%+v)", args)
	}

	return nil
}

func createACL(
	ctx context.Context,
	aclConfigPath string,
	adminClients map[string]admin.Client,
) error {
	clusterConfigPath, err := clusterConfigForACLCreate(aclConfigPath)
	if err != nil {
		return err
	}

	aclConfigs, err := config.LoadACLsFile(aclConfigPath)
	if err != nil {
		return err
	}

	clusterConfig, err := config.LoadClusterFile(clusterConfigPath, createConfig.shared.expandEnv)
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

	for _, aclConfig := range aclConfigs {
		log.Infof(
			"Processing ACL %s in config %s with cluster config %s",
			aclConfig.Meta.Name,
			aclConfigPath,
			clusterConfigPath,
		)

		creatorConfig := create.ACLCreatorConfig{
			DryRun:        applyConfig.dryRun,
			SkipConfirm:   applyConfig.skipConfirm,
			ACLConfig:     aclConfig,
			ClusterConfig: clusterConfig,
		}

		if err := cliRunner.CreateACL(ctx, creatorConfig); err != nil {
			return err
		}
	}

	return nil
}

func clusterConfigForACLCreate(aclConfigPath string) (string, error) {
	if createConfig.shared.clusterConfig != "" {
		return createConfig.shared.clusterConfig, nil
	}

	return filepath.Abs(
		filepath.Join(
			filepath.Dir(aclConfigPath),
			"..",
			"cluster.yaml",
		),
	)
}
