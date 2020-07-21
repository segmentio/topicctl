package subcmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/config"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var validateCmd = &cobra.Command{
	Use:   "validate [topic configs]",
	Short: "validate one or more topic configs",
	Args:  cobra.MinimumNArgs(1),
	RunE:  validateRun,
}

type validateCmdConfig struct {
	clusterConfig string
	pathPrefix    string
}

var validateConfig validateCmdConfig

func init() {
	validateCmd.Flags().StringVar(
		&validateConfig.clusterConfig,
		"cluster-config",
		os.Getenv("TOPICCTL_CLUSTER_CONFIG"),
		"Cluster config path",
	)
	validateCmd.Flags().StringVar(
		&validateConfig.pathPrefix,
		"path-prefix",
		os.Getenv("TOPICCTL_APPLY_PATH_PREFIX"),
		"Prefix for topic config paths",
	)

	RootCmd.AddCommand(validateCmd)
}

func validateRun(cmd *cobra.Command, args []string) error {
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
		if validateConfig.pathPrefix != "" {
			arg = filepath.Join(validateConfig.pathPrefix, arg)
		}

		matches, err := filepath.Glob(arg)
		if err != nil {
			return err
		}

		for _, match := range matches {
			matchCount++
			if err := validateTopic(ctx, match); err != nil {
				return err
			}
		}
	}

	if matchCount == 0 {
		return fmt.Errorf("No topic configs match the provided args (%+v)", args)
	}

	return nil
}

func validateTopic(
	ctx context.Context,
	topicConfigPath string,
) error {
	clusterConfigPath, err := clusterConfigForTopicValidate(topicConfigPath)
	if err != nil {
		return err
	}

	log.Infof(
		"Validating topic in %s with cluster in %s",
		topicConfigPath,
		clusterConfigPath,
	)

	topicConfig, err := config.LoadTopicFile(topicConfigPath)
	if err != nil {
		return err
	}
	topicConfig.SetDefaults()
	if err := topicConfig.Validate(-1); err != nil {
		return fmt.Errorf(
			"Validation error for %s: %+v",
			topicConfigPath,
			err,
		)
	}

	clusterConfig, err := config.LoadClusterFile(clusterConfigPath)
	if err != nil {
		return err
	}
	if err := clusterConfig.Validate(); err != nil {
		return fmt.Errorf(
			"Validation error for %s: %+v",
			clusterConfigPath,
			err,
		)
	}

	if err := config.CheckConsistency(topicConfig, clusterConfig); err != nil {
		return fmt.Errorf(
			"Topic in %s inconsistent with cluster in %s: %+v",
			topicConfigPath,
			clusterConfigPath,
			err,
		)
	}

	return nil
}

func clusterConfigForTopicValidate(topicConfigPath string) (string, error) {
	if validateConfig.clusterConfig != "" {
		return validateConfig.clusterConfig, nil
	}

	return filepath.Abs(
		filepath.Join(
			filepath.Dir(topicConfigPath),
			"..",
			"cluster.yaml",
		),
	)
}
