package subcmd

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/segmentio/topicctl/pkg/cli"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var getCmd = &cobra.Command{
	Use:   "get [resource type]",
	Short: "get instances of a particular type",
	Long: strings.Join(
		[]string{
			"Get instances of a particular type.",
		},
		"\n",
	),
	PersistentPreRunE: getPreRun,
}

type getCmdConfig struct {
	full       bool
	sortValues bool

	shared sharedOptions
}

var getConfig getCmdConfig

func init() {
	getCmd.PersistentFlags().BoolVar(
		&getConfig.full,
		"full",
		false,
		"Show more full information for resources",
	)
	getCmd.PersistentFlags().BoolVar(
		&getConfig.sortValues,
		"sort-values",
		false,
		"Sort by value instead of name; only applies for lags at the moment",
	)
	addSharedFlags(getCmd, &getConfig.shared)
	getCmd.AddCommand(
		balanceCmd(),
		brokersCmd(),
		configCmd(),
		groupsCmd(),
		lagsCmd(),
		membersCmd(),
		partitionsCmd(),
		offsetsCmd(),
		topicsCmd(),
	)
	RootCmd.AddCommand(getCmd)
}

func getPreRun(cmd *cobra.Command, args []string) error {
	return getConfig.shared.validate()
}

func balanceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "balance [optional topic]",
		Short: "Number of replicas per broker position for topic or cluster as a whole",
		Long: strings.Join([]string{
			"Displays the number of replicas per broker position.",
			"Accepts an optional argument of a topic, which will just scope this to that topic. If topic is omitted, the balance displayed will be for the entire cluster.",
		},
			"\n",
		),
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			sess := session.Must(session.NewSession())

			adminClient, err := getConfig.shared.getAdminClient(ctx, sess, true)
			if err != nil {
				return err
			}
			defer adminClient.Close()

			cliRunner := cli.NewCLIRunner(adminClient, log.Infof, !noSpinner)

			var topicName string
			if len(args) == 1 {
				topicName = args[0]
			}
			return cliRunner.GetBrokerBalance(ctx, topicName)
		},
		PreRunE: getPreRun,
	}
	return cmd
}

func brokersCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "brokers",
		Short: "Displays descriptions of each broker in the cluster.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			sess := session.Must(session.NewSession())

			adminClient, err := getConfig.shared.getAdminClient(ctx, sess, true)
			if err != nil {
				return err
			}
			defer adminClient.Close()

			cliRunner := cli.NewCLIRunner(adminClient, log.Infof, !noSpinner)
			return cliRunner.GetBrokers(ctx, getConfig.full)
		},
	}
	return cmd
}

func configCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config [broker or topic]",
		Short: "Displays configuration for the provider broker or topic.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			sess := session.Must(session.NewSession())

			adminClient, err := getConfig.shared.getAdminClient(ctx, sess, true)
			if err != nil {
				return err
			}
			defer adminClient.Close()

			cliRunner := cli.NewCLIRunner(adminClient, log.Infof, !noSpinner)
			return cliRunner.GetConfig(ctx, args[0])
		},
	}
	return cmd
}

func groupsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "groups",
		Short: "Displays consumer group informatin for the cluster.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			sess := session.Must(session.NewSession())

			adminClient, err := getConfig.shared.getAdminClient(ctx, sess, true)
			if err != nil {
				return err
			}
			defer adminClient.Close()

			cliRunner := cli.NewCLIRunner(adminClient, log.Infof, !noSpinner)
			return cliRunner.GetGroups(ctx)
		},
	}
	return cmd
}

func lagsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "lags [topic] [group]",
		Short: "Displays consumer group lag for the specified topic and consumer group.",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			sess := session.Must(session.NewSession())

			adminClient, err := getConfig.shared.getAdminClient(ctx, sess, true)
			if err != nil {
				return err
			}
			defer adminClient.Close()

			cliRunner := cli.NewCLIRunner(adminClient, log.Infof, !noSpinner)
			return cliRunner.GetMemberLags(
				ctx,
				args[0],
				args[1],
				getConfig.full,
				getConfig.sortValues,
			)
		},
	}
	return cmd
}

func membersCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "members [group]",
		Short: "Details of each member in the specified consumer group.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			sess := session.Must(session.NewSession())

			adminClient, err := getConfig.shared.getAdminClient(ctx, sess, true)
			if err != nil {
				return err
			}
			defer adminClient.Close()

			cliRunner := cli.NewCLIRunner(adminClient, log.Infof, !noSpinner)
			return cliRunner.GetGroupMembers(ctx, args[0], getConfig.full)
		},
	}
	return cmd
}

func partitionsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "partitions [topic]",
		Short: "Displays partition information for the specified topic.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			sess := session.Must(session.NewSession())

			adminClient, err := getConfig.shared.getAdminClient(ctx, sess, true)
			if err != nil {
				return err
			}
			defer adminClient.Close()

			cliRunner := cli.NewCLIRunner(adminClient, log.Infof, !noSpinner)
			return cliRunner.GetPartitions(ctx, args[0])
		},
	}
	return cmd
}

func offsetsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "offsets [topic]",
		Short: "Displays offset information for the specified topic along with start and end times.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			sess := session.Must(session.NewSession())

			adminClient, err := getConfig.shared.getAdminClient(ctx, sess, true)
			if err != nil {
				return err
			}
			defer adminClient.Close()

			cliRunner := cli.NewCLIRunner(adminClient, log.Infof, !noSpinner)
			return cliRunner.GetOffsets(ctx, args[0])
		},
	}
	return cmd
}

func topicsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "topics",
		Short: "Displays information for all topics in the cluster.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			sess := session.Must(session.NewSession())

			adminClient, err := getConfig.shared.getAdminClient(ctx, sess, true)
			if err != nil {
				return err
			}
			defer adminClient.Close()

			cliRunner := cli.NewCLIRunner(adminClient, log.Infof, !noSpinner)
			return cliRunner.GetTopics(ctx, getConfig.full)
		},
	}
	return cmd
}
