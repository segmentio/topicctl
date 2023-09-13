package subcmd

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/admin"
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
		aclsCmd(),
	)
	RootCmd.AddCommand(getCmd)
}

func getPreRun(cmd *cobra.Command, args []string) error {
	return getConfig.shared.validate()
}

func getCliRunnerAndCtx() (
	context.Context,
	*cli.CLIRunner,
	error,
) {
	ctx := context.Background()
	sess := session.Must(session.NewSession())

	adminClient, err := getConfig.shared.getAdminClient(ctx, sess, true)
	if err != nil {
		return nil, nil, err
	}
	defer adminClient.Close()

	cliRunner := cli.NewCLIRunner(adminClient, log.Infof, !noSpinner)
	return ctx, cliRunner, nil
}

func balanceCmd() *cobra.Command {
	return &cobra.Command{
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
			ctx, cliRunner, err := getCliRunnerAndCtx()
			if err != nil {
				return err
			}

			var topicName string
			if len(args) == 1 {
				topicName = args[0]
			}
			return cliRunner.GetBrokerBalance(ctx, topicName)
		},
		PreRunE: getPreRun,
	}
}

func brokersCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "brokers",
		Short: "Displays descriptions of each broker in the cluster.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cliRunner, err := getCliRunnerAndCtx()
			if err != nil {
				return err
			}
			return cliRunner.GetBrokers(ctx, getConfig.full)
		},
	}
}

func configCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "config [broker or topic]",
		Short: "Displays configuration for the provider broker or topic.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cliRunner, err := getCliRunnerAndCtx()
			if err != nil {
				return err
			}

			return cliRunner.GetConfig(ctx, args[0])
		},
	}
}

func groupsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "groups",
		Short: "Displays consumer group informatin for the cluster.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cliRunner, err := getCliRunnerAndCtx()
			if err != nil {
				return err
			}
			return cliRunner.GetGroups(ctx)
		},
	}
}

func lagsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "lags [topic] [group]",
		Short: "Displays consumer group lag for the specified topic and consumer group.",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cliRunner, err := getCliRunnerAndCtx()
			if err != nil {
				return err
			}
			return cliRunner.GetMemberLags(
				ctx,
				args[0],
				args[1],
				getConfig.full,
				getConfig.sortValues,
			)
		},
	}
}

func membersCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "members [group]",
		Short: "Details of each member in the specified consumer group.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cliRunner, err := getCliRunnerAndCtx()
			if err != nil {
				return err
			}
			return cliRunner.GetGroupMembers(ctx, args[0], getConfig.full)
		},
	}
}

func partitionsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "partitions [topic]",
		Short: "Displays partition information for the specified topic.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cliRunner, err := getCliRunnerAndCtx()
			if err != nil {
				return err
			}
			return cliRunner.GetPartitions(ctx, args[0])
		},
	}
}

func offsetsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "offsets [topic]",
		Short: "Displays offset information for the specified topic along with start and end times.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cliRunner, err := getCliRunnerAndCtx()
			if err != nil {
				return err
			}
			return cliRunner.GetOffsets(ctx, args[0])
		},
	}
}

func topicsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "topics",
		Short: "Displays information for all topics in the cluster.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cliRunner, err := getCliRunnerAndCtx()
			if err != nil {
				return err
			}
			return cliRunner.GetTopics(ctx, getConfig.full)
		},
	}
}

type aclsCmdConfig struct {
	resourceType        admin.ResourceType
	resourcePatternType admin.PatternType
}

var aclsConfig = aclsCmdConfig{
	resourceType:        admin.ResourceType(kafka.ResourceTypeAny),
	resourcePatternType: admin.PatternType(kafka.PatternTypeAny),
}

func aclsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "acls",
		Short: "Displays information for ACLs in the cluster. Supports filtering with flags.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cliRunner, err := getCliRunnerAndCtx()
			if err != nil {
				return err
			}

			filter := kafka.ACLFilter{
				ResourceTypeFilter:        kafka.ResourceType(aclsConfig.resourceType),
				ResourcePatternTypeFilter: kafka.PatternType(aclsConfig.resourcePatternType),
				Operation:                 kafka.ACLOperationTypeAny,
				PermissionType:            kafka.ACLPermissionTypeAny,
			}
			return cliRunner.GetACLs(ctx, filter)
		},
	}
	cmd.Flags().Var(
		&aclsConfig.resourceType,
		"resource-type",
		`Resource type. allowed: "unknown", "any", "topic", "group", "cluster", "transactionalid", "delegationtoken"`,
	)
	cmd.Flags().Var(
		&aclsConfig.resourcePatternType,
		"resource-pattern-type",
		`Resource pattern type. allowed: "unknown", "any", "match", "literal", "prefixed"`,
	)
	return cmd
}
