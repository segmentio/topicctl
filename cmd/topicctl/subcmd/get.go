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

>>>>>>> master
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
	hostFilter          string
	operationType       admin.ACLOperationType
	permissionType      admin.ACLPermissionType
	principalFilter     string
	resourceNameFilter  string
	resourcePatternType admin.PatternType
	resourceType        admin.ResourceType
}

// aclsConfig defines the default values if a flag is not provided. These all default
// to doing no filtering (e.g. "all" or null)
var aclsConfig = aclsCmdConfig{
	hostFilter:          "",
	operationType:       admin.ACLOperationType(kafka.ACLOperationTypeAny),
	permissionType:      admin.ACLPermissionType(kafka.ACLPermissionTypeAny),
	principalFilter:     "",
	resourceNameFilter:  "",
	resourceType:        admin.ResourceType(kafka.ResourceTypeAny),
	resourcePatternType: admin.PatternType(kafka.PatternTypeAny),
}

func aclsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "acls",
		Short: "Displays information for ACLs in the cluster. Supports filtering with flags.",
		Args:  cobra.NoArgs,
		Example: `List all acls
$ topicctl get acls

List read acls for topic my-topic
$ topicctl get acls --resource-type topic --resource-name my-topic --operations read

List acls for user Alice with permission allow
$ topicctl get acls --principal User:alice --permission-type allow

List acls for host 198.51.100.0
$ topicctl get acls --host 198.51.100.0
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cliRunner, err := getCliRunnerAndCtx()
			if err != nil {
				return err
			}

			filter := kafka.ACLFilter{
				ResourceTypeFilter:        kafka.ResourceType(aclsConfig.resourceType),
				ResourceNameFilter:        aclsConfig.resourceNameFilter,
				ResourcePatternTypeFilter: kafka.PatternType(aclsConfig.resourcePatternType),
				PrincipalFilter:           aclsConfig.principalFilter,
				HostFilter:                aclsConfig.hostFilter,
				Operation:                 kafka.ACLOperationType(aclsConfig.operationType),
				PermissionType:            kafka.ACLPermissionType(aclsConfig.permissionType),
			}
			return cliRunner.GetACLs(ctx, filter)
		},
	}
	cmd.Flags().StringVar(
		&aclsConfig.hostFilter,
		"host",
		"",
		`The host to filter on. (e.g. 198.51.100.0)`,
	)
	cmd.Flags().Var(
		&aclsConfig.operationType,
		"operations",
		`The operation that is being allowed or denied to filter on. allowed: "any", "all", "read", "write", "create", "delete", "alter", "describe", "clusteraction", "describeconfigs", "alterconfigs" or "idempotentwrite"`,
	)
	cmd.Flags().Var(
		&aclsConfig.permissionType,
		"permission-type",
		`The permission type to filter on. allowed: "any", "allow", or "deny"`,
	)
	cmd.Flags().StringVar(
		&aclsConfig.principalFilter,
		"principal",
		"",
		`The principal to filter on in principalType:name format (e.g. User:alice).`,
	)
	cmd.Flags().StringVar(
		&aclsConfig.resourceNameFilter,
		"resource-name",
		"",
		`The resource name to filter on. (e.g. my-topic)`,
	)
	cmd.Flags().Var(
		&aclsConfig.resourcePatternType,
		"resource-pattern-type",
		`The type of the resource pattern or filter. allowed: "any", "match", "literal", "prefixed". "any" will match any pattern type (literal or prefixed), but will match the resource name exactly, where as "match" will perform pattern matching to list all acls that affect the supplied resource(s).`,
	)
	cmd.Flags().Var(
		&aclsConfig.resourceType,
		"resource-type",
		`The type of resource to filter on. allowed: "any", "topic", "group", "cluster", "transactionalid", "delegationtoken"`,
	)
	return cmd
}
