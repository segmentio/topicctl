package subcmd

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/acl"
	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/cli"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var deleteCmd = &cobra.Command{
	Use:   "delete [resource type]",
	Short: "delete instances of a particular type",
	Long: strings.Join(
		[]string{
			"Deletes instances of a particular type.",
		},
		"\n",
	),
	PersistentPreRunE: deletePreRun,
}

type deleteCmdConfig struct {
	dryRun bool

	shared sharedOptions
}

var deleteConfig deleteCmdConfig

func init() {
	deleteCmd.PersistentFlags().BoolVar(
		&deleteConfig.dryRun,
		"dry-run",
		false,
		"Do a dry-run",
	)

	addSharedFlags(deleteCmd, &deleteConfig.shared)
	deleteCmd.AddCommand(
		deleteACLCmd(),
		deleteGroupCmd(),
	)
	RootCmd.AddCommand(deleteCmd)
}

func deletePreRun(cmd *cobra.Command, args []string) error {
	return deleteConfig.shared.validate()
}

var deleteACLsConfig = aclsCmdConfig{
	// This was added in a later version of Kafka, so we provide a default
	// value to avoid breaking existing users by making this required.
	resourcePatternType: admin.PatternType(kafka.PatternTypeAny),
}

func deleteACLCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "acls [flags]",
		Short: "Delete ACLs. Requires providing flags to target ACLs for deletion.",
		Args:  cobra.NoArgs,
		Example: `Delete read acls for topic my-topic, user 'User:default', and host '*'
$ topicctl delete acls --resource-type topic --resource-pattern-type literal --resource-name my-topic --principal 'User:default' --host '*' --operation read --permission-type allow
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			sess := session.Must(session.NewSession())

			adminClient, err := deleteConfig.shared.getAdminClient(ctx, sess, deleteConfig.dryRun)
			if err != nil {
				return err
			}
			defer adminClient.Close()

			cliRunner := cli.NewCLIRunner(adminClient, log.Infof, !noSpinner)

			filter := kafka.DeleteACLsFilter{
				ResourceTypeFilter:        kafka.ResourceType(deleteACLsConfig.resourceType),
				ResourceNameFilter:        deleteACLsConfig.resourceNameFilter,
				ResourcePatternTypeFilter: kafka.PatternType(deleteACLsConfig.resourcePatternType),
				PrincipalFilter:           deleteACLsConfig.principalFilter,
				HostFilter:                deleteACLsConfig.hostFilter,
				Operation:                 kafka.ACLOperationType(deleteACLsConfig.operationType),
				PermissionType:            kafka.ACLPermissionType(deleteACLsConfig.permissionType),
			}

			aclAdminConfig := acl.ACLAdminConfig{
				// Omit fields we don't need for deletes
				DryRun: deleteConfig.dryRun,
				// Deletes cannot be skipped
				SkipConfirm: false,
			}

			return cliRunner.DeleteACL(ctx, aclAdminConfig, filter)
		},
	}
	cmd.Flags().StringVar(
		&deleteACLsConfig.hostFilter,
		"host",
		"",
		`The host to filter on. (e.g. 198.51.100.0) (Required)`,
	)
	cmd.MarkFlagRequired("host")

	cmd.Flags().Var(
		&deleteACLsConfig.operationType,
		"operation",
		`The operation that is being allowed or denied to filter on. allowed: [any, all, read, write, create, delete, alter, describe, clusteraction, describeconfigs, alterconfigs, idempotentwrite] (Required)`,
	)
	cmd.MarkFlagRequired("operation")

	cmd.Flags().Var(
		&deleteACLsConfig.permissionType,
		"permission-type",
		`The permission type to filter on. allowed: [any, allow, deny] (Required)`,
	)
	cmd.MarkFlagRequired("permission-type")

	cmd.Flags().StringVar(
		&deleteACLsConfig.principalFilter,
		"principal",
		"",
		`The principal to filter on in principalType:name format (e.g. User:alice). (Required)`,
	)
	cmd.MarkFlagRequired("principal")

	cmd.Flags().StringVar(
		&deleteACLsConfig.resourceNameFilter,
		"resource-name",
		"",
		`The resource name to filter on. (e.g. my-topic) (Required)`,
	)
	cmd.MarkFlagRequired("resource-name")

	cmd.Flags().Var(
		&deleteACLsConfig.resourcePatternType,
		"resource-pattern-type",
		`The type of the resource pattern or filter. allowed: [any, match, literal, prefixed]. "any" will match any pattern type (literal or prefixed), but will match the resource name exactly, where as "match" will perform pattern matching to list all acls that affect the supplied resource(s).`,
	)

	cmd.Flags().Var(
		&deleteACLsConfig.resourceType,
		"resource-type",
		`The type of resource to filter on. allowed: [any, topic, group, cluster, transactionalid, delegationtoken] (Required)`,
	)
	cmd.MarkFlagRequired("resource-type")
	return cmd
}

func deleteGroupCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "group [group]",
		Short:   "Delete a given consumer group. Ensure the group is not active before deleting.",
		Args:    cobra.ExactArgs(1),
		Example: `Delete group my-group`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			sess := session.Must(session.NewSession())

			adminClient, err := deleteConfig.shared.getAdminClient(ctx, sess, deleteConfig.dryRun)
			if err != nil {
				return err
			}
			defer adminClient.Close()

			group := args[0]
			cliRunner := cli.NewCLIRunner(adminClient, log.Infof, !noSpinner)
			return cliRunner.DeleteGroup(ctx, group)
		},
	}

	return cmd

}
