package subcmd

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/segmentio/kafka-go"
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
	shared sharedOptions
}

var deleteConfig deleteCmdConfig

func init() {
	addSharedFlags(deleteCmd, &deleteConfig.shared)
	deleteCmd.AddCommand(
		deleteACLCmd(),
	)
	RootCmd.AddCommand(deleteCmd)
}

func deletePreRun(cmd *cobra.Command, args []string) error {
	return deleteConfig.shared.validate()
}

func deleteACLCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "acl [flags]",
		Short: "Delete an ACL",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			sess := session.Must(session.NewSession())

			adminClient, err := deleteConfig.shared.getAdminClient(ctx, sess, false)
			if err != nil {
				return err
			}
			defer adminClient.Close()

			cliRunner := cli.NewCLIRunner(adminClient, log.Infof, !noSpinner)

			filter := kafka.ACLFilter{}
			return cliRunner.DeleteACL(ctx, filter)
		},
	}
}
