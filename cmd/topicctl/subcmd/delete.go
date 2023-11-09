package subcmd

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
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
		deleteTopicCmd(),
	)
	RootCmd.AddCommand(deleteCmd)
}

func deletePreRun(cmd *cobra.Command, args []string) error {
	return deleteConfig.shared.validate()
}

func deleteTopicCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "topic [topic name]",
		Short: "Delete a topic",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			sess := session.Must(session.NewSession())

			adminClient, err := deleteConfig.shared.getAdminClient(ctx, sess, false)
			if err != nil {
				return err
			}
			defer adminClient.Close()

			cliRunner := cli.NewCLIRunner(adminClient, log.Infof, !noSpinner)
			return cliRunner.DeleteTopic(ctx, args[0])
		},
	}
}
