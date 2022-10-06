package subcmd

import (
	"context"
	"fmt"
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
			"Supported types currently include: topic.",
			"",
			"See the tool README for a detailed description of each one.",
		},
		"\n",
	),
	Args:    cobra.MinimumNArgs(1),
	RunE:    deleteRun,
	PreRunE: deletePreRun,
}

type deleteCmdConfig struct {
	shared sharedOptions
}

var deleteConfig deleteCmdConfig

func init() {
	addSharedFlags(deleteCmd, &deleteConfig.shared)
	RootCmd.AddCommand(deleteCmd)
}

func deletePreRun(cmd *cobra.Command, args []string) error {
	return deleteConfig.shared.validate()
}

func deleteRun(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	sess := session.Must(session.NewSession())

	adminClient, err := deleteConfig.shared.getAdminClient(ctx, sess, false)
	if err != nil {
		return err
	}
	defer adminClient.Close()

	cliRunner := cli.NewCLIRunner(adminClient, log.Infof, !noSpinner)

	resource := args[0]

	switch resource {
	case "topic":
		var topicName string

		if len(args) == 2 {
			topicName = args[1]
		} else if len(args) > 2 {
			return fmt.Errorf("Can only provide one positional argument with args")
		}

		return cliRunner.DeleteTopic(ctx, topicName)
	default:
		return fmt.Errorf("Unrecognized resource type: %s", resource)
	}
}
