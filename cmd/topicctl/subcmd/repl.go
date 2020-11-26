package subcmd

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/segmentio/topicctl/pkg/cli"
	"github.com/spf13/cobra"
)

var replCmd = &cobra.Command{
	Use:     "repl",
	Short:   "repl allows interactively running commands against a cluster",
	PreRunE: replPreRun,
	RunE:    replRun,
}

type replCmdConfig struct {
	shared sharedOptions
}

var replConfig replCmdConfig

func init() {
	addSharedFlags(replCmd, &replConfig.shared)
	RootCmd.AddCommand(replCmd)
}

func replPreRun(cmd *cobra.Command, args []string) error {
	return replConfig.shared.validate()
}

func replRun(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	sess := session.Must(session.NewSession())

	adminClient, err := replConfig.shared.getAdminClient(ctx, sess, true)
	if err != nil {
		return err
	}
	defer adminClient.Close()

	repl, err := cli.NewRepl(ctx, adminClient)
	if err != nil {
		return err
	}

	repl.Run()
	return nil
}
