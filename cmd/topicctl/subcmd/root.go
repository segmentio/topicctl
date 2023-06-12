package subcmd

import (
	"fmt"
	"os"

	"github.com/segmentio/topicctl/pkg/version"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var debug bool
var noSpinner bool

// RootCmd is the cobra CLI root command.
var RootCmd = &cobra.Command{
	Use:               "topicctl",
	Short:             "topicctl runs topic workflows",
	SilenceUsage:      true,
	SilenceErrors:     true,
	PersistentPreRunE: preRun,
}

func init() {
	log.SetFormatter(&log.JSONFormatter{
		TimestampFormat: "2006-01-02 15:04:05"
	})

	RootCmd.PersistentFlags().BoolVar(
		&debug,
		"debug",
		false,
		"enable debug logging",
	)
	RootCmd.PersistentFlags().BoolVar(
		&noSpinner,
		"no-spinner",
		false,
		"disable all UI spinners",
	)
}

// Execute runs topicctl.
func Execute(versionRef string) {
	RootCmd.Version = fmt.Sprintf("v%s (ref:%s)", version.Version, versionRef)

	if err := RootCmd.Execute(); err != nil {
		log.Errorf("%+v", err)
		os.Exit(1)
	}
}

func preRun(cmd *cobra.Command, args []string) error {
	if debug {
		log.SetLevel(log.DebugLevel)
	}
	return nil
}
