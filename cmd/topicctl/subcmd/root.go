package subcmd

import (
	"errors"
	"fmt"
	"os"

	"github.com/segmentio/topicctl/pkg/version"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
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
	log.SetFormatter(&prefixed.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		FullTimestamp:   true,
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

func validateCommonFlags(
	clusterConfig string,
	zkAddr string,
	zkPrefix string,
	brokerAddr string,
	tlsCACert string,
	tlsCert string,
	tlsKey string,
) error {
	if clusterConfig == "" && zkAddr == "" && brokerAddr == "" {
		return errors.New("Must set either broker-addr, cluster-config, or zk-addr")
	}
	if zkAddr != "" && brokerAddr != "" {
		return errors.New("Cannot set both zk-addr and broker-addr")
	}
	if clusterConfig != "" &&
		(zkAddr != "" || zkPrefix != "" || brokerAddr != "" || tlsCACert != "" ||
			tlsCert != "" || tlsKey != "") {
		log.Warn("Broker and zk flags are ignored when using cluster-config")
	}

	useTLS := tlsCACert != "" || tlsCert != "" || tlsKey != ""
	if useTLS && (tlsCACert == "" || tlsCert == "" || tlsKey == "") {
		return errors.New("Must set tls-ca-cert, tls-cert, and tls-key if using TLS")
	}
	if useTLS && zkAddr != "" {
		log.Warn("Auth flags are ignored accessing cluster via zookeeper")
	}

	return nil
}
