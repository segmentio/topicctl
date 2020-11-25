package subcmd

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/cli"
	"github.com/segmentio/topicctl/pkg/config"
	"github.com/spf13/cobra"
)

var replCmd = &cobra.Command{
	Use:     "repl",
	Short:   "repl allows interactively running commands against a cluster",
	PreRunE: replPreRun,
	RunE:    replRun,
}

type replCmdConfig struct {
	brokerAddr    string
	clusterConfig string
	tlsCACert     string
	tlsCert       string
	tlsKey        string
	tlsSkipVerify bool
	tlsServerName string
	zkAddr        string
	zkPrefix      string
}

var replConfig replCmdConfig

func init() {
	replCmd.Flags().StringVarP(
		&replConfig.brokerAddr,
		"broker-addr",
		"b",
		"",
		"Broker address",
	)
	replCmd.Flags().StringVar(
		&replConfig.clusterConfig,
		"cluster-config",
		os.Getenv("TOPICCTL_CLUSTER_CONFIG"),
		"Cluster config",
	)
	replCmd.Flags().StringVar(
		&replConfig.tlsCACert,
		"tls-ca-cert",
		"",
		"Path to client CA cert PEM file if using TLS",
	)
	replCmd.Flags().StringVar(
		&replConfig.tlsCert,
		"tls-cert",
		"",
		"Path to client cert PEM file if using TLS",
	)
	replCmd.Flags().StringVar(
		&replConfig.tlsKey,
		"tls-key",
		"",
		"Path to client private key PEM file if using TLS",
	)
	replCmd.Flags().StringVar(
		&replConfig.tlsServerName,
		"tls-server-name",
		"",
		"Server name to use for TLS cert verification",
	)
	replCmd.Flags().BoolVar(
		&replConfig.tlsSkipVerify,
		"tls-skip-verify",
		false,
		"Skip hostname verification when using TLS",
	)
	replCmd.Flags().StringVarP(
		&replConfig.zkAddr,
		"zk-addr",
		"z",
		"",
		"ZooKeeper address",
	)
	replCmd.Flags().StringVar(
		&replConfig.zkPrefix,
		"zk-prefix",
		"",
		"Prefix for cluster-related nodes in zk",
	)

	RootCmd.AddCommand(replCmd)
}

func replPreRun(cmd *cobra.Command, args []string) error {
	return validateCommonFlags(
		replConfig.clusterConfig,
		replConfig.zkAddr,
		replConfig.zkPrefix,
		replConfig.brokerAddr,
		replConfig.tlsCACert,
		replConfig.tlsCert,
		replConfig.tlsKey,
	)
}

func replRun(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	sess := session.Must(session.NewSession())

	var adminClient admin.Client
	var clientErr error

	if replConfig.clusterConfig != "" {
		clusterConfig, err := config.LoadClusterFile(replConfig.clusterConfig)
		if err != nil {
			return err
		}
		adminClient, clientErr = clusterConfig.NewAdminClient(ctx, sess, true)
	} else if replConfig.brokerAddr != "" {
		useTLS := (replConfig.tlsCACert != "" ||
			replConfig.tlsCert != "" ||
			replConfig.tlsKey != "")
		adminClient, clientErr = admin.NewBrokerAdminClient(
			ctx,
			admin.BrokerAdminClientConfig{
				ConnectorConfig: admin.ConnectorConfig{
					BrokerAddr: replConfig.brokerAddr,
					UseTLS:     useTLS,
					CACertPath: replConfig.tlsCACert,
					CertPath:   replConfig.tlsCert,
					KeyPath:    replConfig.tlsKey,
					ServerName: replConfig.tlsServerName,
					SkipVerify: replConfig.tlsSkipVerify,
				},
				ReadOnly: true,
			},
		)
	} else {
		adminClient, clientErr = admin.NewZKAdminClient(
			ctx,
			admin.ZKAdminClientConfig{
				ZKAddrs:  []string{replConfig.zkAddr},
				ZKPrefix: replConfig.zkPrefix,
				Sess:     sess,
				// Run in read-only mode to ensure that tailing doesn't make any changes
				// in the cluster
				ReadOnly: true,
			},
		)
	}

	if clientErr != nil {
		return clientErr
	}
	defer adminClient.Close()

	repl, err := cli.NewRepl(ctx, adminClient)
	if err != nil {
		return err
	}

	repl.Run()
	return nil
}
