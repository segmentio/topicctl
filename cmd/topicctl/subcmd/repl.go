package subcmd

import (
	"context"
	"errors"
	"os"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/cli"
	"github.com/segmentio/topicctl/pkg/config"
	log "github.com/sirupsen/logrus"
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
	brokerCACert  string
	brokerCert    string
	brokerKey     string
	clusterConfig string
	zkAddr        string
	zkPrefix      string
}

var replConfig replCmdConfig

func init() {
	replCmd.Flags().StringVar(
		&replConfig.brokerAddr,
		"broker-addr",
		"",
		"Broker address",
	)
	replCmd.Flags().StringVar(
		&replConfig.brokerCACert,
		"broker-ca-cert",
		"",
		"Path to broker client CA cert PEM file if using TLS",
	)
	replCmd.Flags().StringVar(
		&replConfig.brokerCert,
		"broker-cert",
		"",
		"Path to broker client cert PEM file if using TLS",
	)
	replCmd.Flags().StringVar(
		&replConfig.brokerKey,
		"broker-key",
		"",
		"Path to broker client private key PEM file if using TLS",
	)
	replCmd.Flags().StringVar(
		&replConfig.clusterConfig,
		"cluster-config",
		os.Getenv("TOPICCTL_CLUSTER_CONFIG"),
		"Cluster config",
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
	if replConfig.clusterConfig == "" && replConfig.zkAddr == "" &&
		replConfig.brokerAddr == "" {
		return errors.New("Must set either broker-addr, cluster-config, or zk-addr")
	}
	if replConfig.clusterConfig != "" &&
		(replConfig.zkAddr != "" || replConfig.zkPrefix != "" || replConfig.brokerAddr != "") {
		log.Warn("broker and zk flags are ignored when using cluster-config")
	}

	return nil
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
		useTLS := (replConfig.brokerCACert != "" ||
			replConfig.brokerCert != "" ||
			replConfig.brokerKey != "")
		adminClient, clientErr = admin.NewBrokerAdminClient(
			ctx,
			admin.BrokerAdminClientConfig{
				ConnectorConfig: admin.ConnectorConfig{
					BrokerAddr: replConfig.brokerAddr,
					UseTLS:     useTLS,
					CACertPath: replConfig.brokerCACert,
					CertPath:   replConfig.brokerCert,
					KeyPath:    replConfig.brokerKey,
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
