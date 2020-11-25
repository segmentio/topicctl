package subcmd

import (
	"context"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/cli"
	"github.com/segmentio/topicctl/pkg/config"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var tailCmd = &cobra.Command{
	Use:     "tail [topic name]",
	Short:   "tail events in a topic",
	Args:    cobra.MinimumNArgs(1),
	PreRunE: tailPreRun,
	RunE:    tailRun,
}

type tailCmdConfig struct {
	brokerAddr    string
	clusterConfig string
	offset        int64
	partitions    []int
	raw           bool
	tlsCACert     string
	tlsCert       string
	tlsKey        string
	tlsSkipVerify bool
	tlsServerName string
	zkAddr        string
	zkPrefix      string
}

var tailConfig tailCmdConfig

func init() {
	tailCmd.Flags().StringVarP(
		&tailConfig.brokerAddr,
		"broker-addr",
		"b",
		"",
		"Broker address",
	)
	tailCmd.Flags().StringVar(
		&tailConfig.clusterConfig,
		"cluster-config",
		os.Getenv("TOPICCTL_CLUSTER_CONFIG"),
		"Cluster config",
	)
	tailCmd.Flags().Int64Var(
		&tailConfig.offset,
		"offset",
		kafka.LastOffset,
		"Offset (defaults to last)",
	)
	tailCmd.Flags().IntSliceVar(
		&tailConfig.partitions,
		"partitions",
		[]int{},
		"Partition (defaults to all)",
	)
	tailCmd.Flags().BoolVar(
		&tailConfig.raw,
		"raw",
		false,
		"Output raw values only",
	)
	tailCmd.Flags().StringVar(
		&tailConfig.tlsCACert,
		"tls-ca-cert",
		"",
		"Path to client CA cert PEM file if using TLS",
	)
	tailCmd.Flags().StringVar(
		&tailConfig.tlsCert,
		"tls-cert",
		"",
		"Path to client cert PEM file if using TLS",
	)
	tailCmd.Flags().StringVar(
		&tailConfig.tlsKey,
		"tls-key",
		"",
		"Path to client private key PEM file if using TLS",
	)
	tailCmd.Flags().StringVar(
		&tailConfig.tlsServerName,
		"tls-server-name",
		"",
		"Server name to use for TLS cert verification",
	)
	tailCmd.Flags().BoolVar(
		&tailConfig.tlsSkipVerify,
		"tls-skip-verify",
		false,
		"Skip hostname verification when using TLS",
	)
	tailCmd.Flags().StringVarP(
		&tailConfig.zkAddr,
		"zk-addr",
		"z",
		"",
		"ZooKeeper address",
	)
	tailCmd.Flags().StringVar(
		&tailConfig.zkPrefix,
		"zk-prefix",
		"",
		"Prefix for cluster-related nodes in zk",
	)

	RootCmd.AddCommand(tailCmd)
}

func tailPreRun(cmd *cobra.Command, args []string) error {
	if tailConfig.raw {
		// In raw mode, only log out errors
		log.SetLevel(log.ErrorLevel)
	}

	return validateCommonFlags(
		tailConfig.clusterConfig,
		tailConfig.zkAddr,
		tailConfig.zkPrefix,
		tailConfig.brokerAddr,
		tailConfig.tlsCACert,
		tailConfig.tlsCert,
		tailConfig.tlsKey,
	)
}

func tailRun(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	var adminClient admin.Client
	var clientErr error

	if tailConfig.clusterConfig != "" {
		clusterConfig, err := config.LoadClusterFile(tailConfig.clusterConfig)
		if err != nil {
			return err
		}
		adminClient, clientErr = clusterConfig.NewAdminClient(ctx, nil, true)
	} else if tailConfig.brokerAddr != "" {
		useTLS := (tailConfig.tlsCACert != "" ||
			tailConfig.tlsCert != "" ||
			tailConfig.tlsKey != "")
		adminClient, clientErr = admin.NewBrokerAdminClient(
			ctx,
			admin.BrokerAdminClientConfig{
				ConnectorConfig: admin.ConnectorConfig{
					BrokerAddr: tailConfig.brokerAddr,
					UseTLS:     useTLS,
					CACertPath: tailConfig.tlsCACert,
					CertPath:   tailConfig.tlsCert,
					KeyPath:    tailConfig.tlsKey,
					ServerName: tailConfig.tlsServerName,
					SkipVerify: tailConfig.tlsSkipVerify,
				},
				ReadOnly: true,
			},
		)
	} else {
		adminClient, clientErr = admin.NewZKAdminClient(
			ctx,
			admin.ZKAdminClientConfig{
				ZKAddrs:  []string{tailConfig.zkAddr},
				ZKPrefix: tailConfig.zkPrefix,
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

	cliRunner := cli.NewCLIRunner(adminClient, log.Infof, false)
	return cliRunner.Tail(
		ctx,
		args[0],
		tailConfig.offset,
		tailConfig.partitions,
		-1,
		"",
		tailConfig.raw,
	)
}

func stringsToInts(strs []string) ([]int, error) {
	ints := []int{}

	for _, str := range strs {
		nextInt, err := strconv.ParseInt(str, 10, 32)
		if err != nil {
			return nil, err
		}
		ints = append(ints, int(nextInt))
	}

	return ints, nil
}
