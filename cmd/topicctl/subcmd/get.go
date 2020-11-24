package subcmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/cli"
	"github.com/segmentio/topicctl/pkg/config"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var getCmd = &cobra.Command{
	Use:   "get [resource type]",
	Short: "get instances of a particular type",
	Long: strings.Join(
		[]string{
			"Get instances of a particular type.",
			"Supported types currently include: balance, brokers, config, groups, lags, members, partitions, offsets, and topics.",
			"",
			"See the tool README for a detailed description of each one.",
		},
		"\n",
	),
	Args:    cobra.MinimumNArgs(1),
	PreRunE: getPreRun,
	RunE:    getRun,
}

type getCmdConfig struct {
	brokerAddr    string
	clusterConfig string
	full          bool
	zkAddr        string
	zkPrefix      string
}

var getConfig getCmdConfig

func init() {
	getCmd.Flags().StringVar(
		&getConfig.brokerAddr,
		"broker-addr",
		"",
		"Broker address",
	)
	getCmd.Flags().StringVar(
		&getConfig.clusterConfig,
		"cluster-config",
		os.Getenv("TOPICCTL_CLUSTER_CONFIG"),
		"Cluster config",
	)
	getCmd.Flags().BoolVar(
		&getConfig.full,
		"full",
		false,
		"Show more full information for resources",
	)
	getCmd.Flags().StringVarP(
		&getConfig.zkAddr,
		"zk-addr",
		"z",
		"",
		"ZooKeeper address",
	)
	getCmd.Flags().StringVar(
		&getConfig.zkPrefix,
		"zk-prefix",
		"",
		"Prefix for cluster-related nodes in zk",
	)

	RootCmd.AddCommand(getCmd)
}

func getPreRun(cmd *cobra.Command, args []string) error {
	if getConfig.clusterConfig == "" && getConfig.zkAddr == "" && getConfig.brokerAddr == "" {
		return errors.New("Must set either broker-addr, cluster-config, zk address")
	}
	if getConfig.clusterConfig != "" &&
		(getConfig.zkAddr != "" || getConfig.zkPrefix != "" || getConfig.brokerAddr != "") {
		log.Warn("broker and zk flags are ignored when using cluster-config")
	}

	return nil
}

func getRun(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	sess := session.Must(session.NewSession())

	var adminClient admin.Client
	var clientErr error

	if getConfig.clusterConfig != "" {
		clusterConfig, err := config.LoadClusterFile(getConfig.clusterConfig)
		if err != nil {
			return err
		}
		adminClient, clientErr = clusterConfig.NewAdminClient(ctx, sess, true)
	} else if getConfig.brokerAddr != "" {
		adminClient, clientErr = admin.NewBrokerAdminClient(
			ctx,
			admin.BrokerAdminClientConfig{
				ConnectorConfig: admin.ConnectorConfig{
					BrokerAddr: getConfig.brokerAddr,
				},
				ReadOnly: true,
			},
		)
	} else {
		adminClient, clientErr = admin.NewZKAdminClient(
			ctx,
			admin.ZKAdminClientConfig{
				ZKAddrs:  []string{getConfig.zkAddr},
				ZKPrefix: getConfig.zkPrefix,
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

	cliRunner := cli.NewCLIRunner(adminClient, log.Infof, !noSpinner)

	resource := args[0]

	switch resource {
	case "balance":
		var topicName string

		if len(args) == 2 {
			topicName = args[1]
		} else if len(args) > 2 {
			return fmt.Errorf("Can provide at most one positional argument with brokers")
		}

		return cliRunner.GetBrokerBalance(ctx, topicName)
	case "brokers":
		if len(args) > 1 {
			return fmt.Errorf("Can only provide one positional argument with brokers")
		}

		return cliRunner.GetBrokers(ctx, getConfig.full)
	case "config":
		if len(args) != 2 {
			return fmt.Errorf("Must provide broker ID or topic name as second positional argument")
		}

		return cliRunner.GetConfig(ctx, args[1])
	case "groups":
		if len(args) > 1 {
			return fmt.Errorf("Can only provide one positional argument with groups")
		}

		return cliRunner.GetGroups(ctx)
	case "lags":
		if len(args) != 3 {
			return fmt.Errorf("Must provide topic and groupID as additional positional arguments")
		}

		return cliRunner.GetMemberLags(ctx, args[1], args[2], getConfig.full)
	case "members":
		if len(args) != 2 {
			return fmt.Errorf("Must provide group ID as second positional argument")
		}

		return cliRunner.GetGroupMembers(ctx, args[1], getConfig.full)
	case "partitions":
		if len(args) != 2 {
			return fmt.Errorf("Must provide topic as second positional argument")
		}
		topicName := args[1]

		return cliRunner.GetPartitions(ctx, topicName)
	case "offsets":
		if len(args) != 2 {
			return fmt.Errorf("Must provide topic as second positional argument")
		}
		topicName := args[1]

		return cliRunner.GetOffsets(ctx, topicName)
	case "topics":
		if len(args) > 1 {
			return fmt.Errorf("Can only provide one positional argument with args")
		}

		return cliRunner.GetTopics(ctx, getConfig.full)
	default:
		return fmt.Errorf("Unrecognized resource type: %s", resource)
	}
}
