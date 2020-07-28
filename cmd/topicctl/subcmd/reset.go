package subcmd

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/apply"
	"github.com/segmentio/topicctl/pkg/cli"
	"github.com/segmentio/topicctl/pkg/config"
	"github.com/segmentio/topicctl/pkg/groups"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var resetOffsetsCmd = &cobra.Command{
	Use:     "reset-offsets [topic name] [group name]",
	Short:   "reset consumer group offsets",
	Args:    cobra.MinimumNArgs(2),
	PreRunE: resetOffsetsPreRun,
	RunE:    resetOffsetsRun,
}

type resetOffsetsCmdConfig struct {
	clusterConfig string
	offset        int64
	partitions    []int
	zkAddr        string
	zkPrefix      string
}

var resetOffsetsConfig resetOffsetsCmdConfig

func init() {
	resetOffsetsCmd.Flags().StringVar(
		&resetOffsetsConfig.clusterConfig,
		"cluster-config",
		os.Getenv("TOPICCTL_CLUSTER_CONFIG"),
		"Cluster config",
	)
	resetOffsetsCmd.Flags().Int64Var(
		&resetOffsetsConfig.offset,
		"offset",
		-2,
		"Offset",
	)
	resetOffsetsCmd.Flags().IntSliceVar(
		&resetOffsetsConfig.partitions,
		"partitions",
		[]int{},
		"Partition (defaults to all)",
	)
	resetOffsetsCmd.Flags().StringVarP(
		&resetOffsetsConfig.zkAddr,
		"zk-addr",
		"z",
		"",
		"ZooKeeper address",
	)
	resetOffsetsCmd.Flags().StringVar(
		&resetOffsetsConfig.zkPrefix,
		"zk-prefix",
		"",
		"Prefix for cluster-related nodes in zk",
	)

	RootCmd.AddCommand(resetOffsetsCmd)
}

func resetOffsetsPreRun(cmd *cobra.Command, args []string) error {
	if resetOffsetsConfig.clusterConfig == "" && resetOffsetsConfig.zkAddr == "" {
		return errors.New("Must set either cluster-config or zk address")
	}
	if resetOffsetsConfig.clusterConfig != "" &&
		(resetOffsetsConfig.zkAddr != "" || resetOffsetsConfig.zkPrefix != "") {
		log.Warn("zk-addr and zk-prefix flags are ignored when using cluster-config")
	}

	return nil
}

func resetOffsetsRun(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var adminClient *admin.Client
	var clientErr error

	if resetOffsetsConfig.clusterConfig != "" {
		clusterConfig, err := config.LoadClusterFile(resetOffsetsConfig.clusterConfig)
		if err != nil {
			return err
		}
		adminClient, clientErr = clusterConfig.NewAdminClient(ctx, nil, false)
	} else {
		adminClient, clientErr = admin.NewClient(
			ctx,
			admin.ClientConfig{
				ZKAddrs:  []string{resetOffsetsConfig.zkAddr},
				ZKPrefix: resetOffsetsConfig.zkPrefix,
				ReadOnly: false,
			},
		)
	}

	if clientErr != nil {
		return clientErr
	}
	defer adminClient.Close()

	topic := args[0]
	group := args[1]

	topicInfo, err := adminClient.GetTopic(ctx, topic, false)
	if err != nil {
		return err
	}
	partitionIDsMap := map[int]struct{}{}
	for _, partitionInfo := range topicInfo.Partitions {
		partitionIDsMap[partitionInfo.ID] = struct{}{}
	}

	partitionOffsets := map[int]int64{}

	if len(resetOffsetsConfig.partitions) > 0 {
		for _, partition := range resetOffsetsConfig.partitions {
			if _, ok := partitionIDsMap[partition]; !ok {
				return fmt.Errorf("Partition %d not found in topic %s", partition, topic)
			}

			partitionOffsets[partition] = resetOffsetsConfig.offset
		}
	} else {
		for _, partitionInfo := range topicInfo.Partitions {
			partitionOffsets[partitionInfo.ID] = resetOffsetsConfig.offset
		}
	}

	log.Infof(
		"This will reset the offsets for the following partitions in topic %s for group %s:\n%s",
		topic,
		group,
		groups.FormatPartitionOffsets(partitionOffsets),
	)

	ok, _ := apply.Confirm("OK to continue?", false)
	if !ok {
		return errors.New("Stopping because of user response")
	}

	cliRunner := cli.NewCLIRunner(adminClient, log.Infof, true)
	return cliRunner.ResetOffsets(
		ctx,
		topic,
		group,
		partitionOffsets,
	)
}
