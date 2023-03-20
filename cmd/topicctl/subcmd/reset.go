package subcmd

import (
	"context"
	"errors"
	"fmt"

	"github.com/segmentio/topicctl/pkg/apply"
	"github.com/segmentio/topicctl/pkg/cli"
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
	offset     int64
	partitions []int
	toEarliest bool
	toLatest   bool

	shared sharedOptions
}

var resetOffsetsConfig resetOffsetsCmdConfig

func init() {
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

	resetOffsetsCmd.Flags().BoolVar(
		&resetOffsetsConfig.toEarliest,
		"to-earliest",
		false,
		"Resets offsets of consumer group members to earliest offsets of partitions")
	resetOffsetsCmd.Flags().BoolVar(
		&resetOffsetsConfig.toLatest,
		"to-latest",
		false,
		"Resets offsets of consumer group members to latest offsets of partitions")

	addSharedFlags(resetOffsetsCmd, &resetOffsetsConfig.shared)
	RootCmd.AddCommand(resetOffsetsCmd)
}

func resetOffsetsPreRun(cmd *cobra.Command, args []string) error {
	msg := "You must choose only one of the following reset-offset specifications: --to-earliest, --to-latest, --offset."

	if resetOffsetsConfig.toEarliest {
		if resetOffsetsConfig.toLatest {
			return errors.New(msg)
		}

		if cmd.Flags().Changed("offset") {
			return errors.New(msg)
		}

	} else if resetOffsetsConfig.toLatest {
		if resetOffsetsConfig.toEarliest {
			return errors.New(msg)
		}
		if cmd.Flags().Changed("offset") {
			return errors.New(msg)
		}
	}
	return resetOffsetsConfig.shared.validate()
}

func resetOffsetsRun(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	adminClient, err := resetOffsetsConfig.shared.getAdminClient(ctx, nil, true)
	if err != nil {
		return err
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
	var strategy string
	if resetOffsetsConfig.toLatest {
		strategy = "latest"
	} else if resetOffsetsConfig.toEarliest {
		strategy = "earliest"
	}
	partitionOffsets := map[int]int64{}

	if len(resetOffsetsConfig.partitions) > 0 {
		for _, partition := range resetOffsetsConfig.partitions {
			if _, ok := partitionIDsMap[partition]; !ok {
				return fmt.Errorf("Partition %d not found in topic %s", partition, topic)
			}

			partitionOffsets[partition], err = groups.GetOffset(ctx, adminClient.GetConnector(), topic, strategy, partition, resetOffsetsConfig.offset)
			if err != nil {
				return err
			}
		}
	} else {
		for _, partitionInfo := range topicInfo.Partitions {
			partitionOffsets[partitionInfo.ID], err = groups.GetOffset(ctx, adminClient.GetConnector(), topic, strategy, partitionInfo.ID, resetOffsetsConfig.offset)
			if err != nil {
				return err
			}
		}
	}

	log.Infof(
		"This will reset the offsets for the following partitions in topic %s for group %s:\n%s",
		topic,
		group,
		groups.FormatPartitionOffsets(partitionOffsets),
	)
	log.Info(
		"Please ensure that all other consumers are stopped, otherwise the reset might be overridden.",
	)

	ok, _ := apply.Confirm("OK to continue?", false)
	if !ok {
		return errors.New("Stopping because of user response")
	}

	cliRunner := cli.NewCLIRunner(adminClient, log.Infof, !noSpinner)

	return cliRunner.ResetOffsets(
		ctx,
		topic,
		group,
		partitionOffsets,
	)
}
