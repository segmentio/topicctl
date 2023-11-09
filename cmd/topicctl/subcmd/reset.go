package subcmd

import (
	"context"
	"errors"
	"fmt"
	"strconv"

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
	offset             int64
	partitions         []int
	partitionOffsetMap map[string]int64
	toEarliest         bool
	toLatest           bool

	shared sharedOptions
}

var resetOffsetsConfig resetOffsetsCmdConfig

func init() {
	resetOffsetsCmd.Flags().Int64Var(
		&resetOffsetsConfig.offset,
		"offset",
		-2,
		"Desired offset for the target partitions",
	)
	resetOffsetsCmd.Flags().IntSliceVar(
		&resetOffsetsConfig.partitions,
		"partitions",
		[]int{},
		"List of partitions to reset e.g. 1,2,3,.. (defaults to all)",
	)
	resetOffsetsCmd.Flags().StringToInt64Var(
		&resetOffsetsConfig.partitionOffsetMap,
		"partition-offset-map",
		map[string]int64{},
		"Map of partition IDs to their corresponding desired offsets e.g. 1=5,2=10,3=12,...",
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
	resetOffsetSpecification := "You must choose only one of the following reset-offset specifications: --to-earliest, --to-latest, --offset."
	offsetMapSpecification := "--partition-offset-map option cannot be coupled with any of the following options: --partitions, --to-earliest, --to-latest, --offset."

	if len(resetOffsetsConfig.partitionOffsetMap) > 0 && (cmd.Flags().Changed("offset") ||
		len(resetOffsetsConfig.partitions) > 0 ||
		resetOffsetsConfig.toEarliest ||
		resetOffsetsConfig.toLatest) {
		return errors.New(offsetMapSpecification)

	} else if resetOffsetsConfig.toEarliest && resetOffsetsConfig.toLatest {
		return errors.New(resetOffsetSpecification)

	} else if cmd.Flags().Changed("offset") && (resetOffsetsConfig.toEarliest || resetOffsetsConfig.toLatest) {
		return errors.New(resetOffsetSpecification)
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
	var resetOffsetsStrategy string
	if resetOffsetsConfig.toLatest {
		resetOffsetsStrategy = groups.LatestResetOffsetsStrategy
	} else if resetOffsetsConfig.toEarliest {
		resetOffsetsStrategy = groups.EarliestResetOffsetsStrategy
	}
	partitionOffsets := map[int]int64{}

	if len(resetOffsetsConfig.partitionOffsetMap) > 0 {
		for partition, offset := range resetOffsetsConfig.partitionOffsetMap {
			var partitionID int
			if partitionID, err = strconv.Atoi(partition); err != nil {
				return fmt.Errorf("Partition value %s must be a number", partition)
			}
			if _, ok := partitionIDsMap[partitionID]; !ok {
				return fmt.Errorf("Partition %d not found in topic %s", partitionID, topic)
			}

			partitionOffsets[partitionID] = offset

		}

	} else if len(resetOffsetsConfig.partitions) > 0 {
		for _, partition := range resetOffsetsConfig.partitions {
			if _, ok := partitionIDsMap[partition]; !ok {
				return fmt.Errorf("Partition %d not found in topic %s", partition, topic)
			}
			if resetOffsetsConfig.toEarliest || resetOffsetsConfig.toLatest {
				partitionOffsets[partition], err = groups.GetEarliestOrLatestOffset(ctx, adminClient.GetConnector(), topic, resetOffsetsStrategy, partition)
				if err != nil {
					return err
				}
			} else {
				partitionOffsets[partition] = resetOffsetsConfig.offset
			}

		}
	} else {
		for _, partitionInfo := range topicInfo.Partitions {
			if resetOffsetsConfig.toEarliest || resetOffsetsConfig.toLatest {
				partitionOffsets[partitionInfo.ID], err = groups.GetEarliestOrLatestOffset(ctx, adminClient.GetConnector(), topic, resetOffsetsStrategy, partitionInfo.ID)
				if err != nil {
					return err
				}
			} else {
				partitionOffsets[partitionInfo.ID] = resetOffsetsConfig.offset
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
