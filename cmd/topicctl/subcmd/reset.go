package subcmd

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/cli"
	"github.com/segmentio/topicctl/pkg/groups"
	"github.com/segmentio/topicctl/pkg/util"
)

var resetOffsetsCmd = &cobra.Command{
	Use:     "reset-offsets <topic-name> <group-name>",
	Short:   "reset consumer group offsets",
	Args:    cobra.ExactArgs(2),
	PreRunE: resetOffsetsPreRun,
	RunE:    resetOffsetsRun,
}

type resetOffsetsCmdConfig struct {
	offset             int64
	partitions         []int
	partitionOffsetMap map[string]int64
	toEarliest         bool
	toLatest           bool
	delete             bool

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
	resetOffsetsCmd.Flags().BoolVar(
		&resetOffsetsConfig.delete,
		"delete",
		false,
		"Deletes offsets for the given consumer group")

	addSharedFlags(resetOffsetsCmd, &resetOffsetsConfig.shared)
	RootCmd.AddCommand(resetOffsetsCmd)
}

func resetOffsetsPreRun(cmd *cobra.Command, args []string) error {
	resetOffsetSpec := "You must choose only one of the following " +
		"reset-offset specifications: --delete, --to-earliest, --to-latest, " +
		"--offset, or --partition-offset-map."
	offsetMapSpec := "--partition-offset-map option cannot be used with --partitions."

	cfg := resetOffsetsConfig

	numOffsetSpecs := numTrue(
		cfg.toEarliest,
		cfg.toLatest,
		cfg.delete,
		cmd.Flags().Changed("offset"),
		len(cfg.partitionOffsetMap) > 0,
	)

	if numOffsetSpecs > 1 {
		return errors.New(resetOffsetSpec)
	}

	if len(cfg.partitionOffsetMap) > 0 && len(cfg.partitions) > 0 {
		return errors.New(offsetMapSpec)
	}

	return cfg.shared.validate()
}

func resetOffsetsRun(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := resetOffsetsConfig

	adminClient, err := cfg.shared.getAdminClient(ctx, nil, true)
	if err != nil {
		return err
	}

	defer adminClient.Close()

	connector := adminClient.GetConnector()

	topic := args[0]
	group := args[1]

	topicInfo, err := adminClient.GetTopic(ctx, topic, false)
	if err != nil {
		return err
	}

	partitionIDsMap := make(map[int]struct{}, len(topicInfo.Partitions))
	for _, partitionInfo := range topicInfo.Partitions {
		partitionIDsMap[partitionInfo.ID] = struct{}{}
	}

	var strategy string

	switch {
	case resetOffsetsConfig.toLatest:
		strategy = groups.LatestResetOffsetsStrategy
	case resetOffsetsConfig.toEarliest:
		strategy = groups.EarliestResetOffsetsStrategy
	}

	// If explicit per-partition offsets were specified, set them now.
	partitionOffsets, err := parsePartitionOffsetMap(partitionIDsMap, cfg.partitionOffsetMap)
	if err != nil {
		return err
	}

	// Set explicit partitions (without offsets) if specified,
	// otherwise operate on fetched partition info;
	// these will only take effect of per-partition offsets were not specified.
	partitions := cfg.partitions
	if len(partitions) == 0 && len(partitionOffsets) == 0 {
		convert := func(info admin.PartitionInfo) int { return info.ID }
		partitions = convertSlice(topicInfo.Partitions, convert)
	}

	for _, partition := range partitions {
		_, ok := partitionIDsMap[partition]
		if !ok {
			format := "Partition %d not found in topic %s"
			return fmt.Errorf(format, partition, topic)
		}

		if strategy == "" {
			partitionOffsets[partition] = cfg.offset
			return nil
		}

		offset, err := groups.GetEarliestOrLatestOffset(ctx, connector, topic, strategy, partition)
		if err != nil {
			return err
		}

		partitionOffsets[partition] = offset
	}

	log.Infof(
		"This will reset the offsets for the following partitions "+
			"in topic %s for group %s:\n%s",
		topic,
		group,
		groups.FormatPartitionOffsets(partitionOffsets),
	)

	log.Info("Please ensure that all other consumers are stopped, " +
		"otherwise the reset might be overridden.")

	ok, _ := util.Confirm("OK to continue?", false)
	if !ok {
		return errors.New("Stopping because of user response")
	}

	cliRunner := cli.NewCLIRunner(adminClient, log.Infof, !noSpinner)

	if resetOffsetsConfig.delete {
		input := groups.DeleteOffsetsInput{
			GroupID:    group,
			Topic:      topic,
			Partitions: partitions,
		}

		return cliRunner.DeleteOffsets(ctx, &input)
	}

	return cliRunner.ResetOffsets(ctx, topic, group, partitionOffsets)
}

func numTrue(bools ...bool) int {
	var n int
	for _, b := range bools {
		if b {
			n++
		}
	}

	return n
}

func convertSlice[T1, T2 any](input []T1, fn func(T1) T2) []T2 {
	out := make([]T2, len(input))

	for i, v := range input {
		out[i] = fn(v)
	}

	return out
}

func parsePartitionOffsetMap(partitionIDsMap map[int]struct{}, input map[string]int64) (map[int]int64, error) {
	out := make(map[int]int64, len(input))

	for partition, offset := range input {
		partitionID, err := strconv.Atoi(partition)
		if err != nil {
			format := "Partition value %s must be an integer"
			return nil, fmt.Errorf(format, partition)
		}

		_, ok := partitionIDsMap[partitionID]
		if !ok {
			format := "Partition %d not found"
			return nil, fmt.Errorf(format, partitionID)
		}

		out[partitionID] = offset
	}

	return out, nil
}
