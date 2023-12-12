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
	beforeEarliest     bool
	afterLatest        bool
	toEarliest         bool
	toLatest           bool
	delete             bool

	shared sharedOptions
}

var resetOffsetsConfig resetOffsetsCmdConfig

func init() {
	cfg := &resetOffsetsConfig
	cmd := resetOffsetsCmd
	flags := cmd.Flags()

	flags.Int64Var(
		&cfg.offset,
		"offset",
		-2,
		"Desired offset for the target partitions",
	)
	flags.IntSliceVar(
		&cfg.partitions,
		"partitions",
		[]int{},
		"List of partitions to reset e.g. 1,2,3,.. (defaults to all)",
	)
	flags.StringToInt64Var(
		&cfg.partitionOffsetMap,
		"partition-offset-map",
		map[string]int64{},
		"Map of partition IDs to their corresponding desired offsets e.g. 1=5,2=10,3=12,...",
	)
	flags.BoolVar(
		&cfg.beforeEarliest,
		"before-earliest",
		false,
		"Apply only to offsets below the partition minimum",
	)
	flags.BoolVar(
		&cfg.afterLatest,
		"after-latest",
		false,
		"Apply only to offsets above the partition maximum",
	)
	flags.BoolVar(
		&cfg.toEarliest,
		"to-earliest",
		false,
		"Resets offsets of consumer group members to earliest offsets of partitions",
	)
	flags.BoolVar(
		&cfg.toLatest,
		"to-latest",
		false,
		"Resets offsets of consumer group members to latest offsets of partitions",
	)
	flags.BoolVar(
		&cfg.delete,
		"delete",
		false,
		"Deletes offsets for the given consumer group",
	)

	addSharedFlags(cmd, &resetOffsetsConfig.shared)
	RootCmd.AddCommand(cmd)
}

func resetOffsetsPreRun(cmd *cobra.Command, args []string) error {
	const (
		resetOffsetSpec = "You must choose only one of the following " +
			"reset-offset specifications: --delete, --to-earliest, --to-latest, " +
			"--offset, or --partition-offset-map"
		offsetMapSpec = "--partition-offset-map option cannot be used with " +
			"--partitions, --before-earliest, or --after-latest"
		rangeSpec = "--before-earliest cannot be combined with --after-latest"
	)

	cfg := resetOffsetsConfig

	hasMap := len(cfg.partitionOffsetMap) > 0
	hasSlice := len(cfg.partitions) > 0

	numOffsetSpecs := numTrue(
		cfg.toEarliest,
		cfg.toLatest,
		cfg.delete,
		cmd.Flags().Changed("offset"),
		hasMap,
	)

	if numOffsetSpecs > 1 {
		return errors.New(resetOffsetSpec)
	}

	if cfg.beforeEarliest && cfg.afterLatest {
		return errors.New(rangeSpec)
	}

	if hasMap && hasSlice {
		return errors.New(offsetMapSpec)
	}

	if numTrue(hasMap, cfg.beforeEarliest, cfg.afterLatest) > 1 {
		return errors.New(offsetMapSpec)
	}

	return cfg.shared.validate()
}

func resetOffsetsRun(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topic := args[0]
	group := args[1]

	cfg := resetOffsetsConfig

	adminClient, err := cfg.shared.getAdminClient(ctx, nil, true)
	if err != nil {
		return err
	}

	defer adminClient.Close()

	connector := adminClient.GetConnector()

	getLagsInput := groups.GetMemberLagsInput{
		GroupID: group,
		Topic:   topic,

		// We need partition-accurate range bounds,
		// but don't care about consumer-group message timings.
		FullRange: true,
	}

	partitionLags, err := groups.GetMemberLags(ctx, connector, &getLagsInput)
	if err != nil {
		return err
	}

	infoByPartition := sliceToMapKeyFunc(partitionLags, func(v *groups.MemberPartitionLag) int { return v.Partition })

	// If explicit per-partition offsets were specified, set them now.
	partitionOffsets, err := parsePartitionOffsetMap(infoByPartition, cfg.partitionOffsetMap)
	if err != nil {
		return err
	}

	// Set explicit partitions (without offsets) if specified,
	// otherwise operate on fetched partition info;
	// these will only take effect of per-partition offsets were not specified.
	partitions := cfg.partitions
	if len(partitions) == 0 && len(partitionOffsets) == 0 {
		partitions = mapKeys(infoByPartition)
	}

	// Re-append applicable partitions back over the same slice.
	n := len(partitions)
	partitions = partitions[:0]

	for _, partition := range partitions[:n] {
		info := infoByPartition[partition]
		if info == nil {
			format := "Partition %d not found in topic %s"
			return fmt.Errorf(format, partition, topic)
		}

		// Skip partitions with in-range group offsets.
		switch {
		case cfg.beforeEarliest && info.MemberOffset >= info.OldestOffset:
			continue
		case cfg.afterLatest && info.MemberOffset <= info.NewestOffset:
			continue
		}

		partitions = append(partitions, partition)

		offset := cfg.offset
		switch {
		case cfg.delete:
			continue // storing an offset is not applicable when deleting.
		case cfg.toEarliest:
			offset = info.OldestOffset
		case cfg.toLatest:
			offset = info.NewestOffset
		}

		partitionOffsets[partition] = offset
	}

	if cfg.delete {

	}

	message := "This will reset the offsets for the following partitions " +
		"in topic %s for group %s:\n%s"
	formatTable := func() string { return groups.FormatPartitionOffsets(partitionOffsets) }

	if cfg.delete {
		message = "This will delete the offsets for the following partitions " +
			"in topic %s for group %s:\n%s"
		formatTable = func() string { return groups.FormatPartitions(partitions) }
	}

	log.Infof(message, topic, group, formatTable())

	// Stopping consumers is typically only relevant to resets,
	// since deleting offsets is usually just for unblocking stuck partitions:
	// if the group offset for a partition is being actively updated,
	// then it's not stuck.
	if !cfg.delete {
		log.Info("Please ensure that all other consumers are stopped, " +
			"otherwise the reset might be overridden.")
	}

	ok, _ := apply.Confirm("OK to continue?", false)
	if !ok {
		return errors.New("Stopping because of user response")
	}

	cliRunner := cli.NewCLIRunner(adminClient, log.Infof, !noSpinner)

	if cfg.delete {
		input := groups.DeleteOffsetsInput{
			GroupID:    group,
			Topic:      topic,
			Partitions: partitions,
		}

		return cliRunner.DeleteOffsets(ctx, &input)
	}

	input := groups.ResetOffsetsInput{
		GroupID:          group,
		Topic:            topic,
		PartitionOffsets: partitionOffsets,
	}

	return cliRunner.ResetOffsets(ctx, &input)
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

func mapKeys[K comparable, V any](m map[K]V) []K {
	s := make([]K, 0, len(m))

	for k := range m {
		s = append(s, k)
	}

	return s
}

func sliceToMapKeyFunc[K comparable, V any](s []V, fn func(*V) K) map[K]*V {
	return sliceToMapFunc(s, func(v *V) (K, *V) { return fn(v), v })
}

func sliceToMapFunc[K comparable, V1, V2 any](s []V1, fn func(*V1) (K, V2)) map[K]V2 {
	m := make(map[K]V2, len(s))

	for i := range s {
		v1 := &s[i]
		k, v2 := fn(v1)
		m[k] = v2
	}

	return m
}

func convertSlice[T1, T2 any](input []T1, fn func(T1) T2) []T2 {
	out := make([]T2, len(input))

	for i, v := range input {
		out[i] = fn(v)
	}

	return out
}

func parsePartitionOffsetMap[T any](partitionIDsMap map[int]T, input map[string]int64) (map[int]int64, error) {
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
