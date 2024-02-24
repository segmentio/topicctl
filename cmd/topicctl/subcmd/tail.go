package subcmd

import (
	"context"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/cli"
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
	offset     int64
	partitions []int
	raw        bool
	headers    bool
	groupID    string

	shared sharedOptions
}

var tailConfig tailCmdConfig

func init() {
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
	tailCmd.Flags().BoolVar(
		&tailConfig.headers,
		"headers",
		true,
		"Output message headers",
	)
	tailCmd.Flags().StringVar(
		&tailConfig.groupID,
		"group-id",
		"",
		"Consumer group ID to tail with",
	)

	addSharedFlags(tailCmd, &tailConfig.shared)
	RootCmd.AddCommand(tailCmd)
}

func tailPreRun(cmd *cobra.Command, args []string) error {
	if tailConfig.raw {
		// In raw mode, only log out errors
		log.SetLevel(log.ErrorLevel)
	}
	return tailConfig.shared.validate()
}

func tailRun(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	adminClient, err := tailConfig.shared.getAdminClient(ctx, nil, true)
	if err != nil {
		return err
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
		tailConfig.headers,
		tailConfig.groupID,
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
