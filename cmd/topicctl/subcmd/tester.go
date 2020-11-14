package subcmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/apply"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var testerCmd = &cobra.Command{
	Use:     "tester",
	Short:   "tester reads or writes test events to a cluster",
	PreRunE: testerPreRun,
	RunE:    testerRun,
}

type testerCmdConfig struct {
	brokerAddr   string
	mode         string
	readConsumer string
	topic        string
	writeRate    int
	zkAddr       string
}

var testerConfig testerCmdConfig

func init() {
	testerCmd.Flags().StringVar(
		&testerConfig.brokerAddr,
		"broker-addr",
		"",
		"Broker address",
	)
	testerCmd.Flags().StringVar(
		&testerConfig.mode,
		"mode",
		"writer",
		"Tester mode (one of 'reader', 'writer')",
	)
	testerCmd.Flags().StringVar(
		&testerConfig.readConsumer,
		"read-consumer",
		"test-consumer",
		"Consumer group ID for reads; if blank, no consumer group is set",
	)
	testerCmd.Flags().StringVar(
		&testerConfig.topic,
		"topic",
		"",
		"Topic to write to",
	)
	testerCmd.Flags().IntVar(
		&testerConfig.writeRate,
		"write-rate",
		5,
		"Approximate number of messages to write per sec",
	)
	testerCmd.Flags().StringVar(
		&testerConfig.zkAddr,
		"zk-addr",
		"localhost:2181",
		"Zookeeper address",
	)

	testerCmd.MarkFlagRequired("topic")

	RootCmd.AddCommand(testerCmd)
}

func testerPreRun(cmd *cobra.Command, args []string) error {
	if testerConfig.zkAddr == "" && tailConfig.brokerAddr == "" {
		return errors.New("Must set either broker-addr or zk-addr")
	}
	return nil
}

func testerRun(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	switch testerConfig.mode {
	case "reader":
		return runTestReader(ctx)
	case "writer":
		return runTestWriter(ctx)
	default:
		return fmt.Errorf("Mode must be set to either 'reader' or 'writer'")
	}
}

func runTestReader(ctx context.Context) error {
	brokerAddr, err := getBrokerAddr(ctx)
	if err != nil {
		return err
	}

	log.Infof(
		"This will read test messages from the '%s' topic in %s using the consumer group ID '%s'",
		testerConfig.topic,
		brokerAddr,
		testerConfig.readConsumer,
	)

	ok, _ := apply.Confirm("OK to continue?", false)
	if !ok {
		return errors.New("Stopping because of user response")
	}

	reader := kafka.NewReader(
		kafka.ReaderConfig{
			Brokers:     []string{brokerAddr},
			GroupID:     testerConfig.readConsumer,
			Topic:       testerConfig.topic,
			MinBytes:    10e3, // 10KB
			MaxBytes:    10e6, // 10MB
			StartOffset: kafka.LastOffset,
		},
	)

	log.Info("Starting read loop")

	for {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			return err
		}
		log.Infof(
			"Message at partition %d, offset %d: %s=%s",
			message.Partition,
			message.Offset,
			string(message.Key),
			string(message.Value),
		)
	}
}

func runTestWriter(ctx context.Context) error {
	brokerAddr, err := getBrokerAddr(ctx)
	if err != nil {
		return err
	}

	log.Infof(
		"This will write test messages to the '%s' topic in %s at a rate of %d/sec.",
		testerConfig.topic,
		brokerAddr,
		testerConfig.writeRate,
	)

	ok, _ := apply.Confirm("OK to continue?", false)
	if !ok {
		return errors.New("Stopping because of user response")
	}

	writer := kafka.NewWriter(
		kafka.WriterConfig{
			Brokers:       []string{brokerAddr},
			Topic:         testerConfig.topic,
			Balancer:      &kafka.LeastBytes{},
			Async:         true,
			QueueCapacity: 5,
			BatchSize:     5,
		},
	)
	defer writer.Close()

	index := 0
	tickDuration := time.Duration(1000.0/float64(testerConfig.writeRate)) * time.Millisecond
	sendTicker := time.NewTicker(tickDuration)
	logTicker := time.NewTicker(5 * time.Second)

	log.Info("Starting write loop")

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-sendTicker.C:
			err := writer.WriteMessages(
				ctx,
				kafka.Message{
					Key:   []byte(fmt.Sprintf("msg_%d", index)),
					Value: []byte(fmt.Sprintf("Contents of test message %d", index)),
				},
			)
			if err != nil {
				return err
			}
			index++
		case <-logTicker.C:
			log.Infof("%d messages sent", index)
		}
	}
}

func getBrokerAddr(ctx context.Context) (string, error) {
	if testerConfig.brokerAddr == "" {
		adminClient, err := admin.NewZKAdminClient(
			ctx,
			admin.ZKAdminClientConfig{
				ZKAddrs: []string{testerConfig.zkAddr},
			},
		)
		if err != nil {
			return "", err
		}
		return adminClient.GetBootstrapAddrs()[0], nil
	} else {
		return testerConfig.brokerAddr, nil
	}
}
