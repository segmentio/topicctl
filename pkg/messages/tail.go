package messages

import (
	"context"
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/fatih/color"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/util"
	log "github.com/sirupsen/logrus"

	// Read snappy-compressed messages
	_ "github.com/segmentio/kafka-go/snappy"

	// Read zstd-encoded messages
	_ "github.com/segmentio/kafka-go/zstd"
)

// TopicTailer fetches a stream of messages from a topic.
type TopicTailer struct {
	Connector  *admin.Connector
	topic      string
	partitions []int
	offset     int64
	minBytes   int
	maxBytes   int
}

// NewTopicTailer returns a new TopicTailer instance.
func NewTopicTailer(
	Connector *admin.Connector,
	topic string,
	partitions []int,
	offset int64,
	minBytes int,
	maxBytes int,
) *TopicTailer {
	return &TopicTailer{
		Connector:  Connector,
		topic:      topic,
		partitions: partitions,
		offset:     offset,
		minBytes:   minBytes,
		maxBytes:   maxBytes,
	}
}

// TailMessage represents a single message retrieved from a kafka reader.
type TailMessage struct {
	Message   kafka.Message
	Partition int
	Err       error
}

// TailStats stores stats on all partitions that are tailed.
type TailStats struct {
	PartitionStats map[int]*TailPartitionStats
}

// TailPartitionStats stores stats on the fetches from a single topic
// partition.
type TailPartitionStats struct {
	TotalErrors               int
	TotalMessages             int
	TotalMessageBytes         int64
	TotalMessagesFiltered     int
	TotalMessageBytesFiltered int64
	FirstOffset               int64
	FirstTime                 time.Time
	LastOffset                int64
	LastTime                  time.Time
}

// GetMessages gets a stream of messages from the tailer. These are passed
// back through the argument channel.
func (t *TopicTailer) GetMessages(
	ctx context.Context,
	messagesChan chan TailMessage,
) {
	readers := []*kafka.Reader{}

	for _, partition := range t.partitions {
		reader := kafka.NewReader(
			kafka.ReaderConfig{
				Brokers:        []string{t.Connector.Config.BrokerAddr},
				Dialer:         t.Connector.Dialer,
				Topic:          t.topic,
				Partition:      partition,
				MinBytes:       t.minBytes,
				MaxBytes:       t.maxBytes,
				ReadBackoffMin: 200 * time.Millisecond,
				ReadBackoffMax: 3 * time.Second,
				MaxAttempts:    5,
			},
		)

		reader.SetOffset(t.offset)
		readers = append(readers, reader)
	}

	for _, reader := range readers {
		go func(r *kafka.Reader) {
			log.Debugf(
				"Starting read loop for partition %d",
				r.Config().Partition,
			)
			defer r.Close()

			for {
				message, err := r.ReadMessage(ctx)

				if err != nil {
					partition := r.Config().Partition

					if strings.Contains(err.Error(), "connection reset") ||
						strings.Contains(err.Error(), "broken pipe") ||
						strings.Contains(err.Error(), "i/o timeout") {
						// These errors are recoverable, just try again
						log.Warnf(
							"Got connection error reading from partition %d, retrying: %+v",
							partition,
							err,
						)
						continue
					} else {
						// Any other error will cause the reader to stop
						messagesChan <- TailMessage{
							Partition: r.Config().Partition,
							Err:       err,
						}
						return
					}
				}

				if message.Topic != t.topic {
					// At the start, we sometimes get a junk message with an
					// empty topic
					continue
				}

				messagesChan <- TailMessage{
					Message:   message,
					Partition: r.Config().Partition,
					Err:       nil,
				}
			}
		}(reader)
	}
}

// LogMessages logs out the message stream from the tailer. It returns stats
// from the tail run that can be displayed by the caller after the context is cancelled or
// maxMessages messages have been tailed.
func (t *TopicTailer) LogMessages(
	ctx context.Context,
	maxMessages int,
	filterRegexp string,
	raw bool,
	headers bool,
) (TailStats, error) {
	var filterRegexpObj *regexp.Regexp
	var err error
	if filterRegexp != "" {
		filterRegexpObj, err = regexp.Compile(filterRegexp)
		if err != nil {
			return TailStats{}, err
		}
	}

	messagesChan := make(chan TailMessage, 100)
	t.GetMessages(ctx, messagesChan)

	stats := TailStats{
		PartitionStats: map[int]*TailPartitionStats{},
	}

	for _, partition := range t.partitions {
		stats.PartitionStats[partition] = &TailPartitionStats{}
	}

	for {
		select {
		case <-ctx.Done():
			return stats, ctx.Err()
		case tailMessage := <-messagesChan:
			partition := tailMessage.Partition
			partitionStats := stats.PartitionStats[partition]

			if tailMessage.Err != nil {
				log.Warnf("Got error: %+v", tailMessage.Err)
				partitionStats.TotalErrors++
				continue
			}

			if partitionStats.TotalMessages == 0 {
				partitionStats.FirstOffset = tailMessage.Message.Offset
				partitionStats.FirstTime = tailMessage.Message.Time
			}

			partitionStats.TotalMessages++
			partitionStats.TotalMessageBytes += int64(len(tailMessage.Message.Key))
			partitionStats.TotalMessageBytes += int64(len(tailMessage.Message.Value))
			partitionStats.LastOffset = tailMessage.Message.Offset
			partitionStats.LastTime = tailMessage.Message.Time

			if filterRegexpObj != nil && !filterRegexpObj.Match(tailMessage.Message.Value) {
				continue
			}

			partitionStats.TotalMessagesFiltered++
			partitionStats.TotalMessageBytesFiltered += int64(len(tailMessage.Message.Key))
			partitionStats.TotalMessageBytesFiltered += int64(len(tailMessage.Message.Value))

			if raw {
				fmt.Printf("%s\n", string(tailMessage.Message.Value))
				continue
			}

			var dividerPrinter func(f string, a ...interface{}) string
			var keyPrinter func(f string, a ...interface{}) string
			var valuePrinter func(f string, a ...interface{}) string
			var messagePrinter func(f string, a ...interface{}) string

			if !util.InTerminal() {
				dividerPrinter = fmt.Sprintf
				keyPrinter = fmt.Sprintf
				valuePrinter = fmt.Sprintf
				messagePrinter = fmt.Sprintf
			} else {
				dividerPrinter = color.New(color.FgGreen, color.Faint).SprintfFunc()
				keyPrinter = color.New(color.FgCyan, color.Bold).SprintfFunc()
				valuePrinter = color.New(color.FgYellow).SprintfFunc()
				messagePrinter = fmt.Sprintf
			}

			fmt.Println(
				dividerPrinter("======================================================="),
			)
			fmt.Printf(
				"%s %s\n",
				keyPrinter("Partition:"),
				valuePrinter("%d", tailMessage.Message.Partition),
			)
			fmt.Printf(
				"%s %s\n",
				keyPrinter("Offset:   "),
				valuePrinter("%d", tailMessage.Message.Offset),
			)
			fmt.Printf(
				"%s %s\n",
				keyPrinter("Time:     "),
				valuePrinter(tailMessage.Message.Time.Format(time.RFC3339)),
			)
			if headers {
				fmt.Printf(
					"%s %s\n",
					keyPrinter("Headers:  "),
					valuePrinter(formatHeaders(tailMessage.Message.Headers)),
				)
			}
			fmt.Printf(
				"%s %s\n",
				keyPrinter("Key:      "),
				valuePrinter(bytesToStr(tailMessage.Message.Key)),
			)
			fmt.Printf(
				"%s %s\n",
				keyPrinter("Value:    "),
				messagePrinter(bytesToStr(tailMessage.Message.Value)),
			)

			if maxMessages > 0 && partitionStats.TotalMessages >= maxMessages {
				return stats, nil
			}
		}
	}
}

// bytesToStr makes a screen-printable version of a byte sequence.
func bytesToStr(input []byte) string {
	if utf8.Valid(input) {
		return strings.TrimSpace(string(input))
	}
	return fmt.Sprintf("Binary [%+v]", input)
}

// formatHeaders creates a joined string with all the header keys and their
// base64-encoded values.
func formatHeaders(headers []kafka.Header) string {
	builder := strings.Builder{}
	for i, h := range headers {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(h.Key)
		builder.WriteString("=")
		builder.WriteString(base64.StdEncoding.EncodeToString(h.Value))
	}
	return builder.String()
}
