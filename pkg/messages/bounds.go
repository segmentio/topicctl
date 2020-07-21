package messages

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/segmentio/kafka-go"
	_ "github.com/segmentio/kafka-go/snappy"
	log "github.com/sirupsen/logrus"
)

const (
	maxMessageSizeBytes = 100000

	// Number of partitions to get bounds from in parallel
	numWorkers = 20

	// Parameters for backoff when there are connection errors
	maxRetries               = 4
	backoffInitSleepDuration = 200 * time.Millisecond

	// Connection timeout
	connTimeout = 10 * time.Second
)

// Bounds represents the start and end "bounds" of the messages in
// a partition.
type Bounds struct {
	Partition   int
	FirstTime   time.Time
	FirstOffset int64
	LastTime    time.Time
	LastOffset  int64
}

// GetAllPartitionBounds gets the bounds for all partitions in the argument topic.
// The start of each bound is based on the value in the baseOffsets map or, if this
// is nil, the starting offset in each topic partition.
func GetAllPartitionBounds(
	ctx context.Context,
	brokerAddr string,
	topic string,
	baseOffsets map[int]int64,
) ([]Bounds, error) {
	conn, err := kafka.DefaultDialer.DialContext(ctx, "tcp", brokerAddr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return nil, err
	}

	partitionsChan := make(chan kafka.Partition, len(partitions))
	for i := 0; i < len(partitions); i++ {
		partitionsChan <- partitions[i]
	}
	defer close(partitionsChan)

	type wrappedBounds struct {
		bounds Bounds
		err    error
	}

	resultsChan := make(chan wrappedBounds, len(partitions))
	for i := 0; i < numWorkers; i++ {
		go func() {
			for {
				nextPartition, ok := <-partitionsChan
				if !ok {
					return
				}

				var minOffset int64

				if baseOffset, ok := baseOffsets[nextPartition.ID]; ok {
					minOffset = baseOffset
				} else {
					minOffset = -2
				}

				bounds, err := GetPartitionBounds(
					ctx,
					brokerAddr,
					topic,
					nextPartition.ID,
					minOffset,
				)
				resultsChan <- wrappedBounds{
					bounds: bounds,
					err:    err,
				}
			}
		}()
	}

	allBounds := []Bounds{}

	for i := 0; i < len(partitions); i++ {
		select {
		case result := <-resultsChan:
			if result.err != nil {
				log.Warnf("Error reading result: %+v", result.err)
			} else {
				allBounds = append(allBounds, result.bounds)
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	sort.Slice(allBounds, func(a, b int) bool {
		return allBounds[a].Partition < allBounds[b].Partition
	})

	return allBounds, nil
}

// GetPartitionBounds gets the bounds for a single partition in the argument topic.
// It does this by dialing the leader of the partition and then reading the first
// and last messages. If the provided minOffset is greater than the first offset,
// this is used instead of the actual first offset.
func GetPartitionBounds(
	ctx context.Context,
	brokerAddr string,
	topic string,
	partition int,
	minOffset int64,
) (Bounds, error) {
	log.Debugf(
		"Getting bounds for topic %s, partition %d with minOffset %d",
		topic,
		partition,
		minOffset,
	)

	conn, err := dialLeaderRetries(ctx, brokerAddr, topic, partition)
	if err != nil {
		return Bounds{}, err
	}
	defer conn.Close()

	firstOffset, lastOffset, err := conn.ReadOffsets()
	if err != nil {
		return Bounds{}, fmt.Errorf(
			"Error getting offsets for partition %d: %+v",
			partition,
			err,
		)
	}
	log.Debugf(
		"Got offsets for %d: %d->%d",
		partition,
		firstOffset,
		lastOffset,
	)

	if firstOffset == lastOffset {
		// This means that there's no data in the partition
		return Bounds{
			Partition:   partition,
			FirstOffset: firstOffset,
			LastOffset:  lastOffset,
		}, nil
	}

	if minOffset > firstOffset {
		log.Debugf(
			"Moving first offset forward to match min offset (%d)",
			minOffset,
		)
		firstOffset = minOffset
	}

	var firstMessage kafka.Message

	_, err = conn.Seek(firstOffset, kafka.SeekAbsolute|kafka.SeekDontCheck)
	if err != nil {
		return Bounds{}, fmt.Errorf(
			"Error seeking for partition %d at first offset (%d): %+v",
			partition,
			firstOffset,
			err,
		)
	}

	firstMessage, err = conn.ReadMessage(maxMessageSizeBytes)
	if err != nil {
		return Bounds{}, fmt.Errorf(
			"Error reading first message for partition %d (offset %d): %+v",
			partition,
			firstOffset,
			err,
		)
	}

	// Use a separate connection for reading the last message. For whatever reason,
	// reusing the same connection with kafka-go on newer kafka versions can lead to read errors.
	conn2, err := dialLeaderRetries(ctx, brokerAddr, topic, partition)
	if err != nil {
		return Bounds{}, err
	}
	defer conn2.Close()

	// Subtract 1 from the last offset to get the last message
	_, err = conn2.Seek(lastOffset-1, kafka.SeekAbsolute|kafka.SeekDontCheck)
	if err != nil {
		return Bounds{}, fmt.Errorf(
			"Error seeking for partition %d at last offset (%d): %+v",
			partition,
			lastOffset,
			err,
		)
	}

	lastMessage, err := conn2.ReadMessage(maxMessageSizeBytes)
	if err != nil {
		return Bounds{}, fmt.Errorf(
			"Error reading last message for partition %d (offset %d): %+v",
			partition,
			lastOffset,
			err,
		)
	}

	return Bounds{
		Partition:   partition,
		FirstOffset: firstMessage.Offset,
		FirstTime:   firstMessage.Time,
		LastOffset:  lastMessage.Offset,
		LastTime:    lastMessage.Time,
	}, nil
}

func dialLeaderRetries(
	ctx context.Context,
	brokerAddr string,
	topic string,
	partition int,
) (*kafka.Conn, error) {
	var conn *kafka.Conn
	var err error

	sleepDuration := backoffInitSleepDuration

	for i := 0; i < maxRetries; i++ {
		conn, err = kafka.DialLeader(ctx, "tcp", brokerAddr, topic, partition)
		if err == nil {
			break
		}
		log.Debugf("Error dialing partition %d: %+v; retrying", partition, err)

		time.Sleep(sleepDuration)
		sleepDuration *= 2
	}

	if err != nil {
		return nil, fmt.Errorf("Error dialing partition %d: %+v", partition, err)
	}

	conn.SetDeadline(time.Now().Add(connTimeout))
	return conn, nil
}
