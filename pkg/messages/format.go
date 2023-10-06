package messages

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/segmentio/topicctl/pkg/util"
)

// FormatTailStats generates a pretty table from a TailStats instance.
func FormatTailStats(stats TailStats, filtered bool) string {
	buf := &bytes.Buffer{}

	table := tablewriter.NewWriter(buf)

	var headerNames []string

	if filtered {
		headerNames = []string{
			"Partition",
			"Messages Tailed\n(Total)",
			"Messages Tailed\n(Filtered)",
			"First Offset",
			"First Time",
			"Last Offset",
			"Last Time",
		}
	} else {
		headerNames = []string{
			"Partition",
			"Messages Tailed",
			"First Offset",
			"First Time",
			"Last Offset",
			"Last Time",
		}
	}

	table.SetHeader(headerNames)

	table.SetAutoWrapText(false)
	table.SetColumnAlignment(
		[]int{
			tablewriter.ALIGN_LEFT,
			tablewriter.ALIGN_LEFT,
			tablewriter.ALIGN_LEFT,
			tablewriter.ALIGN_LEFT,
			tablewriter.ALIGN_LEFT,
			tablewriter.ALIGN_LEFT,
		},
	)
	table.SetBorders(
		tablewriter.Border{
			Left:   false,
			Top:    true,
			Right:  false,
			Bottom: true,
		},
	)

	partitions := []int{}

	for partition, partitionStats := range stats.PartitionStats {
		if partitionStats.TotalMessages == 0 {
			continue
		}
		partitions = append(partitions, partition)
	}

	sort.Slice(partitions, func(a, b int) bool {
		return partitions[a] < partitions[b]
	})

	for _, partition := range partitions {
		partitionStats := stats.PartitionStats[partition]

		columnValues := []string{
			fmt.Sprintf("%d", partition),
			fmt.Sprintf("%d", partitionStats.TotalMessages),
		}

		if filtered {
			columnValues = append(
				columnValues,
				fmt.Sprintf("%d", partitionStats.TotalMessagesFiltered),
			)
		}

		columnValues = append(
			columnValues,
			fmt.Sprintf("%d", partitionStats.FirstOffset),
			partitionStats.FirstTime.Format(time.RFC3339),
			fmt.Sprintf("%d", partitionStats.LastOffset),
			partitionStats.LastTime.Format(time.RFC3339),
		)

		table.Append(columnValues)
	}

	table.Render()
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}

// FormatBounds makes a pretty table from the results of a GetAllPartitionBounds
// call.
func FormatBounds(boundsSlice []Bounds) string {
	buf := &bytes.Buffer{}

	table := tablewriter.NewWriter(buf)
	table.SetHeader(
		[]string{
			"Partition",
			"First Offset",
			"First Time",
			"Last Offset",
			"Last Time",
			"Messages",
			"Duration",
			"Avg Rate",
		},
	)
	table.SetAutoWrapText(false)
	table.SetColumnAlignment(
		[]int{
			tablewriter.ALIGN_LEFT,
			tablewriter.ALIGN_LEFT,
			tablewriter.ALIGN_LEFT,
			tablewriter.ALIGN_LEFT,
			tablewriter.ALIGN_LEFT,
			tablewriter.ALIGN_LEFT,
			tablewriter.ALIGN_LEFT,
			tablewriter.ALIGN_LEFT,
		},
	)
	table.SetBorders(
		tablewriter.Border{
			Left:   false,
			Top:    true,
			Right:  false,
			Bottom: true,
		},
	)

	for _, bounds := range boundsSlice {
		if bounds.FirstOffset == bounds.LastOffset {
			table.Append(
				[]string{
					fmt.Sprintf("%d", bounds.Partition),
					fmt.Sprintf("%d", bounds.FirstOffset),
					"",
					fmt.Sprintf("%d", bounds.LastOffset),
					"",
					"0",
					"",
					"",
				},
			)
		} else {
			numMessages := bounds.LastOffset - bounds.FirstOffset + 1
			duration := bounds.LastTime.Sub(bounds.FirstTime)

			table.Append(
				[]string{
					fmt.Sprintf("%d", bounds.Partition),
					fmt.Sprintf("%d", bounds.FirstOffset),
					bounds.FirstTime.Format(time.RFC3339),
					fmt.Sprintf("%d", bounds.LastOffset),
					bounds.LastTime.Format(time.RFC3339),
					fmt.Sprintf("%d", numMessages),
					util.PrettyDuration(duration),
					util.PrettyRate(numMessages, duration),
				},
			)
		}
	}

	table.Render()
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}

// FormatBoundTotals makes a pretty table from the totals across
// all partition results from a GetAllPartitionBounds call.
func FormatBoundTotals(boundsSlice []Bounds) string {
	buf := &bytes.Buffer{}

	table := tablewriter.NewWriter(buf)
	table.SetHeader(
		[]string{
			"Earliest Time",
			"Latest Time",
			"Total Messages",
			"Duration",
			"Avg Rate",
		},
	)
	table.SetAutoWrapText(false)
	table.SetColumnAlignment(
		[]int{
			tablewriter.ALIGN_LEFT,
			tablewriter.ALIGN_LEFT,
			tablewriter.ALIGN_LEFT,
			tablewriter.ALIGN_LEFT,
			tablewriter.ALIGN_LEFT,
			tablewriter.ALIGN_LEFT,
		},
	)
	table.SetBorders(
		tablewriter.Border{
			Left:   false,
			Top:    true,
			Right:  false,
			Bottom: true,
		},
	)

	var totalMessages int64
	var earliestTime time.Time
	var latestTime time.Time

	for _, bounds := range boundsSlice {
		if bounds.FirstOffset < bounds.LastOffset {
			totalMessages += bounds.LastOffset - bounds.FirstOffset + 1

			if earliestTime.IsZero() {
				earliestTime = bounds.FirstTime
			} else if bounds.FirstTime.Before(earliestTime) {
				earliestTime = bounds.FirstTime
			}

			if latestTime.IsZero() {
				latestTime = bounds.LastTime
			} else if bounds.LastTime.After(latestTime) {
				latestTime = bounds.LastTime
			}
		}
	}

	if totalMessages == 0 {
		// Don't bother printing a table if there are no messages
		return ""
	}

	duration := latestTime.Sub(earliestTime)

	table.Append(
		[]string{
			earliestTime.Format(time.RFC3339),
			latestTime.Format(time.RFC3339),
			fmt.Sprintf("%d", totalMessages),
			util.PrettyDuration(duration),
			util.PrettyRate(totalMessages, duration),
		},
	)

	table.Render()
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}
