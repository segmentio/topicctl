package groups

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	"github.com/segmentio/topicctl/pkg/util"
)

// FormatGroupCoordinators generates a pretty table from the results of a call to GetGroups.
func FormatGroupCoordinators(groupCoordinators []GroupCoordinator) string {
	buf := &bytes.Buffer{}

	table := tablewriter.NewWriter(buf)
	table.SetHeader(
		[]string{
			"Group",
			"Coordinator",
		},
	)
	table.SetAutoWrapText(false)
	table.SetColumnAlignment(
		[]int{
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

	for _, groupCoordinator := range groupCoordinators {
		table.Append(
			[]string{
				groupCoordinator.GroupID,
				fmt.Sprintf("%d", groupCoordinator.Coordinator),
			},
		)
	}

	table.Render()
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}

// FormatGroupMembers generates a pretty table from a slice of MemberInfo details.
func FormatGroupMembers(members []MemberInfo, full bool) string {
	buf := &bytes.Buffer{}

	table := tablewriter.NewWriter(buf)
	table.SetHeader(
		[]string{
			"Member ID",
			"Client Host",
			"Partition\nAssignments",
		},
	)
	table.SetAutoWrapText(true)
	table.SetColumnAlignment(
		[]int{
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

	for _, member := range members {
		var clientHost string
		if strings.HasPrefix(member.ClientHost, "/") {
			clientHost = member.ClientHost[1:]
		} else {
			clientHost = member.ClientHost
		}

		var memberID string
		if full {
			memberID = member.MemberID
		} else {
			memberID, _ = util.TruncateStringMiddle(member.MemberID, 40, 5)
		}

		table.Append(
			[]string{
				memberID,
				clientHost,
				fmt.Sprintf("%+v", member.TopicPartitions),
			},
		)
	}

	table.Render()
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}

// FormatMemberLags generates a pretty table from the results of GetMemberLags.
func FormatMemberLags(memberLags []MemberPartitionLag) string {
	buf := &bytes.Buffer{}

	table := tablewriter.NewWriter(buf)
	table.SetHeader(
		[]string{
			"Partition",
			"Member ID",
			"Member Offset",
			"Member Time",
			"Latest Offset",
			"Latest Time",
			"Offset Lag",
			"Time Lag",
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

	for _, memberLag := range memberLags {
		var memberID string

		if memberLag.MemberID != "" {
			memberID, _ = util.TruncateStringMiddle(memberLag.MemberID, 30, 5)
		}

		var memberIDPrinter func(f string, a ...interface{}) string
		if !util.InTerminal() || memberID != "" {
			memberIDPrinter = fmt.Sprintf
		} else {
			memberID = "None"
			memberIDPrinter = color.New(color.FgRed).SprintfFunc()
		}

		var memberTimeStr string
		var timeLagStr string

		// For whatever reason, the time on the last member message sometimes isn't properly set;
		// only show this and the time lag if it's set.
		if !memberLag.MemberTime.IsZero() {
			memberTimeStr = memberLag.MemberTime.Format(time.RFC3339)
			timeLagStr = prettyDuration(memberLag.TimeLag())
		}

		table.Append(
			[]string{
				fmt.Sprintf("%d", memberLag.Partition),
				memberIDPrinter("%s", memberID),
				fmt.Sprintf("%d", memberLag.MemberOffset),
				memberTimeStr,
				fmt.Sprintf("%d", memberLag.NewestOffset),
				memberLag.NewestTime.Format(time.RFC3339),
				fmt.Sprintf("%d", memberLag.OffsetLag()),
				timeLagStr,
			},
		)
	}

	table.Render()
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}

func FormatPartitionOffsets(partitionOffsets map[int]int64) string {
	buf := &bytes.Buffer{}

	table := tablewriter.NewWriter(buf)
	table.SetHeader(
		[]string{
			"Partition",
			"New Offset",
		},
	)
	table.SetAutoWrapText(false)
	table.SetColumnAlignment(
		[]int{
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

	partitionIDs := []int{}
	for partitionID := range partitionOffsets {
		partitionIDs = append(partitionIDs, partitionID)
	}

	sort.Slice(partitionIDs, func(a, b int) bool {
		return partitionIDs[a] < partitionIDs[b]
	})

	for _, partitionID := range partitionIDs {
		table.Append(
			[]string{
				fmt.Sprintf("%d", partitionID),
				fmt.Sprintf("%d", partitionOffsets[partitionID]),
			},
		)
	}

	table.Render()
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}

func prettyDuration(duration time.Duration) string {
	seconds := duration.Seconds()

	if seconds < 1.0 {
		return fmt.Sprintf("%dms", duration.Milliseconds())
	} else if seconds < 240.0 {
		return fmt.Sprintf("%ds", int(seconds))
	} else if seconds < (2.0 * 60.0 * 60.0) {
		return fmt.Sprintf("%dm", int(duration.Minutes()))
	} else {
		return fmt.Sprintf("%dh", int(duration.Hours()))
	}
}
