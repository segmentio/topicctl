package admin

import (
	"bytes"
	"cmp"
	"fmt"
	"math"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	"github.com/segmentio/topicctl/pkg/util"
)

// FormatBrokers creates a pretty table from a list of brokers.
func FormatBrokers(brokers []BrokerInfo, full bool) string {
	buf := &bytes.Buffer{}

	table := tablewriter.NewWriter(buf)

	var hasInstances bool
	for _, broker := range brokers {
		if broker.InstanceID != "" {
			hasInstances = true
			break
		}
	}

	headers := []string{
		"ID",
		"Host",
		"Port",
	}

	if hasInstances {
		headers = append(
			headers,
			"Instance",
			"Instance\nType",
		)
	}

	headers = append(
		headers,
		"Rack",
		"Timestamp",
	)

	if full {
		headers = append(headers, "Config")
	}

	table.SetHeader(headers)

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

	for _, broker := range brokers {
		row := []string{
			fmt.Sprintf("%d", broker.ID),
			broker.Host,
			fmt.Sprintf("%d", broker.Port),
		}

		if hasInstances {
			row = append(
				row,
				broker.InstanceID,
				broker.InstanceType,
			)
		}

		row = append(
			row,
			broker.Rack,
			broker.Timestamp.UTC().Format(time.RFC3339),
		)

		if full {
			row = append(row, prettyConfig(broker.Config))
		}

		table.Append(row)
	}

	table.Render()
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}

// FormatBrokerReplicas creates a pretty table that shows how many replicas are in each
// position (i.e., leader, second, third) by broker across all topics. Useful for showing
// total-topic balance.
func FormatBrokerReplicas(brokers []BrokerInfo, topics []TopicInfo) string {
	buf := &bytes.Buffer{}

	table := tablewriter.NewWriter(buf)

	maxReplicas := MaxReplication(topics)
	hasLeaders := HasLeaders(topics)

	headers := []string{
		"ID",
		"Rack",
	}

	if hasLeaders {
		headers = append(headers, "Leader")
	}

	for p := 0; p < maxReplicas; p++ {
		headers = append(headers, fmt.Sprintf("Position %d", p+1))
	}
	headers = append(headers, "Total")

	table.SetHeader(headers)

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

	brokerLeaders := map[int]int{}
	brokerPositions := map[int][]int{}

	for _, broker := range brokers {
		brokerPositions[broker.ID] = []int{}
		for p := 0; p < maxReplicas; p++ {
			brokerPositions[broker.ID] = append(brokerPositions[broker.ID], 0)
		}
	}
	for _, topic := range topics {
		for _, partition := range topic.Partitions {
			brokerLeaders[partition.Leader]++

			for r, replica := range partition.Replicas {
				brokerPositions[replica][r]++
			}
		}
	}

	for _, broker := range brokers {
		row := []string{
			fmt.Sprintf("%d", broker.ID),
			broker.Rack,
		}

		if hasLeaders {
			row = append(
				row,
				fmt.Sprintf("%d", brokerLeaders[broker.ID]),
			)
		}

		total := 0

		for p := 0; p < maxReplicas; p++ {
			row = append(
				row,
				fmt.Sprintf("%d", brokerPositions[broker.ID][p]),
			)
			total += brokerPositions[broker.ID][p]
		}
		row = append(row, fmt.Sprintf("%d", total))

		table.Append(row)
	}

	table.Render()
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}

// FormatBrokerRackReplicas creates a pretty table that shows how many replicas are in each
// position (i.e., leader, second, third) by rack across all topics. Useful for showing
// total-topic balance.
func FormatBrokerRackReplicas(brokers []BrokerInfo, topics []TopicInfo) string {
	buf := &bytes.Buffer{}

	table := tablewriter.NewWriter(buf)

	maxReplicas := MaxReplication(topics)
	hasLeaders := HasLeaders(topics)

	headers := []string{
		"Rack",
	}

	if hasLeaders {
		headers = append(headers, "Leader")
	}

	for p := 0; p < maxReplicas; p++ {
		headers = append(headers, fmt.Sprintf("Position %d", p+1))
	}
	headers = append(headers, "Total")

	table.SetHeader(headers)

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

	rackLeaders := map[string]int{}
	rackPositions := map[string][]int{}

	racks := DistinctRacks(brokers)
	brokerRacks := BrokerRacks(brokers)

	for _, rack := range racks {
		rackPositions[rack] = []int{}
		for p := 0; p < maxReplicas; p++ {
			rackPositions[rack] = append(rackPositions[rack], 0)
		}
	}
	for _, topic := range topics {
		for _, partition := range topic.Partitions {
			leaderRack := brokerRacks[partition.Leader]
			rackLeaders[leaderRack]++

			for r, replica := range partition.Replicas {
				rack := brokerRacks[replica]
				rackPositions[rack][r]++
			}
		}
	}

	for _, rack := range racks {
		row := []string{
			rack,
		}

		if hasLeaders {
			row = append(
				row,
				fmt.Sprintf("%d", rackLeaders[rack]),
			)
		}

		total := 0

		for p := 0; p < maxReplicas; p++ {
			row = append(
				row,
				fmt.Sprintf("%d", rackPositions[rack][p]),
			)
			total += rackPositions[rack][p]
		}
		row = append(row, fmt.Sprintf("%d", total))

		table.Append(row)
	}

	table.Render()
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}

// FormatBrokersPerRack creates a pretty table that shows the number of
// brokers per rack.
func FormatBrokersPerRack(brokers []BrokerInfo) string {
	buf := &bytes.Buffer{}

	table := tablewriter.NewWriter(buf)
	table.SetHeader(
		[]string{
			"Rack",
			"Num Brokers",
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

	brokerCountsPerRack := BrokerCountsPerRack(brokers)
	racks := DistinctRacks(brokers)

	for _, rack := range racks {
		table.Append(
			[]string{
				rack,
				fmt.Sprintf("%d", brokerCountsPerRack[rack]),
			},
		)
	}

	table.Render()
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}

// FormatTopics creates a pretty table that lists the details of the
// argument topics.
func FormatTopics(topics []TopicInfo, brokers []BrokerInfo, full bool) string {
	buf := &bytes.Buffer{}

	headers := []string{
		"Name",
		"Partitions",
		"Replication",
		"Retention\nMins",
		"Racks\n(min,max)",
	}

	if full {
		headers = append(headers, "Config")
	}

	table := tablewriter.NewWriter(buf)
	table.SetHeader(headers)
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

	brokerRacks := BrokerRacks(brokers)

	for _, topic := range topics {
		var retentionStr string

		retention := topic.Retention()
		if retention > 0 {
			retentionStr = fmt.Sprintf("%d", int(retention.Minutes()))
		}

		minRacks, maxRacks, _ := topic.RackCounts(brokerRacks)

		row := []string{
			topic.Name,
			fmt.Sprintf("%d", len(topic.Partitions)),
			fmt.Sprintf("%d", topic.MaxReplication()),
			retentionStr,
			fmt.Sprintf("(%d,%d)", minRacks, maxRacks),
		}

		if full {
			row = append(row, prettyConfig(topic.Config))
		}

		table.Append(row)
	}

	table.Render()
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}

// FormatTopicPartitions creates a pretty table with information on all of the
// partitions for a topic.
func FormatTopicPartitions(partitions []PartitionInfo, brokers []BrokerInfo) string {
	buf := &bytes.Buffer{}

	table := tablewriter.NewWriter(buf)
	table.SetHeader(
		[]string{
			"ID", "Leader", "Replicas", "ISR", "Distinct\nRacks", "Racks", "Status",
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

	brokerRacks := BrokerRacks(brokers)
	maxBrokerWidth := maxValueToMaxWidth(len(brokers))

	for _, partition := range partitions {
		racks, _ := partition.Racks(brokerRacks)
		rackCount, _ := partition.NumRacks(brokerRacks)

		inSync := util.SameElements(partition.Replicas, partition.ISR)

		var correctLeader bool
		if len(partition.Replicas) > 0 {
			correctLeader = partition.Leader == partition.Replicas[0]
		} else {
			// No replica information yet
			correctLeader = false
		}

		var statusPrinter func(f string, a ...interface{}) string
		if !util.InTerminal() || (inSync && correctLeader) {
			statusPrinter = fmt.Sprintf
		} else if !inSync {
			statusPrinter = color.New(color.FgRed).SprintfFunc()
		} else if !correctLeader {
			statusPrinter = color.New(color.FgCyan).SprintfFunc()
		}

		var statusStr string
		if !inSync {
			statusStr = "Out-of-sync"
		} else if !correctLeader {
			statusStr = "Wrong leader"
		} else {
			statusStr = "OK"
		}

		table.Append(
			[]string{
				fmt.Sprintf("%d", partition.ID),
				fmt.Sprintf("%d", partition.Leader),
				intSliceString(partition.Replicas, maxBrokerWidth),
				intSliceString(partition.ISR, maxBrokerWidth),
				fmt.Sprintf("%d", rackCount),
				fmt.Sprintf("%+v", racks),
				statusPrinter("%s", statusStr),
			},
		)
	}

	table.Render()
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}

// FormatTopicsPartitionsSummary creates a pretty table with summary of the
// partitions for topics.
func FormatTopicsPartitionsSummary(
	topicsPartitionsStatusSummary map[string]map[PartitionStatus][]int,
) string {
	buf := &bytes.Buffer{}

	headers := []string{
		"Topic",
		"Status",
		"Count",
		"IDs",
	}
	columnAligment := []int{
		tablewriter.ALIGN_LEFT,
		tablewriter.ALIGN_LEFT,
		tablewriter.ALIGN_LEFT,
		tablewriter.ALIGN_LEFT,
	}

	table := tablewriter.NewWriter(buf)
	table.SetHeader(headers)
	table.SetAutoWrapText(true)
	table.SetColumnAlignment(columnAligment)
	table.SetBorders(
		tablewriter.Border{
			Left:   false,
			Top:    true,
			Right:  false,
			Bottom: true,
		},
	)

	topicNames := make([]string, 0, len(topicsPartitionsStatusSummary))
	tableData := make(map[string][][]string, len(topicsPartitionsStatusSummary))

	for topicName, partitionsStatusSummary := range topicsPartitionsStatusSummary {
		topicTableRows := make([][]string, 0, len(partitionsStatusSummary))

		for partitionStatus, partitionStatusIDs := range partitionsStatusSummary {
			topicTableRows = append(topicTableRows, []string{
				topicName,
				string(partitionStatus),
				fmt.Sprint(len(partitionStatusIDs)),
				fmt.Sprintf("%+v", partitionStatusIDs),
			})
		}

		// sort the topicTableRows by partitionStatus:
		// second element in the row is PartitionStatus
		statusSort := func(x, y []string) int { return cmp.Compare(x[1], y[1]) }
		slices.SortFunc(topicTableRows, statusSort)

		tableData[topicName] = topicTableRows
		topicNames = append(topicNames, topicName)
	}

	sort.Strings(topicNames)
	for _, topicName := range topicNames {
		_, exists := tableData[topicName]
		if exists {
			for _, topicTableRow := range tableData[topicName] {
				table.Append(topicTableRow)
			}
		}
	}

	table.Render()
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}

// FormatTopicsPartitions creates a pretty table with information on all of the
// partitions for topics.
func FormatTopicsPartitions(
	topicsPartitionsStatusInfo map[string][]PartitionStatusInfo,
	brokers []BrokerInfo,
) string {
	buf := &bytes.Buffer{}

	headers := []string{
		"Topic",
		"ID",
		"Leader",
		"ISR",
		"Replicas",
		"Distinct\nRacks",
		"Racks",
		"Status",
	}
	columnAligment := []int{
		tablewriter.ALIGN_LEFT,
		tablewriter.ALIGN_LEFT,
		tablewriter.ALIGN_LEFT,
		tablewriter.ALIGN_LEFT,
		tablewriter.ALIGN_LEFT,
		tablewriter.ALIGN_LEFT,
		tablewriter.ALIGN_LEFT,
		tablewriter.ALIGN_LEFT,
	}

	table := tablewriter.NewWriter(buf)
	table.SetHeader(headers)
	table.SetAutoWrapText(false)
	table.SetColumnAlignment(columnAligment)
	table.SetBorders(
		tablewriter.Border{
			Left:   false,
			Top:    true,
			Right:  false,
			Bottom: true,
		},
	)

	inTerminal := util.InTerminal()
	topicNames := make([]string, 0, len(topicsPartitionsStatusInfo))
	brokerRacks := BrokerRacks(brokers)
	tableData := make(map[string][][]string)

	for topicName, partitionsStatusInfo := range topicsPartitionsStatusInfo {
		topicTableRows := [][]string{}
		for _, partitionStatusInfo := range partitionsStatusInfo {
			racks := partitionStatusInfo.Racks(brokerRacks)

			distinctRacks := make(map[string]int, len(racks))
			for _, rack := range racks {
				distinctRacks[rack] += 1
			}

			partitionISRs := make([]int, len(partitionStatusInfo.Partition.Isr))
			for i, isr := range partitionStatusInfo.Partition.Isr {
				partitionISRs[i] = isr.ID
			}

			partitionReplicas := make([]int, len(partitionStatusInfo.Partition.Replicas))
			for i, replica := range partitionStatusInfo.Partition.Replicas {
				partitionReplicas[i] = replica.ID
			}

			inSync := true
			if partitionStatusInfo.Status != Ok {
				inSync = false
			}

			correctLeader := true
			if partitionStatusInfo.LeaderState != CorrectLeader {
				correctLeader = false
			}

			statusPrinter := fmt.Sprint
			if inTerminal && !inSync {
				statusPrinter = color.New(color.FgRed).Sprint
			}

			statePrinter := fmt.Sprintf
			if inTerminal && !correctLeader {
				statePrinter = color.New(color.FgCyan).Sprintf
			}

			leaderStateString := fmt.Sprint(partitionStatusInfo.Partition.Leader.ID)
			if !correctLeader {
				leaderStateString += " " + statePrinter("(%s)", partitionStatusInfo.LeaderState)
			}

			topicTableRows = append(topicTableRows, []string{
				topicName,
				fmt.Sprint(partitionStatusInfo.Partition.ID),
				leaderStateString,
				fmt.Sprintf("%+v", partitionISRs),
				fmt.Sprintf("%+v", partitionReplicas),
				fmt.Sprint(len(distinctRacks)),
				fmt.Sprintf("%+v", racks),
				statusPrinter(partitionStatusInfo.Status),
			})
		}

		tableData[topicName] = topicTableRows
		topicNames = append(topicNames, topicName)
	}

	sort.Strings(topicNames)
	for _, topicName := range topicNames {
		_, exists := tableData[topicName]
		if exists {
			for _, topicTableRow := range tableData[topicName] {
				table.Append(topicTableRow)
			}
		}
	}

	table.Render()
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}

// FormatConfig creates a pretty table with all of the keys and values in a topic or
// broker config.
func FormatConfig(configMap map[string]string) string {
	buf := &bytes.Buffer{}

	table := tablewriter.NewWriter(buf)
	table.SetHeader(
		[]string{
			"Key",
			"Value",
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

	keys := []string{}
	for key := range configMap {
		keys = append(keys, key)
	}

	sort.Slice(keys, func(a, b int) bool {
		return keys[a] < keys[b]
	})

	for _, key := range keys {
		table.Append(
			[]string{
				key,
				configMap[key],
			},
		)
	}

	table.Render()
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}

// FormatTopicLeadersPerRack creates a pretty table that shows the number
// of partitions with a leader in each rack.
func FormatTopicLeadersPerRack(topic TopicInfo, brokers []BrokerInfo) string {
	buf := &bytes.Buffer{}

	table := tablewriter.NewWriter(buf)
	table.SetHeader(
		[]string{
			"Rack", "Num Leaders",
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

	leadersPerRack := LeadersPerRack(brokers, topic)
	racks := DistinctRacks(brokers)

	for _, rack := range racks {
		table.Append(
			[]string{
				rack,
				fmt.Sprintf("%d", leadersPerRack[rack]),
			},
		)
	}

	table.Render()
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}

// FormatAssignentDiffs generates a pretty table that shows the before
// and after states of a partition replica and/or leader update.
func FormatAssignentDiffs(
	curr []PartitionAssignment,
	desired []PartitionAssignment,
	brokers []BrokerInfo,
) string {
	buf := &bytes.Buffer{}

	table := tablewriter.NewWriter(buf)
	table.SetHeader(
		[]string{
			"Partition",
			"Curr\nReplicas",
			"Proposed\nReplicas",
			"Diff?",
			"New\nLeader?",
		},
	)
	table.SetAutoWrapText(false)
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

	diffs := AssignmentDiffs(curr, desired)

	brokerRacks := BrokerRacks(brokers)
	maxWidth := maxValueToMaxWidth(len(brokers))

	for _, diff := range diffs {
		var diffStr string
		var newLeaderStr string

		if !reflect.DeepEqual(diff.Old.Replicas, diff.New.Replicas) {
			diffStr = "Y"
		}
		if len(diff.Old.Replicas) > 0 &&
			len(diff.New.Replicas) > 0 &&
			diff.Old.Replicas[0] != diff.New.Replicas[0] {
			newLeaderStr = "Y"
		}

		table.Append(
			[]string{
				fmt.Sprintf("%d", diff.PartitionID),
				assignmentRacksStr(diff.Old, brokerRacks, maxWidth),
				assignmentRacksDiffStr(diff.Old, diff.New, brokerRacks, maxWidth),
				diffStr,
				newLeaderStr,
			},
		)
	}

	table.Render()
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}

// FormatBrokerMaxPartitions generates a pretty table that shows the total number of
// partitions that each broker is involved in for a diff. It's used to evaluate
// the potential extra load that could occur on brokers during a migration.
func FormatBrokerMaxPartitions(
	curr []PartitionAssignment,
	desired []PartitionAssignment,
	brokers []BrokerInfo,
) string {
	buf := &bytes.Buffer{}

	table := tablewriter.NewWriter(buf)
	table.SetHeader(
		[]string{
			"Broker",
			"Curr\nPartitions",
			"Max\nPartitions",
			"Proposed\nPartitions",
		},
	)
	table.SetAutoWrapText(false)
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

	startPartitionsPerBroker := MaxPartitionsPerBroker(curr)
	maxPartitionsPerBroker := MaxPartitionsPerBroker(curr, desired)
	finalPartitionsPerBroker := MaxPartitionsPerBroker(desired)

	maxCount := max(
		maxMapValues(startPartitionsPerBroker),
		maxMapValues(maxPartitionsPerBroker),
		maxMapValues(finalPartitionsPerBroker),
	)

	maxCountWidth := maxValueToMaxWidth(maxCount)

	for _, broker := range brokers {
		migrationExtraPartitions := maxPartitionsPerBroker[broker.ID] -
			startPartitionsPerBroker[broker.ID]
		finalExtraPartitions := finalPartitionsPerBroker[broker.ID] -
			startPartitionsPerBroker[broker.ID]

		table.Append(
			[]string{
				fmt.Sprintf("%d", broker.ID),
				fmt.Sprintf("%d", startPartitionsPerBroker[broker.ID]),
				fmt.Sprintf(
					"%*d%s",
					maxCountWidth,
					maxPartitionsPerBroker[broker.ID],
					partitionCountDiffStr(migrationExtraPartitions),
				),
				fmt.Sprintf(
					"%*d%s",
					maxCountWidth,
					finalPartitionsPerBroker[broker.ID],
					partitionCountDiffStr(finalExtraPartitions),
				),
			},
		)
	}

	table.Render()
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}

// FormatACLs creates a pretty table that lists the details of the
// argument acls.
func FormatACLs(acls []ACLInfo) string {
	buf := &bytes.Buffer{}

	headers := []string{
		"Resource Type",
		"Pattern Type",
		"Resource Name",
		"Principal",
		"Host",
		"Operation",
		"Permission Type",
	}

	table := tablewriter.NewWriter(buf)
	table.SetHeader(headers)
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

	for _, acl := range acls {
		row := []string{
			acl.ResourceType.String(),
			acl.PatternType.String(),
			acl.ResourceName,
			acl.Principal,
			acl.Host,
			acl.Operation.String(),
			acl.PermissionType.String(),
		}

		table.Append(row)
	}

	table.Render()
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}

// FormatUsers creates a pretty table that lists the details of the
// argument users.
func FormatUsers(users []UserInfo) string {
	buf := &bytes.Buffer{}

	headers := []string{
		"Name",
		"Mechanism",
		"Iterations",
	}

	table := tablewriter.NewWriter(buf)
	table.SetHeader(headers)
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

	for _, user := range users {
		for _, credential := range user.CredentialInfos {
			row := []string{
				user.Name,
				credential.ScramMechanism.String(),
				fmt.Sprintf("%d", credential.Iterations),
			}

			table.Append(row)
		}
	}

	table.Render()
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}

func prettyConfig(config map[string]string) string {
	rows := []string{}

	for key, value := range config {
		rows = append(rows, fmt.Sprintf("%s=%s", key, value))
	}

	sort.Slice(rows, func(a, b int) bool {
		return rows[a] < rows[b]
	})

	return strings.Join(rows, "\n")
}

func assignmentRacksStr(
	assignment PartitionAssignment,
	brokerRacks map[int]string,
	maxWidth int,
) string {
	if len(assignment.Replicas) == 0 {
		return ""
	}

	elements := []string{}

	for _, replica := range assignment.Replicas {
		elements = append(
			elements,
			fmt.Sprintf("%*d (%s)", maxWidth, replica, brokerRacks[replica]),
		)
	}

	return strings.Join(elements, ", ")
}

func assignmentRacksDiffStr(
	old PartitionAssignment,
	new PartitionAssignment,
	brokerRacks map[int]string,
	maxWidth int,
) string {
	if len(new.Replicas) == 0 {
		return ""
	}

	if !util.InTerminal() {
		return assignmentRacksStr(new, brokerRacks, maxWidth)
	}

	elements := []string{}

	added := color.New(color.FgRed).SprintfFunc()
	moved := color.New(color.FgCyan).SprintfFunc()

	for r, replica := range new.Replicas {
		var element string

		if r < len(old.Replicas) && replica == old.Replicas[r] {
			element = fmt.Sprintf("%*d (%s)", maxWidth, replica, brokerRacks[replica])
		} else if old.Index(replica) != -1 {
			element = moved("%*d (%s)", maxWidth, replica, brokerRacks[replica])
		} else {
			element = added("%*d (%s)", maxWidth, replica, brokerRacks[replica])
		}

		elements = append(elements, element)
	}

	return strings.Join(elements, ", ")
}

func partitionCountDiffStr(diffValue int) string {
	if diffValue == 0 {
		return ""
	}

	var increasedSprintf func(format string, a ...interface{}) string
	var decreasedSprintf func(format string, a ...interface{}) string

	if !util.InTerminal() {
		increasedSprintf = fmt.Sprintf
		decreasedSprintf = fmt.Sprintf
	} else {
		increasedSprintf = color.New(color.FgRed).SprintfFunc()
		decreasedSprintf = color.New(color.FgCyan).SprintfFunc()
	}

	if diffValue > 0 {
		return fmt.Sprintf(" (%s)", increasedSprintf("%+d", diffValue))
	}
	return fmt.Sprintf(" (%s)", decreasedSprintf("%-d", diffValue))
}

func intSliceString(values []int, maxWidth int) string {
	strValues := []string{}

	for _, value := range values {
		strValues = append(strValues, fmt.Sprintf("%*d", maxWidth, value))
	}

	return fmt.Sprintf("%+v", strValues)
}

func maxValueToMaxWidth(maxValue int) int {
	return int(math.Log10(float64(maxValue))) + 1
}

func maxMapValues(inputMap map[int]int) int {
	var maxValue int
	currIndex := 0

	for _, value := range inputMap {
		if currIndex == 0 || value > maxValue {
			maxValue = value
		}
		currIndex++
	}

	return maxValue
}
