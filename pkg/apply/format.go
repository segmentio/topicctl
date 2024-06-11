package apply

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/config"
	log "github.com/sirupsen/logrus"
)

// FormatNewTopicConfig generates a pretty string representation of a kafka-go
// topic config.
func FormatNewTopicConfig(config kafka.TopicConfig) string {
	content, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		log.Warnf("Error marshalling topic config: %+v", err)
		return "Error"
	}

	return string(content)
}

// FormatSettingsDiff generates a table that summarizes the differences between
// the topic settings from a topic config and the settings from ZK.
func FormatSettingsDiff(
	topicSettings config.TopicSettings,
	configMap map[string]string,
	diffKeys []string,
) (string, error) {
	buf := &bytes.Buffer{}

	table := tablewriter.NewWriter(buf)

	headers := []string{
		"Key",
		"Cluster Value (Curr)",
		"Config Value (New)",
	}

	table.SetHeader(headers)

	table.SetAutoWrapText(false)
	table.SetColumnAlignment(
		[]int{
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

	for _, diffKey := range diffKeys {
		configValueStr := configMap[diffKey]

		var valueStr string
		var err error

		if topicSettings.HasKey(diffKey) {
			valueStr, err = topicSettings.GetValueStr(diffKey)
			if err != nil {
				return "", err
			}
		}

		// Add a human-formatted minutes suffix to time-related fields
		if strings.HasSuffix(diffKey, ".ms") {
			configValueStr = fmt.Sprintf("%s%s", configValueStr, timeSuffix(configValueStr))
			valueStr = fmt.Sprintf("%s%s", valueStr, timeSuffix(valueStr))
		}

		row := []string{
			diffKey,
			configValueStr,
			valueStr,
		}

		table.Append(row)
	}

	table.Render()
	return string(bytes.TrimRight(buf.Bytes(), "\n")), nil
}

// FormatSettingsDiffJson formats the settings diffs as a JSON object instead of a table
func FormatSettingsDiffJson(
	topicSettings config.TopicSettings,
	configMap map[string]string,
	diffKeys []string,
) ([]byte, error) {
	diffsMap := make(map[string]map[string]interface{})

	for _, diffKey := range diffKeys {
		configValueStr := configMap[diffKey]

		var valueStr string
		var err error

		if topicSettings.HasKey(diffKey) {
			valueStr, err = topicSettings.GetValueStr(diffKey)
			if err != nil {
				return []byte{}, err
			}
		}

		diffsMap[diffKey] = make(map[string]interface{})
		diffsMap[diffKey]["current"] = configValueStr
		diffsMap[diffKey]["updated"] = valueStr
	}
	return json.Marshal(diffsMap)
}

// FormatMissingKeys generates a table that summarizes the key/value pairs
// that are set in the config in ZK but missing from the topic config.
func FormatMissingKeys(
	configMap map[string]string,
	missingKeys []string,
) string {
	buf := &bytes.Buffer{}

	table := tablewriter.NewWriter(buf)

	headers := []string{
		"Key",
		"Cluster Value",
	}

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

	for _, missingKey := range missingKeys {
		configValueStr := configMap[missingKey]

		// Add a human-formatted minutes suffix to time-related fields
		if strings.HasSuffix(missingKey, ".ms") {
			configValueStr = fmt.Sprintf("%s%s", configValueStr, timeSuffix(configValueStr))
		}

		row := []string{
			missingKey,
			configValueStr,
		}

		table.Append(row)
	}

	table.Render()
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}

func timeSuffix(msStr string) string {
	msInt, err := strconv.ParseInt(msStr, 10, 64)
	if err != nil {
		return ""
	}

	if msInt < 60000 {
		return ""
	}

	if msInt%60000 != 0 {
		return ""
	}

	return fmt.Sprintf(" (%d min)", msInt/60000)
}
