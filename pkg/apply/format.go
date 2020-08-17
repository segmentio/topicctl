package apply

import (
	"encoding/json"

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
	clientSettings map[string]string,
) string {
	return ""
}
