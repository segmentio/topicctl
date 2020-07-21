package apply

import (
	"encoding/json"

	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

func FormatNewTopicConfig(config kafka.TopicConfig) string {
	content, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		log.Warnf("Error marshalling topic config: %+v", err)
		return "Error"
	}

	return string(content)
}
