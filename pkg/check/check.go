package check

import (
	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/config"
)

type TopicChecker struct {
	adminClient *admin.Client
	topicConfig config.TopicConfig
}

func NewTopicChecker(
	adminClient *admin.Client,
	topicConfig config.TopicConfig,
) *TopicChecker {
	return &TopicChecker{
		adminClient: adminClient,
		topicConfig: topicConfig,
	}
}

func (t *TopicChecker) Check() error {
	return nil
}
