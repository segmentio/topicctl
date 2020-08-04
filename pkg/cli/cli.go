package cli

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/briandowns/spinner"
	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/apply"
	"github.com/segmentio/topicctl/pkg/config"
	"github.com/segmentio/topicctl/pkg/groups"
	"github.com/segmentio/topicctl/pkg/messages"
	log "github.com/sirupsen/logrus"
)

const (
	spinnerCharSet  = 36
	spinnerDuration = 200 * time.Millisecond
)

type CLIRunner struct {
	adminClient  *admin.Client
	groupsClient *groups.Client
	printer      func(f string, a ...interface{})
	spinnerObj   *spinner.Spinner
}

func NewCLIRunner(
	adminClient *admin.Client,
	printer func(f string, a ...interface{}),
	showSpinner bool,
) *CLIRunner {
	var spinnerObj *spinner.Spinner

	if showSpinner {
		spinnerObj = spinner.New(
			spinner.CharSets[spinnerCharSet],
			spinnerDuration,
			spinner.WithWriter(os.Stderr),
			spinner.WithHiddenCursor(true),
		)
		spinnerObj.Prefix = "Loading: "
	}

	return &CLIRunner{
		adminClient:  adminClient,
		groupsClient: groups.NewClient(adminClient.GetBootstrapAddrs()[0]),
		printer:      printer,
		spinnerObj:   spinnerObj,
	}
}

func (c *CLIRunner) GetBrokers(ctx context.Context, full bool) error {
	c.startSpinner()

	brokers, err := c.adminClient.GetBrokers(ctx, nil)
	c.stopSpinner()
	if err != nil {
		return err
	}

	c.printer("Brokers:\n%s", admin.FormatBrokers(brokers, full))
	c.printer("Brokers per rack:\n%s", admin.FormatBrokersPerRack(brokers))

	return nil
}

func (c *CLIRunner) ApplyTopic(
	ctx context.Context,
	applierConfig apply.TopicApplierConfig,
) error {
	applier, err := apply.NewTopicApplier(
		ctx,
		c.adminClient,
		applierConfig,
	)
	if err != nil {
		return err
	}

	c.printer(
		"Starting apply for topic %s in environment %s, cluster %s",
		applierConfig.TopicConfig.Meta.Name,
		applierConfig.TopicConfig.Meta.Environment,
		applierConfig.TopicConfig.Meta.Cluster,
	)

	err = applier.Apply(ctx)
	if err != nil {
		return err
	}

	c.printer("Apply completed successfully!")
	return nil
}

func (c *CLIRunner) BootstrapTopics(
	ctx context.Context,
	topics []string,
	clusterConfig config.ClusterConfig,
	matchRegexpStr string,
	excludeRegexpStr string,
	outputDir string,
	overwrite bool,
) error {
	topicInfoObjs, err := c.adminClient.GetTopics(ctx, topics, false)
	if err != nil {
		return err
	}

	matchRegexp, err := regexp.Compile(matchRegexpStr)
	if err != nil {
		return err
	}
	excludeRegexp, err := regexp.Compile(excludeRegexpStr)
	if err != nil {
		return err
	}

	topicConfigs := []config.TopicConfig{}

	for _, topicInfo := range topicInfoObjs {
		if strings.HasPrefix(topicInfo.Name, "__") {
			// Never include underscore topics
			continue
		} else if !matchRegexp.MatchString(topicInfo.Name) {
			continue
		} else if excludeRegexp.MatchString(topicInfo.Name) {
			continue
		}

		topicConfig := config.TopicConfig{
			Meta: config.TopicMeta{
				Name:        topicInfo.Name,
				Cluster:     clusterConfig.Meta.Name,
				Region:      clusterConfig.Meta.Region,
				Environment: clusterConfig.Meta.Environment,
				Description: "Bootstrapped via topicctl bootstrap",
			},
			Spec: config.TopicSpec{
				Partitions:        len(topicInfo.Partitions),
				ReplicationFactor: len(topicInfo.Partitions[0].Replicas),
				RetentionMinutes:  int(topicInfo.Retention().Minutes()),
				PlacementConfig: config.TopicPlacementConfig{
					Strategy: config.PlacementStrategyAny,
				},
			},
		}

		topicConfigs = append(topicConfigs, topicConfig)
	}

	for _, topicConfig := range topicConfigs {
		yamlStr, err := topicConfig.ToYAML()
		if err != nil {
			return err
		}

		if outputDir != "" {
			outputPath := filepath.Join(
				outputDir,
				fmt.Sprintf("%s.yaml", topicConfig.Meta.Name),
			)

			var isNew bool

			_, err := os.Stat(outputPath)
			if err != nil && !os.IsNotExist(err) {
				return err
			}
			isNew = os.IsNotExist(err)

			if isNew || overwrite {
				log.Infof("Writing config to %s", outputPath)
				err = ioutil.WriteFile(outputPath, []byte(yamlStr), 0644)
				if err != nil {
					return err
				}
			} else {
				log.Infof("Skipping over existing config %s", outputPath)
			}
		} else {
			log.Infof("Config for topic %s:\n%s", topicConfig.Meta.Name, yamlStr)
		}
	}

	return nil
}

func (c *CLIRunner) GetBrokerBalance(ctx context.Context, topicName string) error {
	c.startSpinner()

	brokers, err := c.adminClient.GetBrokers(ctx, nil)
	if err != nil {
		c.stopSpinner()
		return err
	}

	var topicNames []string
	if topicName != "" {
		topicNames = []string{topicName}
	}

	topics, err := c.adminClient.GetTopics(ctx, topicNames, false)
	if err != nil {
		c.stopSpinner()
		return err
	}

	c.stopSpinner()

	c.printer("Broker replicas:\n%s", admin.FormatBrokerReplicas(brokers, topics))
	c.printer("Broker rack replicas:\n%s", admin.FormatBrokerRackReplicas(brokers, topics))

	return nil
}

func (c *CLIRunner) GetConfig(ctx context.Context, brokerOrTopic string) error {
	c.startSpinner()

	brokerIDs, err := c.adminClient.GetBrokerIDs(ctx)
	if err != nil {
		c.stopSpinner()
		return err
	}

	topicNames, err := c.adminClient.GetTopicNames(ctx)
	if err != nil {
		c.stopSpinner()
		return err
	}

	for _, brokerID := range brokerIDs {
		if fmt.Sprintf("%d", brokerID) == brokerOrTopic {
			brokers, err := c.adminClient.GetBrokers(ctx, []int{brokerID})
			if err != nil {
				c.stopSpinner()
				return err
			}
			if len(brokers) != 1 {
				c.stopSpinner()
				return fmt.Errorf("Could not find broker %d", brokerID)
			}
			c.stopSpinner()

			c.printer(
				"Config for broker %d:\n%s",
				brokerID,
				admin.FormatConfig(brokers[0].Config),
			)
			return nil
		}
	}

	for _, topicName := range topicNames {
		if topicName == brokerOrTopic {
			topics, err := c.adminClient.GetTopics(ctx, []string{topicName}, false)
			if err != nil {
				c.stopSpinner()
				return err
			}
			if len(topics) != 1 {
				c.stopSpinner()
				return fmt.Errorf("Could not find topic %s", topicName)
			}
			c.stopSpinner()

			c.printer(
				"Config for topic %s:\n%s",
				topicName,
				admin.FormatConfig(topics[0].Config),
			)
			return nil
		}
	}

	c.stopSpinner()
	return fmt.Errorf("Could not find broker or topic named %s", brokerOrTopic)
}

func (c *CLIRunner) GetGroups(ctx context.Context) error {
	c.startSpinner()

	groupCoordinators, err := c.groupsClient.GetGroups(ctx)
	c.stopSpinner()
	if err != nil {
		return err
	}

	c.printer("Groups:\n%s", groups.FormatGroupCoordinators(groupCoordinators))
	return nil
}

func (c *CLIRunner) GetGroupMembers(ctx context.Context, groupID string, full bool) error {
	c.startSpinner()

	groupDetails, err := c.groupsClient.GetGroupDetails(ctx, groupID)
	c.stopSpinner()
	if err != nil {
		return err
	}

	c.printer("Group state: %s", groupDetails.State)
	c.printer(
		"Group members (%d):\n%s",
		len(groupDetails.Members),
		groups.FormatGroupMembers(groupDetails.Members, full),
	)
	c.printer(
		"Member frequency by partition count:\n%s",
		groups.FormatMemberPartitionCounts(groupDetails.Members),
	)

	return nil
}

func (c *CLIRunner) GetMemberLags(ctx context.Context, topic string, groupID string) error {
	c.startSpinner()

	// Check that topic exists before getting offsets; otherwise, the topic get
	// created as part of the lag check.
	_, err := c.adminClient.GetTopic(ctx, topic, false)
	if err != nil {
		c.stopSpinner()
		return fmt.Errorf("Error fetching topic info: %+v", err)
	}

	memberLags, err := c.groupsClient.GetMemberLags(ctx, topic, groupID)
	c.stopSpinner()

	if err != nil {
		return err
	}

	c.printer("Group member lags:\n%s", groups.FormatMemberLags(memberLags))
	return nil
}

func (c *CLIRunner) GetPartitions(ctx context.Context, topic string) error {
	c.startSpinner()

	topicInfo, err := c.adminClient.GetTopic(ctx, topic, true)
	if err != nil {
		c.stopSpinner()
		return err
	}

	brokers, err := c.adminClient.GetBrokers(ctx, nil)
	c.stopSpinner()
	if err != nil {
		return err
	}

	c.printer(
		"Partitions for topic %s:\n%s",
		topic,
		admin.FormatTopicPartitions(topicInfo.Partitions, brokers),
	)

	return nil
}

func (c *CLIRunner) GetOffsets(ctx context.Context, topic string) error {
	c.startSpinner()

	// Check that topic exists before getting offsets; otherwise, the topic might
	// be created as part of the bounds check.
	_, err := c.adminClient.GetTopic(ctx, topic, false)
	if err != nil {
		c.stopSpinner()
		return fmt.Errorf("Error fetching topic info: %+v", err)
	}

	bounds, err := messages.GetAllPartitionBounds(
		ctx,
		c.adminClient.GetBootstrapAddrs()[0],
		topic,
		nil,
	)
	c.stopSpinner()
	if err != nil {
		return err
	}

	c.printer(
		"Partition bounds for topic %s:\n%s",
		topic,
		messages.FormatBounds(bounds),
	)

	formattedTotals := messages.FormatBoundTotals(bounds)

	if formattedTotals != "" {
		c.printer(
			"Total bounds for topic %s across all partitions:\n%s",
			topic,
			formattedTotals,
		)
	}

	return nil
}

func (c *CLIRunner) GetTopics(ctx context.Context, full bool) error {
	c.startSpinner()

	topics, err := c.adminClient.GetTopics(ctx, nil, false)
	if err != nil {
		c.stopSpinner()
		return err
	}
	brokers, err := c.adminClient.GetBrokers(ctx, nil)
	c.stopSpinner()
	if err != nil {
		return err
	}

	c.printer("Topics:\n%s", admin.FormatTopics(topics, brokers, full))

	return nil
}

func (c *CLIRunner) ResetOffsets(
	ctx context.Context,
	topic string,
	groupID string,
	partitionOffsets map[int]int64,
) error {
	c.startSpinner()
	err := c.groupsClient.ResetOffsets(ctx, topic, groupID, partitionOffsets)
	c.stopSpinner()
	if err != nil {
		return err
	}

	c.printer("Success")

	return nil
}

func (c *CLIRunner) Tail(
	ctx context.Context,
	topic string,
	offset int64,
	partitions []int,
	maxMessages int,
	filterRegexp string,
	raw bool,
) error {
	var err error
	if len(partitions) == 0 {
		topicInfo, err := c.adminClient.GetTopic(ctx, topic, false)
		if err != nil {
			return err
		}
		partitions = topicInfo.PartitionIDs()
	}

	log.Debugf("Tailing partitions %+v", partitions)

	tailer := messages.NewTopicTailer(
		c.adminClient.GetBootstrapAddrs()[0],
		topic,
		partitions,
		offset,
		10e3,
		10e6,
	)
	stats, err := tailer.LogMessages(ctx, maxMessages, filterRegexp, raw)
	filtered := filterRegexp != ""

	if !raw {
		c.printer("Tail stats:\n%s", messages.FormatTailStats(stats, filtered))
	}

	return err
}

func (c *CLIRunner) startSpinner() {
	if c.spinnerObj != nil {
		c.spinnerObj.Start()
	}
}

func (c *CLIRunner) stopSpinner() {
	if c.spinnerObj != nil && c.spinnerObj.Active() {
		c.spinnerObj.Stop()
	}
}

func stringsToInts(strs []string) ([]int, error) {
	ints := []int{}

	for _, str := range strs {
		nextInt, err := strconv.ParseInt(str, 10, 32)
		if err != nil {
			return nil, err
		}
		ints = append(ints, int(nextInt))
	}

	return ints, nil
}
