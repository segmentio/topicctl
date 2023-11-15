package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/briandowns/spinner"
	"github.com/fatih/color"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/apply"
	"github.com/segmentio/topicctl/pkg/check"
	"github.com/segmentio/topicctl/pkg/config"
	"github.com/segmentio/topicctl/pkg/groups"
	"github.com/segmentio/topicctl/pkg/messages"
	log "github.com/sirupsen/logrus"
)

const (
	spinnerCharSet  = 36
	spinnerDuration = 200 * time.Millisecond
)

// CLIRunner is a utility that runs commands from either the command-line or the repl.
type CLIRunner struct {
	adminClient admin.Client
	printer     func(f string, a ...interface{})
	spinnerObj  *spinner.Spinner
}

// NewCLIRunner creates and returns a new CLIRunner instance.
func NewCLIRunner(
	adminClient admin.Client,
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

	cliRunner := &CLIRunner{
		adminClient: adminClient,
		printer:     printer,
		spinnerObj:  spinnerObj,
	}

	return cliRunner
}

// GetBrokers gets all brokers and prints out a summary for the user.
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

// ApplyTopic does an apply run according to the spec in the argument config.
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

	highlighter := color.New(color.FgYellow, color.Bold).SprintfFunc()

	c.printer(
		"Starting apply for topic %s in environment %s, cluster %s",
		highlighter(applierConfig.TopicConfig.Meta.Name),
		highlighter(applierConfig.TopicConfig.Meta.Environment),
		highlighter(applierConfig.TopicConfig.Meta.Cluster),
	)

	err = applier.Apply(ctx)
	if err != nil {
		return err
	}

	c.printer("Apply completed successfully!")
	return nil
}

// BootstrapTopics creates configs for one or more topics based on their current state in the
// cluster.
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

		topicConfig := config.TopicConfigFromTopicInfo(
			clusterConfig,
			topicInfo,
		)
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
				err = os.WriteFile(outputPath, []byte(yamlStr), 0644)
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

// CheckTopic runs a topic check against a single topic and prints a summary of the results out.
func (c *CLIRunner) CheckTopic(
	ctx context.Context,
	checkConfig check.CheckConfig,
) (bool, error) {
	results, err := check.CheckTopic(ctx, checkConfig)

	if results.AllOK() {
		c.printer(
			"Topic %s (cluster=%s, env=%s) OK",
			checkConfig.TopicConfig.Meta.Name,
			checkConfig.ClusterConfig.Meta.Name,
			checkConfig.ClusterConfig.Meta.Environment,
		)
	} else {
		c.printer(
			"Check failed for topic %s (cluster=%s, env=%s):\n%s",
			checkConfig.TopicConfig.Meta.Name,
			checkConfig.ClusterConfig.Meta.Name,
			checkConfig.ClusterConfig.Meta.Environment,
			check.FormatResults(results),
		)
	}

	return results.AllOK(), err
}

// GetBrokerBalance evaluates the balance of the brokers for a single topic and prints a summary
// out for user inspection.
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

// GetConfig fetches the config for a broker or topic and prints it out for user inspection.
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

// GetGroups fetches all consumer groups and prints them out for user inspection.
func (c *CLIRunner) GetGroups(ctx context.Context) error {
	c.startSpinner()

	groupCoordinators, err := groups.GetGroups(ctx, c.adminClient.GetConnector())
	c.stopSpinner()
	if err != nil {
		return err
	}

	c.printer("Groups:\n%s", groups.FormatGroupCoordinators(groupCoordinators))
	return nil
}

// GetGroupMembers fetches and prints out information about every member in a consumer group.
func (c *CLIRunner) GetGroupMembers(ctx context.Context, groupID string, full bool) error {
	c.startSpinner()

	groupDetails, err := groups.GetGroupDetails(
		ctx,
		c.adminClient.GetConnector(),
		groupID,
	)
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

// GetMemberLags fetches and prints a summary of the consumer group lag for each partition
// in a single topic.
func (c *CLIRunner) GetMemberLags(
	ctx context.Context,
	topic string,
	groupID string,
	full bool,
	sortByValues bool,
) error {
	c.startSpinner()

	// Check that topic exists before getting offsets; otherwise, the topic get
	// created as part of the lag check.
	_, err := c.adminClient.GetTopic(ctx, topic, false)
	if err != nil {
		c.stopSpinner()
		return fmt.Errorf("Error fetching topic info: %+v", err)
	}

	memberLags, err := groups.GetMemberLags(
		ctx,
		c.adminClient.GetConnector(),
		topic,
		groupID,
	)
	c.stopSpinner()

	if err != nil {
		return err
	}

	if sortByValues {
		sort.Slice(memberLags, func(a, b int) bool {
			return memberLags[a].TimeLag() < memberLags[b].TimeLag()
		})
	}

	c.printer(
		"Group member lags:\n%s",
		groups.FormatMemberLags(memberLags, full),
	)
	return nil
}

// GetPartitions fetches the details of each partition in a topic and prints out a summary for
// user inspection.
func (c *CLIRunner) GetPartitions(
	ctx context.Context,
	topics []string,
	status admin.PartitionStatus,
	summary bool,
) error {
	c.startSpinner()

	metadata, err := c.adminClient.GetAllTopicsMetadata(ctx)
	if err != nil {
		c.stopSpinner()
		return err
	}

	brokers, err := c.adminClient.GetBrokers(ctx, nil)
	if err != nil {
		c.stopSpinner()
		return err
	}

	if !summary {
		topicsPartitionsStatusInfo := admin.GetTopicsPartitionsStatusInfo(metadata, topics, status)
		c.stopSpinner()

		c.printer(
			"Partitions:\n%s",
			admin.FormatTopicsPartitions(topicsPartitionsStatusInfo, brokers),
		)

		return nil
	}

	statusSummary, okCount, offlineCount, underReplicatedCount := admin.GetTopicsPartitionsStatusSummary(metadata,
		topics,
		status,
	)
	c.stopSpinner()

	c.printer(
		"Partitions Summary:\n%s",
		admin.FormatTopicsPartitionsSummary(statusSummary),
	)

	c.printer(
		"%d %v partitions, %d %v partitions, %d %v partitions are found",
		okCount,
		admin.Ok,
		underReplicatedCount,
		admin.UnderReplicated,
		offlineCount,
		admin.Offline,
	)

	return nil
}

// GetOffsets fetches details about all partition offsets in a single topic and prints out
// a summary.
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
		c.adminClient.GetConnector(),
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

// GetTopics fetches the details of each topic in the cluster and prints out a summary.
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

// GerUsers fetches the details of each user in the cluster and prints out a table of them.
func (c *CLIRunner) GetUsers(ctx context.Context, names []string) error {
	c.startSpinner()

	users, err := c.adminClient.GetUsers(ctx, names)
	c.stopSpinner()
	if err != nil {
		return err
	}

	c.printer("Users:\n%s", admin.FormatUsers(users))

	return nil
}

// ResetOffsets resets the offsets for a single consumer group / topic combination.
func (c *CLIRunner) ResetOffsets(
	ctx context.Context,
	topic string,
	groupID string,
	partitionOffsets map[int]int64,
) error {
	c.startSpinner()
	err := groups.ResetOffsets(
		ctx,
		c.adminClient.GetConnector(),
		topic,
		groupID,
		partitionOffsets,
	)
	c.stopSpinner()
	if err != nil {
		return err
	}

	c.printer("Success")

	return nil
}

// Tail prints out a stream of the latest messages in a topic.
func (c *CLIRunner) Tail(
	ctx context.Context,
	topic string,
	offset int64,
	partitions []int,
	maxMessages int,
	filterRegexp string,
	raw bool,
	headers bool,
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
		c.adminClient.GetConnector(),
		topic,
		partitions,
		offset,
		10e3,
		10e6,
	)
	stats, err := tailer.LogMessages(ctx, maxMessages, filterRegexp, raw, headers)
	filtered := filterRegexp != ""

	if !raw {
		c.printer("Tail stats:\n%s", messages.FormatTailStats(stats, filtered))
	}

	return err
}

// GetACLs fetches the details of each acl in the cluster and prints out a summary.
func (c *CLIRunner) GetACLs(
	ctx context.Context,
	filter kafka.ACLFilter,
) error {
	c.startSpinner()

	acls, err := c.adminClient.GetACLs(ctx, filter)
	c.stopSpinner()
	if err != nil {
		return err
	}

	c.printer("ACLs:\n%s", admin.FormatACLs(acls))

	return nil
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
