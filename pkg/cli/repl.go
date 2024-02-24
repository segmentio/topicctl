package cli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"

	"github.com/c-bata/go-prompt"
	"github.com/olekukonko/tablewriter"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/groups"
	log "github.com/sirupsen/logrus"
)

var (
	commandSuggestions = []prompt.Suggest{
		{
			Text:        "get",
			Description: "Get information about one or more resources in the cluster",
		},
		{
			Text:        "tail",
			Description: "Tail all messages in a topic",
		},
		{
			Text:        "help",
			Description: "Show all commands",
		},
		{
			Text:        "exit",
			Description: "Quit the repl",
		},
	}

	getSuggestions = []prompt.Suggest{
		{
			Text:        "acls",
			Description: "Get all ACLs",
		},
		{
			Text:        "balance",
			Description: "Get positions of all brokers in a topic or across entire cluster",
		},
		{
			Text:        "brokers",
			Description: "Get all brokers",
		},
		{
			Text:        "config",
			Description: "Get config for broker or topic",
		},
		{
			Text:        "groups",
			Description: "Get all consumer groups",
		},
		{
			Text:        "lags",
			Description: "Get partition lags for all members of a consumer group",
		},
		{
			Text:        "members",
			Description: "Get members in a consumer group",
		},
		{
			Text:        "partitions",
			Description: "Get all partitions for a topic",
		},
		{
			Text:        "offsets",
			Description: "Get the offset ranges for all partitions in a topic",
		},
		{
			Text:        "topics",
			Description: "Get all topics",
		},
		{
			Text:        "users",
			Description: "Get all users",
		},
	}

	helpTableStr = helpTable()
)

// Repl manages the repl mode for topicctl.
type Repl struct {
	cliRunner                 *CLIRunner
	brokerAndTopicSuggestions []prompt.Suggest
	topicSuggestions          []prompt.Suggest
	groupSuggestions          []prompt.Suggest
}

// NewRepl initializes and returns a Repl instance.
func NewRepl(
	ctx context.Context,
	adminClient admin.Client,
) (*Repl, error) {
	cliRunner := NewCLIRunner(
		adminClient,
		func(f string, a ...interface{}) {
			fmt.Printf("> ")
			fmt.Printf(f, a...)
			// Add newline since printf doesn't do this automatically
			fmt.Printf("\n")
		},
		true,
	)

	log.Debug("Loading topic names for auto-complete")
	topicNames, err := adminClient.GetTopicNames(ctx)

	if err != nil {
		return nil, err
	}
	sort.Slice(topicNames, func(a, b int) bool {
		return topicNames[a] < topicNames[b]
	})

	topicSuggestions := []prompt.Suggest{}

	for _, topicName := range topicNames {
		topicSuggestions = append(
			topicSuggestions,
			prompt.Suggest{
				Text: topicName,
			},
		)
	}

	log.Debug("Loading brokers for auto-complete")
	brokerIDs, err := adminClient.GetBrokerIDs(ctx)
	if err != nil {
		return nil, err
	}
	sort.Slice(brokerIDs, func(a, b int) bool {
		return brokerIDs[a] < brokerIDs[b]
	})

	brokerAndTopicSuggestions := []prompt.Suggest{}

	for _, brokerID := range brokerIDs {
		brokerAndTopicSuggestions = append(
			brokerAndTopicSuggestions,
			prompt.Suggest{
				Text:        fmt.Sprintf("%d", brokerID),
				Description: fmt.Sprintf("Broker %d", brokerID),
			},
		)
	}
	for _, topicName := range topicNames {
		brokerAndTopicSuggestions = append(
			brokerAndTopicSuggestions,
			prompt.Suggest{
				Text:        topicName,
				Description: fmt.Sprintf("Topic %s", topicName),
			},
		)
	}

	log.Debug("Loading consumer groups for auto-complete")
	groupCoordinators, err := groups.GetGroups(ctx, adminClient.GetConnector())
	if err != nil {
		log.Warnf(
			"Error getting groups for auto-complete: %+v; auto-complete might not be fully functional",
			err,
		)
	}

	groupSuggestions := []prompt.Suggest{}

	for _, groupCoordinator := range groupCoordinators {
		groupSuggestions = append(
			groupSuggestions,
			prompt.Suggest{
				Text:        groupCoordinator.GroupID,
				Description: fmt.Sprintf("Group %s", groupCoordinator.GroupID),
			},
		)
	}

	return &Repl{
		cliRunner:                 cliRunner,
		brokerAndTopicSuggestions: brokerAndTopicSuggestions,
		topicSuggestions:          topicSuggestions,
		groupSuggestions:          groupSuggestions,
	}, nil
}

// Run starts the repl main loop.
func (r *Repl) Run() {
	fmt.Println("Welcome to the topicctl repl. Type 'help' for available commands.")

	promptObj := prompt.New(
		r.executor,
		r.completer,
		prompt.OptionPrefix(">>> "),
	)
	promptObj.Run()
}

func (r *Repl) executor(in string) {
	in = strings.TrimSpace(in)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()
	defer signal.Stop(sigChan)

	command := parseReplInputs(in)
	if len(command.args) == 0 {
		return
	}

	switch command.args[0] {
	case "exit":
		fmt.Println("Bye!")
		os.Exit(0)
	case "get":
		if len(command.args) == 1 {
			log.Error("Unrecognized input. Run 'help' for details on available commands.")
			return
		}

		switch command.args[1] {
		case "acls":
			if err := command.checkArgs(2, 2, nil); err != nil {
				log.Errorf("Error: %+v", err)
				return
			}
			if err := r.cliRunner.GetACLs(ctx, kafka.ACLFilter{}); err != nil {
				log.Errorf("Error: %+v", err)
				return
			}
		case "balance":
			if err := command.checkArgs(2, 3, nil); err != nil {
				log.Errorf("Error: %+v", err)
				return
			}
			var topicName string
			if len(command.args) == 3 {
				topicName = command.args[2]
			}

			if err := r.cliRunner.GetBrokerBalance(ctx, topicName); err != nil {
				log.Errorf("Error: %+v", err)
				return
			}
		case "brokers":
			if err := command.checkArgs(2, 2, map[string]struct{}{"full": {}}); err != nil {
				log.Errorf("Error: %+v", err)
				return
			}
			if err := r.cliRunner.GetBrokers(ctx, command.getBoolValue("full")); err != nil {
				log.Errorf("Error: %+v", err)
				return
			}
		case "config":
			if err := command.checkArgs(3, 3, nil); err != nil {
				log.Errorf("Error: %+v", err)
				return
			}
			if err := r.cliRunner.GetConfig(ctx, command.args[2]); err != nil {
				log.Errorf("Error: %+v", err)
				return
			}
		case "groups":
			if err := command.checkArgs(2, 2, nil); err != nil {
				log.Errorf("Error: %+v", err)
				return
			}
			if err := r.cliRunner.GetGroups(ctx); err != nil {
				log.Errorf("Error: %+v", err)
				return
			}
		case "lags":
			if err := command.checkArgs(
				4,
				4,
				map[string]struct{}{"full": {}, "sort-values": {}},
			); err != nil {
				log.Errorf("Error: %+v", err)
				return
			}
			if err := r.cliRunner.GetMemberLags(
				ctx,
				command.args[2],
				command.args[3],
				command.getBoolValue("full"),
				command.getBoolValue("sort-values"),
			); err != nil {
				log.Errorf("Error: %+v", err)
				return
			}
		case "members":
			if err := command.checkArgs(3, 3, map[string]struct{}{"full": {}}); err != nil {
				log.Errorf("Error: %+v", err)
				return
			}
			if err := r.cliRunner.GetGroupMembers(
				ctx,
				command.args[2],
				command.getBoolValue("full"),
			); err != nil {
				log.Errorf("Error: %+v", err)
				return
			}
		case "partitions":
			//
			// from terminal, topicctl get partitions can take more than one argument
			//
			// from repl, filtering multiple topics can get tricky.
			// current repl implementation takes only fixed number of words (command.args)
			// hence in repl, we will make get partitions work with only
			// one argument (topic) and PartitionStatus as "" implying all status
			//
			// repl get partitions expect minimum of 3 arguments and maximum of 4
			// repl> get partitions <topic> -> this works
			// repl> get partitions <topic> --summary -> this works
			// repl> get partitions <topic1> <topic2> -> this works only for <topic1>
			// repl> get partitions <topic1> <topic2> --summary -> this will not work
			//
			if err := command.checkArgs(3, 4, map[string]struct{}{"summary": {}}); err != nil {
				log.Errorf("Error: %+v", err)
				return
			}

			if err := r.cliRunner.GetPartitions(
				ctx,
				[]string{command.args[2]},
				admin.PartitionStatus(""),
				command.getBoolValue("summary"),
			); err != nil {
				log.Errorf("Error: %+v", err)
				return
			}
		case "offsets":
			if err := command.checkArgs(3, 3, nil); err != nil {
				log.Errorf("Error: %+v", err)
				return
			}
			if err := r.cliRunner.GetOffsets(ctx, command.args[2]); err != nil {
				log.Errorf("Error: %+v", err)
				return
			}
		case "topics":
			if err := command.checkArgs(2, 2, nil); err != nil {
				log.Errorf("Error: %+v", err)
				return
			}
			if err := r.cliRunner.GetTopics(ctx, false); err != nil {
				log.Errorf("Error: %+v", err)
				return
			}
		case "users":
			if err := command.checkArgs(2, 2, nil); err != nil {
				log.Errorf("Error: %+v", err)
				return
			}
			if err := r.cliRunner.GetUsers(ctx, nil); err != nil {
				log.Errorf("Error: %+v", err)
				return
			}
		default:
			log.Error("Unrecognized input. Run 'help' for details on available commands.")
		}
	case "help":
		if err := command.checkArgs(1, 1, nil); err != nil {
			log.Errorf("Error: %+v", err)
			return
		}

		fmt.Printf("> Commands:\n%s\n", helpTableStr)
		return
	case "tail":
		if err := command.checkArgs(
			2,
			3,
			map[string]struct{}{"filter": {}, "raw": {}},
		); err != nil {
			log.Errorf("Error: %+v", err)
			return
		}

		// Support filter as either an arg or a flag for backwards-compatibility purposes
		var filterRegexp string
		if len(command.args) == 3 {
			filterRegexp = command.args[2]
		} else {
			filterRegexp = command.flags["filter"]
		}

		err := r.cliRunner.Tail(
			ctx,
			command.args[1],
			kafka.LastOffset,
			nil,
			-1,
			filterRegexp,
			command.getBoolValue("raw"),
			command.getBoolValue("headers"),
			command.flags["group_id"],
		)
		if err != nil {
			log.Errorf("Error: %+v", err)
		}
	default:
		if len(in) > 0 {
			log.Error("Unrecognized input. Run 'help' for details on available commands.")
		}
	}
}

func (r *Repl) completer(doc prompt.Document) []prompt.Suggest {
	var suggestions []prompt.Suggest
	text := doc.TextBeforeCursor()

	if text != "" {
		words := strings.Split(text, " ")
		if len(words) == 1 {
			suggestions = commandSuggestions
		} else if len(words) == 2 && words[0] == "get" {
			suggestions = getSuggestions
		} else if len(words) == 3 && words[0] == "get" &&
			(words[1] == "balance" ||
				words[1] == "lags" ||
				words[1] == "partitions" ||
				words[1] == "offsets") {
			suggestions = r.topicSuggestions
		} else if len(words) == 4 && words[0] == "get" && words[1] == "lags" {
			suggestions = r.groupSuggestions
		} else if len(words) == 3 && words[0] == "get" && words[1] == "members" {
			suggestions = r.groupSuggestions
		} else if len(words) == 3 && words[0] == "get" && words[1] == "config" {
			suggestions = r.brokerAndTopicSuggestions
		} else if len(words) == 2 && words[0] == "tail" {
			suggestions = r.topicSuggestions
		}
	}

	return prompt.FilterHasPrefix(
		suggestions,
		doc.GetWordBeforeCursor(),
		true,
	)
}

func helpTable() string {
	buf := &bytes.Buffer{}

	table := tablewriter.NewWriter(buf)
	table.SetAutoWrapText(false)
	table.SetColumnAlignment(
		[]int{
			tablewriter.ALIGN_LEFT,
			tablewriter.ALIGN_LEFT,
		},
	)
	table.SetColumnSeparator("")
	table.SetBorders(
		tablewriter.Border{
			Left:   false,
			Top:    false,
			Right:  false,
			Bottom: false,
		},
	)

	table.AppendBulk(
		[][]string{
			{
				"  get acls",
				"Get all ACLs",
			},
			{
				"  get balance [optional topic]",
				"Get positions of all brokers in topic or across cluster",
			},
			{
				"  get brokers [--full]",
				"Get all brokers",
			},
			{
				"  get config [broker or topic]",
				"Get config for a broker or topic",
			},
			{
				"  get groups",
				"Get all consumer groups",
			},
			{
				"  get lags [topic] [group] [--full] [--sort-values]",
				"Get consumer group lags for all partitions in a topic",
			},
			{
				"  get members [group] [--full]",
				"Get the members of a consumer group",
			},
			{
				"  get partitions [topic]  [--summary]",
				"Get all partitions for a topic",
			},
			{
				"  get offsets [topic]",
				"Get the offset ranges for all partitions in a topic",
			},
			{
				"  get topics",
				"Get all topics",
			},
			{
				"  get users",
				"Get all users",
			},
			{
				"  tail [topic] [optional filter regexp] [--raw]",
				"Tail all messages in a topic",
			},
			{
				"  exit",
				"Exit the repl",
			},
		},
	)

	table.Render()
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}
