package cli

import (
	"fmt"
	"strings"
)

type replCommand struct {
	args  []string
	flags map[string]string
}

func (r replCommand) getBoolValue(key string) bool {
	value, ok := r.flags[key]

	if value == "true" {
		return true
	} else if value == "" && ok {
		// If key is set but value is not, treat this as "true"
		return true
	} else {
		return false
	}
}

func (r replCommand) checkArgs(
	minArgs int,
	maxArgs int,
	allowedFlags map[string]struct{},
) error {
	if minArgs == maxArgs {
		if len(r.args) != minArgs {
			return fmt.Errorf("Expected %d args", minArgs)
		}
	} else {
		if len(r.args) < minArgs || len(r.args) > maxArgs {
			return fmt.Errorf("Expected between %d and %d args", minArgs, maxArgs)
		}
	}

	for key := range r.flags {
		if allowedFlags == nil {
			return fmt.Errorf("Flag %s not recognized", key)
		}
		if _, ok := allowedFlags[key]; !ok {
			return fmt.Errorf("Flag %s not recognized", key)
		}
	}

	return nil
}

func parseReplInputs(input string) replCommand {
	args := []string{}
	flags := map[string]string{}

	components := strings.Split(input, " ")

	for c, component := range components {
		if component == "" {
			continue
		} else if c > 0 && strings.HasPrefix(component, "--") {
			subcomponents := strings.SplitN(component, "=", 2)
			key := subcomponents[0][2:]
			var value string
			if len(subcomponents) > 1 {
				value = subcomponents[1]
			}
			flags[key] = value
		} else {
			args = append(args, component)
		}
	}

	return replCommand{
		args:  args,
		flags: flags,
	}
}
