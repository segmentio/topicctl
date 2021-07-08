package cli

import "strings"

type cliCommand struct {
	args  []string
	flags map[string]string
}

func (c cliCommand) getBoolValue(key string) bool {
	value, ok := c.flags[key]

	if value == "true" {
		return true
	} else if value == "" && ok {
		// If key is set but value is not, treat this as "true"
		return true
	} else {
		return false
	}
}

// parse parses the CLI inputs into command args and flags.
func parseInputs(input string) cliCommand {
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

	return cliCommand{
		args:  args,
		flags: flags,
	}
}
