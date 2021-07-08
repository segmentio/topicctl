package cli

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	assert.Equal(
		t,
		cliCommand{
			args:  []string{"arg1", "arg2"},
			flags: map[string]string{},
		},
		parseInputs("arg1   arg2"),
	)
	assert.Equal(
		t,
		cliCommand{
			args:  []string{"--flag1=value1", "arg1", "arg2"},
			flags: map[string]string{},
		},
		parseInputs("--flag1=value1  arg1   arg2"),
	)
	assert.Equal(
		t,
		cliCommand{
			args: []string{"arg1", "arg2", "arg3"},
			flags: map[string]string{
				"flag1": "value1",
				"flag2": "value2",
			},
		},
		parseInputs("arg1 arg2 --flag1=value1 arg3 --flag2=value2"),
	)
}

func TestGetBoolValue(t *testing.T) {
	command := cliCommand{
		flags: map[string]string{
			"key1": "",
			"key2": "true",
			"key3": "false",
		},
	}
	assert.True(t, command.getBoolValue("key1"))
	assert.True(t, command.getBoolValue("key2"))
	assert.False(t, command.getBoolValue("key3"))
	assert.False(t, command.getBoolValue("non-existent-key"))
}
