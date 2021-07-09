package cli

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseReplInputs(t *testing.T) {
	assert.Equal(
		t,
		replCommand{
			args:  []string{"arg1", "arg2"},
			flags: map[string]string{},
		},
		parseReplInputs("arg1   arg2"),
	)
	assert.Equal(
		t,
		replCommand{
			args:  []string{"--flag1=value1", "arg1", "arg2"},
			flags: map[string]string{},
		},
		parseReplInputs("--flag1=value1  arg1   arg2"),
	)
	assert.Equal(
		t,
		replCommand{
			args: []string{"arg1", "arg2", "arg3"},
			flags: map[string]string{
				"flag1": "value1",
				"flag2": "value2",
			},
		},
		parseReplInputs("arg1 arg2 --flag1=value1 arg3 --flag2=value2"),
	)
}

func TestGetBoolValue(t *testing.T) {
	command := replCommand{
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

func TestCheckArgs(t *testing.T) {
	command := replCommand{
		args: []string{
			"arg1",
			"arg2",
		},
		flags: map[string]string{
			"key1": "value1",
		},
	}
	assert.NoError(t, command.checkArgs(2, 2, map[string]struct{}{"key1": {}}))
	assert.NoError(t, command.checkArgs(2, 3, map[string]struct{}{"key1": {}}))
	assert.NoError(t, command.checkArgs(1, 2, map[string]struct{}{"key1": {}}))
	assert.NoError(t, command.checkArgs(1, 2, map[string]struct{}{"key1": {}, "key2": {}}))
	assert.Error(t, command.checkArgs(3, 3, map[string]struct{}{"key1": {}}))
	assert.Error(t, command.checkArgs(3, 5, map[string]struct{}{"key1": {}}))
	assert.Error(t, command.checkArgs(2, 2, map[string]struct{}{"key2": {}}))
	assert.Error(t, command.checkArgs(2, 2, nil))
}
