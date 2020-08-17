package config

import (
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

func TestValidateSettings(t *testing.T) {
	type testCase struct {
		description string
		settings    TopicSettings
		expError    bool
	}

	testCases := []testCase{
		{
			description: "base types",
			settings: TopicSettings{
				"cleanup.policy":            "compact",
				"retention.ms":              1234,
				"min.cleanable.dirty.ratio": 0.54,
				"preallocate":               true,
				"follower.replication.throttled.replicas": []string{
					"1:3",
					"4:5",
					"6:7",
				},
				"leader.replication.throttled.replicas": []string{
					"1:3",
					"4:5",
					"6:7",
				},
			},
			expError: false,
		},
		{
			description: "string types",
			settings: TopicSettings{
				"cleanup.policy":                          "compact",
				"retention.ms":                            "1234",
				"min.cleanable.dirty.ratio":               "0.54",
				"preallocate":                             "true",
				"follower.replication.throttled.replicas": "1:3,4:5,6:7",
			},
			expError: false,
		},
		{
			description: "empty values",
			settings: TopicSettings{
				"cleanup.policy": "",
				"retention.ms":   "",
			},
			expError: false,
		},
		{
			description: "unrecognized key",
			settings: TopicSettings{
				"bad-key": "1",
			},
			expError: true,
		},
		{
			description: "non-matching string",
			settings: TopicSettings{
				"cleanup.policy": "non-matching",
			},
			expError: true,
		},
		{
			description: "bad int",
			settings: TopicSettings{
				"retention.ms": "not-an-int",
			},
			expError: true,
		},
		{
			description: "out-of-range int",
			settings: TopicSettings{
				"retention.ms": -100,
			},
			expError: true,
		},
		{
			description: "bad throttles",
			settings: TopicSettings{
				"follower.replication.throttled.replicas": "3,4:5",
			},
			expError: true,
		},
	}

	for _, testCase := range testCases {
		err := testCase.settings.Validate()
		if testCase.expError {
			assert.Error(t, err, testCase.description)
		} else {
			assert.NoError(t, err, testCase.description)
		}
	}
}

func TestSettingsToConfigEntries(t *testing.T) {
	settings := TopicSettings{
		"cleanup.policy": "compact",
		"follower.replication.throttled.replicas": []string{
			"1:3",
			"4:5",
			"6:7",
		},
		"leader.replication.throttled.replicas": nil,
		"min.cleanable.dirty.ratio":             0.54,
		"preallocate":                           true,
		"retention.ms":                          1234,
	}

	configEntries, err := settings.ToConfigEntries()
	assert.Nil(t, err)
	assert.ElementsMatch(
		t,
		[]kafka.ConfigEntry{
			{
				ConfigName:  "cleanup.policy",
				ConfigValue: "compact",
			},
			{
				ConfigName:  "follower.replication.throttled.replicas",
				ConfigValue: "1:3,4:5,6:7",
			},
			{
				ConfigName:  "leader.replication.throttled.replicas",
				ConfigValue: "",
			},
			{
				ConfigName:  "min.cleanable.dirty.ratio",
				ConfigValue: "0.54",
			},
			{
				ConfigName:  "preallocate",
				ConfigValue: "true",
			},
			{
				ConfigName:  "retention.ms",
				ConfigValue: "1234",
			},
		},
		configEntries,
	)

	badSettings := TopicSettings{
		"key": map[string]int{
			"abc": 123,
		},
	}
	_, err = badSettings.ToConfigEntries()
	assert.NotNil(t, err)
}
