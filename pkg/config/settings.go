package config

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/admin"
	log "github.com/sirupsen/logrus"
)

type configValidator func(v string) bool

var keyValidators = map[string]configValidator{
	"cleanup.policy": func(v string) bool {
		subValues := strings.Split(v, ",")
		if len(subValues) > 2 {
			return false
		}

		for _, subValue := range subValues {
			if !inValues(subValue, "compact", "delete") {
				return false
			}
		}

		return true
	},
	"compression.type": func(v string) bool {
		return inValues(v, "uncompressed", "zstd", "lz4", "snappy", "gzip", "producer")
	},
	"delete.retention.ms": func(v string) bool {
		intVal, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return false
		}
		return intVal >= 0
	},
	"file.delete.delay.ms": func(v string) bool {
		intVal, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return false
		}
		return intVal >= 0
	},
	"flush.messages": func(v string) bool {
		intVal, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return false
		}
		return intVal >= 0
	},
	"flush.ms": func(v string) bool {
		intVal, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return false
		}
		return intVal >= 0
	},
	"follower.replication.throttled.replicas": func(v string) bool {
		if v == "*" {
			return true
		}
		subValues := strings.Split(v, ",")
		for _, subValue := range subValues {
			elements := strings.Split(subValue, ":")
			if len(elements) != 2 {
				return false
			}
			if _, err := strconv.ParseInt(elements[0], 10, 64); err != nil {
				return false
			}
			if _, err := strconv.ParseInt(elements[1], 10, 64); err != nil {
				return false
			}
		}

		return true
	},
	"index.interval.bytes": func(v string) bool {
		intVal, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return false
		}
		return intVal >= 0
	},
	"leader.replication.throttled.replicas": func(v string) bool {
		if v == "*" {
			return true
		}
		subValues := strings.Split(v, ",")
		for _, subValue := range subValues {
			elements := strings.Split(subValue, ":")
			if len(elements) != 2 {
				return false
			}
			if _, err := strconv.ParseInt(elements[0], 10, 64); err != nil {
				return false
			}
			if _, err := strconv.ParseInt(elements[1], 10, 64); err != nil {
				return false
			}
		}

		return true
	},
	"max.compaction.lag.ms": func(v string) bool {
		intVal, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return false
		}
		return intVal >= 1
	},
	"max.message.bytes": func(v string) bool {
		intVal, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return false
		}
		return intVal >= 0
	},
	"message.format.version": func(v string) bool {
		return inValues(
			v,
			"0.8.0",
			"0.8.1",
			"0.8.2",
			"0.9.0",
			"0.10.0-IV0",
			"0.10.0-IV1",
			"0.10.1-IV0",
			"0.10.1-IV1",
			"0.10.1-IV2",
			"0.10.2-IV0",
			"0.11.0-IV0",
			"0.11.0-IV1",
			"0.11.0-IV2",
			"1.0-IV0",
			"1.1-IV0",
			"2.0-IV0",
			"2.0-IV1",
			"2.1-IV0",
			"2.1-IV1",
			"2.1-IV2",
			"2.2-IV0",
			"2.2-IV1",
			"2.3-IV0",
			"2.3-IV1",
			"2.4-IV0",
			"2.4-IV1",
			"2.5-IV0",
			"2.6-IV0",
		)
	},
	"message.timestamp.difference.max.ms": func(v string) bool {
		intVal, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return false
		}
		return intVal >= 0
	},
	"message.timestamp.type": func(v string) bool {
		return inValues(v, "CreateTime", "LogAppendTime")
	},
	"min.cleanable.dirty.ratio": func(v string) bool {
		floatVal, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return false
		}
		return floatVal >= 0 && floatVal <= 1.0
	},
	"min.compaction.lag.ms": func(v string) bool {
		intVal, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return false
		}
		return intVal >= 0
	},
	"min.insync.replicas": func(v string) bool {
		intVal, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return false
		}
		return intVal >= 1
	},
	"preallocate": func(v string) bool {
		_, err := strconv.ParseBool(v)
		return err == nil
	},
	"retention.bytes": func(v string) bool {
		intVal, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return false
		}
		return intVal >= -1
	},
	"retention.ms": func(v string) bool {
		intVal, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return false
		}
		return intVal >= -1
	},
	"local.retention.bytes": func(v string) bool {
		intVal, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return false
		}
		return intVal >= -1
	},
	"local.retention.ms": func(v string) bool {
		intVal, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return false
		}
		return intVal >= -1
	},
	"remote.storage.enable": func(v string) bool {
		_, err := strconv.ParseBool(v)
		return err == nil
	},
	"remote.log.msk.disable.policy": func(v string) bool {
		return inValues(v, "Delete")
	},
	"segment.bytes": func(v string) bool {
		intVal, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return false
		}
		return intVal >= 14
	},
	"segment.index.bytes": func(v string) bool {
		intVal, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return false
		}
		return intVal >= 0
	},
	"segment.jitter.ms": func(v string) bool {
		intVal, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return false
		}
		return intVal >= 0
	},
	"segment.ms": func(v string) bool {
		intVal, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return false
		}
		return intVal >= -1
	},
	"unclean.leader.election.enable": func(v string) bool {
		_, err := strconv.ParseBool(v)
		return err == nil
	},
}

// TopicSettings is a map of key/value pairs that correspond to Kafka
// topic config settings.
type TopicSettings map[string]interface{}

// Validate determines whether the given settings are valid. See
// https://kafka.apache.org/documentation/#topicconfigs for details.
func (t TopicSettings) Validate() error {
	var validateErr error

	for key, value := range t {
		validator, ok := keyValidators[key]
		if !ok {
			validateErr = multierror.Append(
				validateErr,
				fmt.Errorf("Key %s is not recognized topic config setting", key),
			)
			continue
		}

		valueStr, err := interfaceToString(value)
		if err != nil {
			validateErr = multierror.Append(
				validateErr,
				fmt.Errorf(
					"Could not convert value for key %s to string: %+v",
					key,
					err,
				),
			)
			continue
		}

		if valueStr == "" {
			continue
		}

		valid := validator(valueStr)
		if !valid {
			validateErr = multierror.Append(
				validateErr,
				fmt.Errorf("Invalid value for key %s: %s", key, valueStr),
			)
		}
	}

	return validateErr
}

// ToConfigEntries converts the argument keys in the current settings into a slice of
// kafka-go config entries. If keys is nil, then all fields are converted.
func (t TopicSettings) ToConfigEntries(keys []string) ([]kafka.ConfigEntry, error) {
	entries := []kafka.ConfigEntry{}

	if keys == nil {
		for key, value := range t {
			strValue, err := interfaceToString(value)
			if err != nil {
				return nil, fmt.Errorf("Error converting value for key %s: %+v", key, err)
			}

			entries = append(
				entries,
				kafka.ConfigEntry{
					ConfigName:  key,
					ConfigValue: strValue,
				},
			)
		}
	} else {
		for _, key := range keys {
			value, ok := t[key]
			if !ok {
				return nil, fmt.Errorf("Key %s not found", key)
			}

			strValue, err := interfaceToString(value)
			if err != nil {
				return nil, fmt.Errorf("Error converting value for key %s: %+v", key, err)
			}

			entries = append(
				entries,
				kafka.ConfigEntry{
					ConfigName:  key,
					ConfigValue: strValue,
				},
			)
		}
	}

	return entries, nil
}

// HasKey returns whether the current settings instance contains the argument key.
func (t TopicSettings) HasKey(key string) bool {
	_, ok := t[key]
	return ok
}

// GetValueStr returns the string value for a key in this settings instance. It
// returns an error if the key is not found.
func (t TopicSettings) GetValueStr(key string) (string, error) {
	value, ok := t[key]
	if !ok {
		return "", fmt.Errorf("Key %s not found", key)
	}
	return interfaceToString(value)
}

// ConfigMapDiffs compares these topic settings to a string map fetched from
// the cluster. It returns the keys that are set in the settings but different in
// the cluster and also the keys that are set in the cluster but not set in
// the settings.
func (t TopicSettings) ConfigMapDiffs(
	configMap map[string]string,
) ([]string, []string, error) {
	diffKeys := []string{}
	missingKeys := []string{}

	for key, value := range t {
		strValue, err := interfaceToString(value)
		if err != nil {
			return nil, nil, err
		}

		configStrValue := configMap[key]
		if strValue != configStrValue {
			diffKeys = append(diffKeys, key)
		}
	}

	for configKey := range configMap {
		_, ok := t[configKey]
		if !ok {
			missingKeys = append(missingKeys, configKey)
		}
	}

	return diffKeys, missingKeys, nil
}

// ReduceRetentionDrop updates the retention in this TopicSettings instance so that it's dropping
// by no more than the argument retentionDropStepDuration.
func (t TopicSettings) ReduceRetentionDrop(
	configMap map[string]string,
	retentionDropStepDuration time.Duration,
) (bool, error) {
	if retentionDropStepDuration <= 0 {
		return false, nil
	}

	currRetentionMsStr, ok := configMap[admin.RetentionKey]
	if !ok || currRetentionMsStr == "" {
		// No retention currently set
		return false, nil
	}
	currRetentionMs, err := strconv.ParseInt(currRetentionMsStr, 10, 64)
	if err != nil {
		// Parse error
		return false, err
	}

	var setRetentionMsIface interface{}

	for key, value := range t {
		if key == admin.RetentionKey {
			setRetentionMsIface = value
			break
		}
	}

	if setRetentionMsIface == nil {
		// Retention not configured in topic settings
		return false, nil
	}
	setRetentionMs, err := interfaceToInt64(setRetentionMsIface)
	if err != nil {
		// Parse error
		return false, err
	}

	maxDropMs := retentionDropStepDuration.Milliseconds()

	if currRetentionMs-setRetentionMs > maxDropMs {
		// Reduce drop
		log.Debugf(
			"Updating retention from %d to %d ms",
			setRetentionMs,
			currRetentionMs-maxDropMs,
		)
		t[admin.RetentionKey] = currRetentionMs - maxDropMs
		return true, nil
	}

	return false, nil
}

// Copy returns a shallow copy of this settings instance.
func (t TopicSettings) Copy() TopicSettings {
	copy := TopicSettings{}

	for key, value := range t {
		copy[key] = value
	}

	return copy
}

// FromConfigMap converts a string map from a Kafka topic to a TopicSettings instance.
func FromConfigMap(configMap map[string]string) TopicSettings {
	t := TopicSettings{}
	for key, value := range configMap {
		t[key] = value
	}
	return t
}

func interfaceToString(v interface{}) (string, error) {
	if v == nil {
		return "", nil
	}

	switch t := v.(type) {
	case bool:
		return strconv.FormatBool(t), nil
	case float32:
		if t == float32(int64(t)) {
			// Treat this value as an int
			return strconv.FormatInt(int64(t), 10), nil
		}

		return strconv.FormatFloat(float64(t), 'f', 2, 32), nil
	case float64:
		if t == float64(int64(t)) {
			// Treat this value as an int
			return strconv.FormatInt(int64(t), 10), nil
		}

		return strconv.FormatFloat(t, 'f', 2, 64), nil
	case int:
		return strconv.FormatInt(int64(t), 10), nil
	case int64:
		return strconv.FormatInt(t, 10), nil
	case string:
		return t, nil
	case []string:
		return strings.Join(t, ","), nil
	case []interface{}:
		strValues := []string{}

		for _, item := range t {
			itemStr, err := interfaceToString(item)
			if err != nil {
				return "", err
			}
			strValues = append(strValues, itemStr)
		}
		return strings.Join(strValues, ","), nil
	}

	return "", fmt.Errorf("Invalid setting value: %+v (%s)", v, reflect.TypeOf(v))
}

func interfaceToInt64(v interface{}) (int64, error) {
	if v == nil {
		return 0, nil
	}

	switch t := v.(type) {
	case float32:
		return int64(t), nil
	case float64:
		return int64(t), nil
	case int:
		return int64(t), nil
	case int64:
		return t, nil
	case string:
		return strconv.ParseInt(t, 10, 64)
	default:
		return 0, fmt.Errorf(
			"Could not convert value %+v (type %+v) to int64",
			v,
			reflect.TypeOf(v),
		)
	}
}

func inValues(v string, values ...string) bool {
	valuesMap := map[string]struct{}{}
	for _, value := range values {
		valuesMap[value] = struct{}{}
	}

	_, ok := valuesMap[v]
	return ok
}
