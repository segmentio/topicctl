package config

import (
	"errors"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/ghodss/yaml"
	"github.com/hashicorp/go-multierror"
)

var sep = regexp.MustCompile("(?:^|\\s*\n)---\\s*")

// LoadClusterFile loads a ClusterConfig from a path to a YAML file.
func LoadClusterFile(path string) (ClusterConfig, error) {
	contents, err := ioutil.ReadFile(path)
	if err != nil {
		return ClusterConfig{}, err
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		return ClusterConfig{}, err
	}

	config, err := LoadClusterBytes(contents)
	if err != nil {
		return ClusterConfig{}, err
	}

	config.RootDir = filepath.Dir(absPath)
	return config, nil
}

// LoadClusterBytes loads a ClusterConfig from YAML bytes.
func LoadClusterBytes(contents []byte) (ClusterConfig, error) {
	config := ClusterConfig{}
	err := yaml.Unmarshal(contents, &config)
	return config, err
}

// LoadTopicsFile loads one or more TopicConfigs from a path to a YAML file.
func LoadTopicsFile(path string) ([]TopicConfig, error) {
	contents, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	trimmedFile := strings.TrimSpace(string(contents))
	topicStrs := sep.Split(trimmedFile, -1)

	topicConfigs := []TopicConfig{}

	for _, topicStr := range topicStrs {
		topicStr = strings.TrimSpace(topicStr)
		if isEmpty(topicStr) {
			continue
		}

		topicConfig, err := LoadTopicBytes([]byte(topicStr))
		if err != nil {
			return nil, err
		}

		topicConfigs = append(topicConfigs, topicConfig)
	}

	return topicConfigs, nil
}

// LoadTopicBytes loads a TopicConfig from YAML bytes.
func LoadTopicBytes(contents []byte) (TopicConfig, error) {
	config := TopicConfig{}
	err := yaml.Unmarshal(contents, &config)
	return config, err
}

// CheckConsistency verifies that the argument topic config is consistent with the argument
// cluster, e.g. has the same environment and region, etc.
func CheckConsistency(topicConfig TopicConfig, clusterConfig ClusterConfig) error {
	var err error

	if topicConfig.Meta.Cluster != clusterConfig.Meta.Name {
		err = multierror.Append(
			err,
			errors.New("Topic cluster name does not match name in cluster config"),
		)
	}
	if topicConfig.Meta.Environment != clusterConfig.Meta.Environment {
		err = multierror.Append(
			err,
			errors.New("Topic environment does not match cluster environment"),
		)
	}
	if topicConfig.Meta.Region != clusterConfig.Meta.Region {
		err = multierror.Append(
			err,
			errors.New("Topic region does not match cluster region"),
		)
	}

	return err
}

func isEmpty(contents string) bool {
	lines := strings.Split(contents, "\n")
	for _, line := range lines {
		trimmedLine := strings.TrimSpace(line)
		if len(trimmedLine) > 0 && !strings.HasPrefix(trimmedLine, "#") {
			return false
		}
	}

	return true
}
