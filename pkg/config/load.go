package config

import (
	"errors"
	"io/ioutil"

	"github.com/ghodss/yaml"
	"github.com/hashicorp/go-multierror"
)

func LoadClusterFile(path string) (ClusterConfig, error) {
	contents, err := ioutil.ReadFile(path)
	if err != nil {
		return ClusterConfig{}, err
	}
	return LoadClusterBytes(contents)
}

func LoadClusterBytes(contents []byte) (ClusterConfig, error) {
	config := ClusterConfig{}
	err := yaml.Unmarshal(contents, &config)
	return config, err
}

func LoadTopicFile(path string) (TopicConfig, error) {
	contents, err := ioutil.ReadFile(path)
	if err != nil {
		return TopicConfig{}, err
	}
	return LoadTopicBytes(contents)
}

func LoadTopicBytes(contents []byte) (TopicConfig, error) {
	config := TopicConfig{}
	err := yaml.Unmarshal(contents, &config)
	return config, err
}

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
