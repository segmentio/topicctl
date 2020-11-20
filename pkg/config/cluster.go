package config

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/hashicorp/go-multierror"
	"github.com/segmentio/topicctl/pkg/admin"
)

// KafkaVersionMajor is a string type for storing Kafka versions.
type KafkaVersionMajor string

const (
	// KafkaVersionMajor010 represents kafka v0.10 and its associated minor versions.
	KafkaVersionMajor010 KafkaVersionMajor = "v0.10"

	// KafkaVersionMajor2 represents kafka v2 and its associated minor versions.
	KafkaVersionMajor2 KafkaVersionMajor = "v2"
)

// ClusterConfig stores information about a cluster that's referred to by one
// or more topic configs. These configs should reflect the reality of what's been
// set up externally; there's no way to "apply" these at the moment.
type ClusterConfig struct {
	Meta ClusterMeta `json:"meta"`
	Spec ClusterSpec `json:"spec"`
}

// ClusterMeta contains (mostly immutable) metadata about the cluster. Inspired
// by the meta fields in Kubernetes objects.
type ClusterMeta struct {
	Name        string `json:"name"`
	Region      string `json:"region"`
	Environment string `json:"environment"`
	Description string `json:"description"`
}

// ClusterSpec contains the details necessary to communicate with a kafka cluster.
type ClusterSpec struct {
	// BootstrapAddrs is a list of one or more broker bootstrap addresses. These can use IPs
	// or DNS names.
	BootstrapAddrs []string `json:"bootstrapAddrs"`

	// ZKAddrs is a list of one or more zookeeper addresses. These can use IPs
	// or DNS names.
	ZKAddrs []string `json:"zkAddrs"`

	// ZKPrefix is the prefix under which all zk nodes for the cluster are stored. If blank,
	// these are assumed to be under the zk root.
	ZKPrefix string `json:"zkPrefix"`

	// ZKLockPath indicates where locks are stored in zookeeper. If blank, then
	// no locking will be used on apply operations.
	ZKLockPath string `json:"zkLockPath"`

	// ClusterID is the value of the [prefix]/cluster/id node in zookeeper. If set, it's used
	// to validate that the cluster we're communicating with is the right one. If blank,
	// this check isn't done.
	ClusterID string `json:"clusterID"`

	// VersionMajor stores the major version of the cluster. This isn't currently
	// used for any logic in the tool, but it may be used in the future to adjust API calls
	// and/or decide whether to use zk or brokers for certain information.
	VersionMajor KafkaVersionMajor `json:"versionMajor"`

	// DefaultThrottleMB is the default broker throttle used for migrations in this
	// cluster. If unset, then a reasonable default is used instead.
	DefaultThrottleMB int64 `json:"defaultThrottleMB"`

	// DefaultRetentionDropStepDuration is the default amount of time that retention drops will be
	// limited by. If unset, no retention drop limiting will be applied.
	DefaultRetentionDropStepDurationStr string `json:"defaultRetentionDropStepDuration"`

	// UseBrokerAdmin indicates whether we should use a broker-api-based admin (if true) or
	// the old, zk-based admin (if false).
	UseBrokerAdmin bool `json:"useBrokerAdmin"`
}

// Validate evaluates whether the cluster config is valid.
func (c ClusterConfig) Validate() error {
	var err error

	if c.Meta.Name == "" {
		err = multierror.Append(err, errors.New("Name must be set"))
	}
	if c.Meta.Region == "" {
		err = multierror.Append(err, errors.New("Region must be set"))
	}
	if c.Meta.Environment == "" {
		err = multierror.Append(err, errors.New("Environment must be set"))
	}

	if len(c.Spec.BootstrapAddrs) == 0 {
		err = multierror.Append(
			err,
			errors.New("At least one bootstrap broker address must be set"),
		)
	}
	if len(c.Spec.ZKAddrs) == 0 && !c.Spec.UseBrokerAdmin {
		err = multierror.Append(err, errors.New("At least one zookeeper address must be set"))
	}
	if c.Spec.VersionMajor != KafkaVersionMajor010 &&
		c.Spec.VersionMajor != KafkaVersionMajor2 {
		multierror.Append(err, errors.New("MajorVersion must be v0.10 or v2"))
	}

	_, parseErr := c.GetDefaultRetentionDropStepDuration()
	if parseErr != nil {
		err = multierror.Append(
			err,
			fmt.Errorf("Error parsing retention drop step retention: %+v", parseErr),
		)
	}

	return err
}

func (c ClusterConfig) GetDefaultRetentionDropStepDuration() (time.Duration, error) {
	if c.Spec.DefaultRetentionDropStepDurationStr == "" {
		return 0, nil
	}

	return time.ParseDuration(c.Spec.DefaultRetentionDropStepDurationStr)
}

// NewAdminClient returns a new admin client using the parameters in the current cluster config.
func (c ClusterConfig) NewAdminClient(
	ctx context.Context,
	sess *session.Session,
	readOnly bool,
) (admin.Client, error) {
	if c.Spec.UseBrokerAdmin {
		return admin.NewBrokerAdminClient(
			ctx,
			admin.BrokerAdminClientConfig{
				BrokerAddr: c.Spec.BootstrapAddrs[0],
				ReadOnly:   readOnly,
			},
		)
	} else {
		return admin.NewZKAdminClient(
			ctx,
			admin.ZKAdminClientConfig{
				ZKAddrs:           c.Spec.ZKAddrs,
				ZKPrefix:          c.Spec.ZKPrefix,
				BootstrapAddrs:    c.Spec.BootstrapAddrs,
				ExpectedClusterID: c.Spec.ClusterID,
				Sess:              sess,
				ReadOnly:          readOnly,
			},
		)
	}
}
