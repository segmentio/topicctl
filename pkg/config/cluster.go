package config

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/hashicorp/go-multierror"
	"github.com/segmentio/topicctl/pkg/admin"
	log "github.com/sirupsen/logrus"
)

// ClusterConfig stores information about a cluster that's referred to by one
// or more topic configs. These configs should reflect the reality of what's been
// set up externally; there's no way to "apply" these at the moment.
type ClusterConfig struct {
	Meta ClusterMeta `json:"meta"`
	Spec ClusterSpec `json:"spec"`

	// RootDir is the root relative to which paths are evaluated. Set by loader.
	RootDir string `json:"-"`
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
	// or DNS names. If these are omitted, then the tool will use broker APIs exclusively.
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

	// DefaultThrottleMB is the default broker throttle used for migrations in this
	// cluster. If unset, then a reasonable default is used instead.
	DefaultThrottleMB int64 `json:"defaultThrottleMB"`

	// DefaultRetentionDropStepDuration is the default amount of time that retention drops will be
	// limited by. If unset, no retention drop limiting will be applied.
	DefaultRetentionDropStepDurationStr string `json:"defaultRetentionDropStepDuration"`

	// TLS stores how we should use TLS with broker connections, if appropriate. Only
	// applies if using the broker admin.
	TLS TLSConfig `json:"tls"`

	// SASL stores how we should use SASL with broker connections, if appropriate. Only
	// applies if using the broker admin.
	SASL SASLConfig `json:"sasl"`
}

type TLSConfig struct {
	Enabled    bool   `json:"enabled"`
	CACertPath string `json:"caCertPath"`
	CertPath   string `json:"certPath"`
	KeyPath    string `json:"keyPath"`
	ServerName string `json:"serverName"`
	SkipVerify bool   `json:"skipVerify"`
}

type SASLConfig struct {
	Enabled   bool   `json:"enabled"`
	Mechanism string `json:"mechanism"`
	Username  string `json:"username"`
	Password  string `json:"password"`
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

	_, parseErr := c.GetDefaultRetentionDropStepDuration()
	if parseErr != nil {
		err = multierror.Append(
			err,
			fmt.Errorf("Error parsing retention drop step retention: %+v", parseErr),
		)
	}

	if c.Spec.TLS.Enabled && len(c.Spec.ZKAddrs) > 0 {
		err = multierror.Append(
			err,
			errors.New("TLS not supported with zk access mode; omit zk addresses to fix"),
		)
	}
	if c.Spec.SASL.Enabled && len(c.Spec.ZKAddrs) > 0 {
		err = multierror.Append(
			err,
			errors.New("SASL not supported with zk access mode; omit zk addresses to fix"),
		)
	}

	if c.Spec.SASL.Enabled {
		if saslErr := admin.ValidateSASLMechanism(c.Spec.SASL.Mechanism); saslErr != nil {
			err = multierror.Append(err, saslErr)
		}
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
	usernameOverride string,
	passwordOverride string,
) (admin.Client, error) {
	if len(c.Spec.ZKAddrs) == 0 {
		log.Debug("No ZK addresses provided, using broker admin client")

		var saslUsername string
		var saslPassword string
		if usernameOverride != "" {
			log.Debugf("Setting SASL username from override value")
			saslUsername = usernameOverride
		} else {
			saslUsername = c.Spec.SASL.Username
		}

		if passwordOverride != "" {
			log.Debugf("Setting SASL password from override value")
			saslPassword = passwordOverride
		} else {
			saslPassword = c.Spec.SASL.Password
		}

		return admin.NewBrokerAdminClient(
			ctx,
			admin.BrokerAdminClientConfig{
				ConnectorConfig: admin.ConnectorConfig{
					BrokerAddr: c.Spec.BootstrapAddrs[0],
					TLS: admin.TLSConfig{
						Enabled:    c.Spec.TLS.Enabled,
						CACertPath: c.absPath(c.Spec.TLS.CACertPath),
						KeyPath:    c.absPath(c.Spec.TLS.KeyPath),
						ServerName: c.Spec.TLS.ServerName,
						SkipVerify: c.Spec.TLS.SkipVerify,
					},
					SASL: admin.SASLConfig{
						Enabled:   c.Spec.SASL.Enabled,
						Mechanism: c.Spec.SASL.Mechanism,
						Username:  saslUsername,
						Password:  saslPassword,
					},
				},
				ReadOnly: readOnly,
			},
		)
	} else {
		log.Debug("ZK addresses provided, using zk admin client")
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

func (c ClusterConfig) absPath(relPath string) string {
	if relPath == "" || c.RootDir == "" || filepath.IsAbs(relPath) {
		return relPath
	}

	return filepath.Join(c.RootDir, relPath)
}
