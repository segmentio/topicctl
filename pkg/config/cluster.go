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
	Shard       int    `json:"shard"`
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

// TLSConfig contains the details required to use TLS in communication with broker clients.
type TLSConfig struct {
	// Enabled is whether TLS is enabled.
	Enabled bool `json:"enabled"`

	// CACertPath is the path the CA certificate file
	CACertPath string `json:"caCertPath"`

	// CertPath is the path to the client certificate file
	CertPath string `json:"certPath"`

	// KeyPath is the path to the client secret key
	KeyPath string `json:"keyPath"`

	// ServerName is the name that should be used to validate the server certificate. Optional,
	// if not set defaults to the name in the broker address.
	ServerName string `json:"serverName"`

	// SkipVerify indicates whether we should skip all verification of the server TLS
	// certificate.
	SkipVerify bool `json:"skipVerify"`
}

// SASLConfig contains the details required to use SASL to authenticate cluster clients.
type SASLConfig struct {
	// Enabled is whether SASL is enabled.
	Enabled bool `json:"enabled"`

	// Mechanism is the name of the SASL mechanism. Valid values are AWS-MSK-IAM, PLAIN,
	// SCRAM-SHA-256, and SCRAM-SHA-512 (case insensitive).
	Mechanism string `json:"mechanism"`

	// Username is the SASL username. Ignored if mechanism is AWS-MSK-IAM.
	Username string `json:"username"`

	// Password is the SASL password. Ignored if mechanism is AWS-MSK-IAM.
	Password string `json:"password"`

	// Intermediate role ARN to assume. Only used if mechanism is AWS-MSK-IAM.
	AssumeRole string `json:"assumeRole"`
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
		saslMechanism, saslErr := admin.SASLNameToMechanism(c.Spec.SASL.Mechanism)
		if saslErr != nil {
			err = multierror.Append(err, saslErr)
		}

		if saslMechanism == admin.SASLMechanismAWSMSKIAM &&
			(c.Spec.SASL.Username != "" || c.Spec.SASL.Password != "") {
			log.Warn("Username and password are ignored if using SASL AWS-MSK-IAM")
		}

		if saslMechanism != admin.SASLMechanismAWSMSKIAM && c.Spec.SASL.AssumeRole != "" {
			log.Warn("AssumeRole is ignored unless using SASL AWS-MSK-IAM")
		}
	}

	return err
}

// GetDefaultRetentionDropStepDuration gets the default step size to use when reducing
// the message retention in a topic.
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

		var saslMechanism admin.SASLMechanism
		var err error

		if c.Spec.SASL.Mechanism != "" {
			saslMechanism, err = admin.SASLNameToMechanism(c.Spec.SASL.Mechanism)
			if err != nil {
				return nil, err
			}
		}

		return admin.NewBrokerAdminClient(
			ctx,
			admin.BrokerAdminClientConfig{
				ConnectorConfig: admin.ConnectorConfig{
					BrokerAddr: c.Spec.BootstrapAddrs[0],
					TLS: admin.TLSConfig{
						Enabled:    c.Spec.TLS.Enabled,
						CACertPath: c.absPath(c.Spec.TLS.CACertPath),
						CertPath:   c.absPath(c.Spec.TLS.CertPath),
						KeyPath:    c.absPath(c.Spec.TLS.KeyPath),
						ServerName: c.Spec.TLS.ServerName,
						SkipVerify: c.Spec.TLS.SkipVerify,
					},
					SASL: admin.SASLConfig{
						Enabled:    c.Spec.SASL.Enabled,
						Mechanism:  saslMechanism,
						Username:   saslUsername,
						Password:   saslPassword,
						AssumeRole: c.Spec.SASL.AssumeRole,
					},
				},
				ExpectedClusterID: c.Spec.ClusterID,
				ReadOnly:          readOnly,
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
