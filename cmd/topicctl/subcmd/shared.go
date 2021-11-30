package subcmd

import (
	"context"
	"errors"
	"os"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/hashicorp/go-multierror"
	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/config"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type sharedOptions struct {
	brokerAddr    string
	clusterConfig string
	expandEnv     bool
	saslMechanism string
	saslPassword  string
	saslUsername  string
	tlsCACert     string
	tlsCert       string
	tlsEnabled    bool
	tlsKey        string
	tlsSkipVerify bool
	tlsServerName string
	zkAddr        string
	zkPrefix      string
}

func (s sharedOptions) validate() error {
	var err error

	if s.clusterConfig == "" && s.zkAddr == "" && s.brokerAddr == "" {
		err = multierror.Append(
			err,
			errors.New("Must set either broker-addr, cluster-config, or zk-addr"),
		)
	}

	if s.clusterConfig != "" {
		clusterConfig, clusterConfigErr := config.LoadClusterFile(s.clusterConfig, s.expandEnv)
		if clusterConfigErr != nil {
			err = multierror.Append(
				err,
				clusterConfigErr,
			)
		} else {
			clusterConfigValidateErr := clusterConfig.Validate()

			if clusterConfigValidateErr != nil {
				err = multierror.Append(
					err,
					clusterConfigValidateErr,
				)
			}
		}
	}

	if s.zkAddr != "" && s.brokerAddr != "" {
		err = multierror.Append(
			err,
			errors.New("Cannot set both zk-addr and broker-addr"),
		)
	}
	if s.clusterConfig != "" &&
		(s.zkAddr != "" || s.zkPrefix != "" || s.brokerAddr != "" || s.tlsCACert != "" ||
			s.tlsCert != "" || s.tlsKey != "" || s.tlsServerName != "" || s.saslMechanism != "") {
		log.Warn("Broker and zk flags are ignored when using cluster-config")
	}

	if s.clusterConfig != "" {
		return err
	}

	useTLS := s.tlsEnabled || s.tlsCACert != "" || s.tlsCert != "" || s.tlsKey != ""
	useSASL := s.saslMechanism != "" || s.saslPassword != "" || s.saslUsername != ""

	if useTLS && s.zkAddr != "" {
		log.Warn("TLS flags are ignored accessing cluster via zookeeper")
	}
	if useSASL && s.zkAddr != "" {
		log.Warn("SASL flags are ignored accessing cluster via zookeeper")
	}

	if useSASL {
		saslMechanism, saslErr := admin.SASLNameToMechanism(s.saslMechanism)
		if saslErr != nil {
			err = multierror.Append(err, saslErr)
		}

		if saslMechanism == admin.SASLMechanismAWSMSKIAM &&
			(s.saslUsername != "" || s.saslPassword != "") {
			log.Warn("Username and password are ignored if using SASL AWS-MSK-IAM")
		}
	}

	return err
}

func (s sharedOptions) getAdminClient(
	ctx context.Context,
	sess *session.Session,
	readOnly bool,
) (admin.Client, error) {
	if s.clusterConfig != "" {
		clusterConfig, err := config.LoadClusterFile(s.clusterConfig, s.expandEnv)
		if err != nil {
			return nil, err
		}
		return clusterConfig.NewAdminClient(
			ctx,
			sess,
			readOnly,
			s.saslUsername,
			s.saslPassword,
		)
	} else if s.brokerAddr != "" {
		tlsEnabled := (s.tlsEnabled ||
			s.tlsCACert != "" ||
			s.tlsCert != "" ||
			s.tlsKey != "")
		saslEnabled := (s.saslMechanism != "" ||
			s.saslPassword != "" ||
			s.saslUsername != "")

		var saslMechanism admin.SASLMechanism
		var err error

		if s.saslMechanism != "" {
			saslMechanism, err = admin.SASLNameToMechanism(s.saslMechanism)
			if err != nil {
				return nil, err
			}
		}

		return admin.NewBrokerAdminClient(
			ctx,
			admin.BrokerAdminClientConfig{
				ConnectorConfig: admin.ConnectorConfig{
					BrokerAddr: s.brokerAddr,
					TLS: admin.TLSConfig{
						Enabled:    tlsEnabled,
						CACertPath: s.tlsCACert,
						CertPath:   s.tlsCert,
						KeyPath:    s.tlsKey,
						ServerName: s.tlsServerName,
						SkipVerify: s.tlsSkipVerify,
					},
					SASL: admin.SASLConfig{
						Enabled:   saslEnabled,
						Mechanism: saslMechanism,
						Password:  s.saslPassword,
						Username:  s.saslUsername,
					},
				},
				ReadOnly: readOnly,
			},
		)
	} else {
		return admin.NewZKAdminClient(
			ctx,
			admin.ZKAdminClientConfig{
				ZKAddrs:  []string{s.zkAddr},
				ZKPrefix: s.zkPrefix,
				Sess:     sess,
				ReadOnly: readOnly,
			},
		)
	}
}

func addSharedFlags(cmd *cobra.Command, options *sharedOptions) {
	cmd.Flags().StringVarP(
		&options.brokerAddr,
		"broker-addr",
		"b",
		"",
		"Broker address",
	)
	cmd.Flags().BoolVarP(
		&options.expandEnv,
		"expand-env",
		"",
		false,
		"Expand environment in cluster config",
	)
	cmd.Flags().StringVar(
		&options.clusterConfig,
		"cluster-config",
		os.Getenv("TOPICCTL_CLUSTER_CONFIG"),
		"Cluster config",
	)
	cmd.Flags().StringVar(
		&options.saslMechanism,
		"sasl-mechanism",
		"",
		"SASL mechanism if using SASL (choices: AWS-MSK-IAM, PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512)",
	)
	cmd.Flags().StringVar(
		&options.saslPassword,
		"sasl-password",
		os.Getenv("TOPICCTL_SASL_PASSWORD"),
		"SASL password if using SASL; will override value set in cluster config",
	)
	cmd.Flags().StringVar(
		&options.saslUsername,
		"sasl-username",
		os.Getenv("TOPICCTL_SASL_USERNAME"),
		"SASL username if using SASL; will override value set in cluster config",
	)
	cmd.Flags().StringVar(
		&options.tlsCACert,
		"tls-ca-cert",
		"",
		"Path to client CA cert PEM file if using TLS",
	)
	cmd.Flags().StringVar(
		&options.tlsCert,
		"tls-cert",
		"",
		"Path to client cert PEM file if using TLS",
	)
	cmd.Flags().BoolVar(
		&options.tlsEnabled,
		"tls-enabled",
		false,
		"Use TLS for communication with brokers",
	)
	cmd.Flags().StringVar(
		&options.tlsKey,
		"tls-key",
		"",
		"Path to client private key PEM file if using TLS",
	)
	cmd.Flags().StringVar(
		&options.tlsServerName,
		"tls-server-name",
		"",
		"Server name to use for TLS cert verification",
	)
	cmd.Flags().BoolVar(
		&options.tlsSkipVerify,
		"tls-skip-verify",
		false,
		"Skip hostname verification when using TLS",
	)
	cmd.Flags().StringVarP(
		&options.zkAddr,
		"zk-addr",
		"z",
		"",
		"ZooKeeper address",
	)
	cmd.Flags().StringVar(
		&options.zkPrefix,
		"zk-prefix",
		"",
		"Prefix for cluster-related nodes in zk",
	)
}

func addSharedConfigOnlyFlags(cmd *cobra.Command, options *sharedOptions) {
	cmd.Flags().StringVar(
		&options.clusterConfig,
		"cluster-config",
		os.Getenv("TOPICCTL_CLUSTER_CONFIG"),
		"Cluster config",
	)
	cmd.Flags().BoolVarP(
		&options.expandEnv,
		"expand-env",
		"",
		false,
		"Expand environment in cluster config",
	)
	cmd.Flags().StringVar(
		&options.saslPassword,
		"sasl-password",
		os.Getenv("TOPICCTL_SASL_PASSWORD"),
		"SASL password if using SASL; will override value set in cluster config",
	)
	cmd.Flags().StringVar(
		&options.saslUsername,
		"sasl-username",
		os.Getenv("TOPICCTL_SASL_USERNAME"),
		"SASL username if using SASL; will override value set in cluster config",
	)
}
