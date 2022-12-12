package admin

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	sigv4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	log "github.com/sirupsen/logrus"
)

// SASLMechanism is the name of a SASL mechanism that will be used for client authentication.
type SASLMechanism string

const (
	SASLMechanismAWSMSKIAM   SASLMechanism = "aws-msk-iam"
	SASLMechanismPlain       SASLMechanism = "plain"
	SASLMechanismScramSHA256 SASLMechanism = "scram-sha-256"
	SASLMechanismScramSHA512 SASLMechanism = "scram-sha-512"
)

// ConnectorConfig contains the configuration used to contruct a connector.
type ConnectorConfig struct {
	BrokerAddr string
	TLS        TLSConfig
	SASL       SASLConfig
}

// TLSConfig stores the TLS-related configuration for a connection.
type TLSConfig struct {
	Enabled    bool
	CertPath   string
	KeyPath    string
	CACertPath string
	ServerName string
	SkipVerify bool
}

// SASLConfig stores the SASL-related configuration for a connection.
type SASLConfig struct {
	Enabled   bool
	Mechanism SASLMechanism
	Username  string
	Password  string
}

// Connector is a wrapper around the low-level, kafka-go dialer and client.
type Connector struct {
	Config      ConnectorConfig
	Dialer      *kafka.Dialer
	KafkaClient *kafka.Client
}

// NewConnector contructs a new Connector instance given the argument config.
func NewConnector(config ConnectorConfig) (*Connector, error) {
	connector := &Connector{
		Config: config,
	}

	var mechanismClient sasl.Mechanism
	var tlsConfig *tls.Config
	var err error

	if config.SASL.Enabled {
		switch config.SASL.Mechanism {
		case SASLMechanismAWSMSKIAM:
			sess := session.Must(session.NewSession())
			signer := sigv4.NewSigner(sess.Config.Credentials)
			region := aws.StringValue(sess.Config.Region)

			mechanismClient = &aws_msk_iam.Mechanism{
				Signer: signer,
				Region: region,
			}
		case SASLMechanismPlain:
			mechanismClient = plain.Mechanism{
				Username: config.SASL.Username,
				Password: config.SASL.Password,
			}
		case SASLMechanismScramSHA256:
			mechanismClient, err = scram.Mechanism(
				scram.SHA256,
				config.SASL.Username,
				config.SASL.Password,
			)
			if err != nil {
				return nil, err
			}
		case SASLMechanismScramSHA512:
			mechanismClient, err = scram.Mechanism(
				scram.SHA512,
				config.SASL.Username,
				config.SASL.Password,
			)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("Unrecognized SASL mechanism: %s", config.SASL.Mechanism)
		}
	}

	if !config.TLS.Enabled {
		connector.Dialer = kafka.DefaultDialer
	} else {
		var certs []tls.Certificate
		var caCertPool *x509.CertPool

		if config.TLS.CertPath != "" && config.TLS.KeyPath != "" {
			log.Debugf(
				"Loading key pair from %s and %s",
				config.TLS.CertPath,
				config.TLS.KeyPath,
			)
			cert, err := tls.LoadX509KeyPair(config.TLS.CertPath, config.TLS.KeyPath)
			if err != nil {
				return nil, err
			}
			certs = append(certs, cert)
		}

		if config.TLS.CACertPath != "" {
			log.Debugf("Adding CA certs from %s", config.TLS.CACertPath)
			caCertPool = x509.NewCertPool()
			caCertContents, err := ioutil.ReadFile(config.TLS.CACertPath)
			if err != nil {
				return nil, err
			}
			if ok := caCertPool.AppendCertsFromPEM(caCertContents); !ok {
				return nil, fmt.Errorf(
					"Could not append CA certs from %s",
					config.TLS.CACertPath,
				)
			}
		}

		tlsConfig = &tls.Config{
			Certificates:       certs,
			RootCAs:            caCertPool,
			InsecureSkipVerify: config.TLS.SkipVerify,
			ServerName:         config.TLS.ServerName,
		}
		connector.Dialer = &kafka.Dialer{
			SASLMechanism: mechanismClient,
			Timeout:       10 * time.Second,
			TLS:           tlsConfig,
		}
	}

	log.Debugf("Connecting to cluster on address %s with TLS enabled=%v, SASL enabled=%v",
		config.BrokerAddr,
		config.TLS.Enabled,
		config.SASL.Enabled,
	)
	connector.KafkaClient = &kafka.Client{
		Addr: kafka.TCP(config.BrokerAddr),
		Transport: &kafka.Transport{
			Dial: connector.Dialer.DialFunc,
			SASL: mechanismClient,
			TLS:  tlsConfig,
			// Hotfix for https://github.com/segmentio/topicctl/issues/112
			MetadataTTL: 10 * time.Minute,
		},
	}

	return connector, nil
}

// SASLNameToMechanism converts the argument SASL mechanism name string to a valid instance of
// the SASLMechanism enum.
func SASLNameToMechanism(name string) (SASLMechanism, error) {
	normalizedName := strings.ReplaceAll(strings.ToLower(name), "_", "-")
	mechanism := SASLMechanism(normalizedName)

	switch mechanism {
	case SASLMechanismAWSMSKIAM,
		SASLMechanismPlain,
		SASLMechanismScramSHA256,
		SASLMechanismScramSHA512:
		return mechanism, nil
	default:
		return mechanism, fmt.Errorf(
			"SASL mechanism '%s' is not valid; choices are AWS-MSK-IAM, PLAIN, SCRAM-SHA-256, and SCRAM-SHA-512",
			mechanism,
		)
	}
}
