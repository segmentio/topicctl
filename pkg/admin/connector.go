package admin

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	log "github.com/sirupsen/logrus"
)

type ConnectorConfig struct {
	BrokerAddr string
	TLS        TLSConfig
	SASL       SASLConfig
}

type TLSConfig struct {
	Enabled    bool
	CertPath   string
	KeyPath    string
	CACertPath string
	ServerName string
	SkipVerify bool
}

type SASLConfig struct {
	Enabled   bool
	Mechanism string
	Username  string
	Password  string
}

type Connector struct {
	Config      ConnectorConfig
	Dialer      *kafka.Dialer
	KafkaClient *kafka.Client
}

func NewConnector(config ConnectorConfig) (*Connector, error) {
	connector := &Connector{
		Config: config,
	}

	var saslMechanism sasl.Mechanism
	var tlsConfig *tls.Config
	var err error

	if config.SASL.Enabled {
		switch strings.ToLower(config.SASL.Mechanism) {
		case "plain":
			saslMechanism = plain.Mechanism{
				Username: config.SASL.Username,
				Password: config.SASL.Password,
			}
		case "scram-sha-256":
			saslMechanism, err = scram.Mechanism(
				scram.SHA256,
				config.SASL.Username,
				config.SASL.Password,
			)
			if err != nil {
				return nil, err
			}
		case "scram-sha-512":
			saslMechanism, err = scram.Mechanism(
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
			SASLMechanism: saslMechanism,
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
			SASL: saslMechanism,
			TLS:  tlsConfig,
		},
	}

	return connector, nil
}

func ValidateSASLMechanism(mechanism string) error {
	switch strings.ToLower(mechanism) {
	case "plain", "scram-sha-256", "scram-sha-512":
		return nil
	default:
		return fmt.Errorf(
			"SASL mechanism '%s' is not valid; choices are PLAIN, SCRAM-SHA-256, and SCRAM-SHA-512",
			mechanism,
		)
	}
}
