package admin

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

type ConnectorConfig struct {
	BrokerAddr string
	UseTLS     bool
	CertPath   string
	KeyPath    string
	CACertPath string
	ServerName string
	SkipVerify bool
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

	var tlsConfig *tls.Config

	if !config.UseTLS {
		connector.Dialer = kafka.DefaultDialer
	} else {
		log.Debugf("Loading key pair from %s and %s", config.CertPath, config.KeyPath)
		cert, err := tls.LoadX509KeyPair(config.CertPath, config.KeyPath)
		if err != nil {
			return nil, err
		}

		log.Debugf("Adding CA certs from %s", config.CACertPath)
		caCertPool := x509.NewCertPool()
		caCertContents, err := ioutil.ReadFile(config.CACertPath)
		if err != nil {
			return nil, err
		}

		if ok := caCertPool.AppendCertsFromPEM(caCertContents); !ok {
			return nil, fmt.Errorf("Could not append CA certs from %s", config.CACertPath)
		}

		tlsConfig = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
			InsecureSkipVerify: config.SkipVerify,
			ServerName:         config.ServerName,
		}
		connector.Dialer = &kafka.Dialer{
			Timeout: 10 * time.Second,
			TLS:     tlsConfig,
		}
	}

	connector.KafkaClient = &kafka.Client{
		Addr: kafka.TCP(config.BrokerAddr),
		Transport: &kafka.Transport{
			Dial: connector.Dialer.DialFunc,
			TLS:  tlsConfig,
		},
	}

	return connector, nil
}
