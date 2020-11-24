package admin

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/segmentio/kafka-go"
)

type ConnectorConfig struct {
	BrokerAddr string
	UseTLS     bool
	CertPath   string
	KeyPath    string
	CACertPath string
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

	if !config.UseTLS {
		connector.Dialer = kafka.DefaultDialer
	} else {
		cert, err := tls.LoadX509KeyPair(config.CertPath, config.KeyPath)
		if err != nil {
			return nil, err
		}

		caCertPool := x509.NewCertPool()
		caCertContents, err := ioutil.ReadFile(config.CACertPath)
		if err != nil {
			return nil, err
		}

		if ok := caCertPool.AppendCertsFromPEM(caCertContents); !ok {
			return nil, fmt.Errorf("Could not append CA certs from %s", config.CACertPath)
		}

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caCertPool,
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
		},
	}

	return connector, nil
}
