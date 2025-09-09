package config

import (
	"errors"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/segmentio/kafka-go"
)

// UserConfig represents the configuration for a Kafka SCRAM user.
type UserConfig struct {
	Meta ResourceMeta `json:"meta"`
	Spec UserSpec     `json:"spec"`
}

// UserSpec contains the specification for a Kafka SCRAM user.
type UserSpec struct {
	// Name is the username for the SCRAM user
	Name string `json:"name"`

	// Mechanism is the SCRAM mechanism (scram-sha-256 or scram-sha-512)
	Mechanism string `json:"mechanism"`

	// Password is the plain-text password that will be used to generate SCRAM credentials
	Password string `json:"password"`

	// Iterations is the number of iterations to use for SCRAM credential generation.
	// If not specified, defaults to 4096.
	Iterations int `json:"iterations,omitempty"`
}

// SetDefaults sets default values for the user configuration.
func (u *UserConfig) SetDefaults() {
	if u.Spec.Iterations == 0 {
		u.Spec.Iterations = 4096
	}
	if u.Spec.Mechanism == "" {
		u.Spec.Mechanism = "scram-sha-256"
	}
}

// Validate evaluates whether the user config is valid.
func (u *UserConfig) Validate() error {
	var err error

	// Validate metadata
	if metaErr := u.Meta.Validate(); metaErr != nil {
		err = multierror.Append(err, metaErr)
	}

	// Validate user spec
	if u.Spec.Name == "" {
		err = multierror.Append(err, errors.New("User name cannot be empty"))
	}

	if u.Spec.Password == "" {
		err = multierror.Append(err, errors.New("User password cannot be empty"))
	}

	// Validate SCRAM mechanism
	mechanism := strings.ToLower(strings.ReplaceAll(u.Spec.Mechanism, "_", "-"))
	if mechanism != "scram-sha-256" && mechanism != "scram-sha-512" {
		err = multierror.Append(
			err,
			errors.New("User mechanism must be either 'scram-sha-256' or 'scram-sha-512'"),
		)
	}

	// Validate iterations
	if u.Spec.Iterations < 1000 {
		err = multierror.Append(
			err,
			errors.New("User iterations must be at least 1000 for security"),
		)
	}

	return err
}

// ToScramMechanism converts the mechanism string to kafka-go ScramMechanism.
func (u *UserConfig) ToScramMechanism() kafka.ScramMechanism {
	mechanism := strings.ToLower(strings.ReplaceAll(u.Spec.Mechanism, "_", "-"))
	switch mechanism {
	case "scram-sha-256":
		return kafka.ScramMechanismSha256
	case "scram-sha-512":
		return kafka.ScramMechanismSha512
	default:
		// Default to SHA-256 if unknown
		return kafka.ScramMechanismSha256
	}
}
