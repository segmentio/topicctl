package config

import (
	"crypto/rand"
	"crypto/sha512"
	"fmt"

	"github.com/segmentio/kafka-go"
	"github.com/xdg-go/pbkdf2"
)

// TODO: most of this could be abstracted away and made available for any resource

type UserConfig struct {
	Meta UserMeta `json:"meta"`
	Spec UserSpec `json:"spec"`
}

// UserMeta stores the (mostly immutable) metadata associated with a topic.
// Inspired by the meta structs in Kubernetes objects.
type UserMeta struct {
	Name        string            `json:"name"`
	Cluster     string            `json:"cluster"`
	Region      string            `json:"region"`
	Environment string            `json:"environment"`
	Description string            `json:"description"`
	Labels      map[string]string `json:"labels"`
}

type UserSpec struct {
	Authentication AuthenticationConfig `json:"authentication"`
	Authorization  AuthorizationConfig  `json:"authorization,omitempty"`
}

type AuthenticationConfig struct {
	// TODO: extend this type to capture future types
	Type string `json:"type"`
	// TODO: extend this to a type that supports SSMRef
	Password string `json:"password"`
}

type AuthorizationConfig struct {
	Type AuthorizationType `json:"type"`
	ACLs []ACL             `json:"acls,omitempty"`
}

type AuthorizationType string

type ACL struct {
	Resource   ACLResource    `json:"resource"`
	Operations []ACLOperation `json:"operations"`
}

type ACLResource struct {
	Type        string `json:"type"`
	Name        string `json:"name"`
	PatternType string `json:"patternType"`
	Principal   string `json:"principal"`
	Host        string `json:"host"`
}

type ACLOperation string

const (
	SimpleAuthorization AuthorizationType = "simple"
)

// TODO: add validation
// maybe consider breaking out common functionality for Meta validation
func (u *UserConfig) Validate() error {
	// TODO: validation authenticationtype

	// TODO: validate all enums
	var err error

	return err
}

const (
	// Currently only scram-sha-512 is supported
	ScramMechanism kafka.ScramMechanism = kafka.ScramMechanismSha512
	// Use the same default as Postgres and Strimzi for Scram iterations
	ScramIterations int = 4096
)

func (u UserConfig) ToNewUserScramCredentialsUpsertion() (kafka.UserScramCredentialsUpsertion, error) {
	salt := make([]byte, 24)
	if _, err := rand.Read(salt); err != nil {
		return kafka.UserScramCredentialsUpsertion{}, fmt.Errorf("User %s: unable to generate salt: %v", u.Meta.Name, err)
	}
	saltedPassword := pbkdf2.Key([]byte(u.Spec.Authentication.Password), salt, ScramIterations, sha512.Size, sha512.New)

	return kafka.UserScramCredentialsUpsertion{
		Name:           u.Meta.Name,
		Mechanism:      ScramMechanism,
		Iterations:     ScramIterations,
		Salt:           salt,
		SaltedPassword: saltedPassword,
	}, nil
}
