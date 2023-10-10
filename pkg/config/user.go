package config

import (
	"crypto/rand"
	"crypto/sha512"
	"errors"
	"fmt"

	"github.com/hashicorp/go-multierror"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/xdg-go/pbkdf2"
)

type UserConfig struct {
	Meta UserMeta `json:"meta"`
	Spec UserSpec `json:"spec"`
}

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
	Type AuthenticationType `json:"type"`
	// TODO: extend this to a type that supports SSMRef
	Password string `json:"password"`
}

type AuthenticationType string

const (
	ScramSha512 AuthenticationType = "scram-sha-512"
)

var allAuthenticationTypes = []AuthenticationType{
	ScramSha512,
}

type AuthorizationConfig struct {
	Type AuthorizationType `json:"type"`
	ACLs []ACL             `json:"acls,omitempty"`
}

type AuthorizationType string

const (
	SimpleAuthorization AuthorizationType = "simple"
)

var allAuthorizationTypes = []AuthorizationType{
	SimpleAuthorization,
}

type ACL struct {
	Resource   ACLResource `json:"resource"`
	Operations []string    `json:"operations"`
}

// TODO: how should principal and permission type be handled?
// principal will always be the meta name and permission type will always be allowed
type ACLResource struct {
	Type        string `json:"type"`
	Name        string `json:"name"`
	PatternType string `json:"patternType"`
	Principal   string `json:"principal"`
	Host        string `json:"host"`
}

func keys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

var allResourceTypes = keys(admin.ResourceTypeMap)
var allPatternTypes = keys(admin.PatternTypeMap)
var allOperationTypes = keys(admin.AclOperationTypeMap)

func (u *UserConfig) SetDefaults() {
	if u.Spec.Authorization.Type == "" {
		u.Spec.Authorization.Type = SimpleAuthorization
	}
}

func (u *UserConfig) Validate() error {
	// TODO: validate password types
	var err error

	if u.Meta.Name == "" {
		err = multierror.Append(err, errors.New("Name must be set"))
	}
	if u.Meta.Cluster == "" {
		err = multierror.Append(err, errors.New("Cluster must be set"))
	}
	if u.Meta.Region == "" {
		err = multierror.Append(err, errors.New("Region must be set"))
	}
	if u.Meta.Environment == "" {
		err = multierror.Append(err, errors.New("Environment must be set"))
	}

	authenticationTypeFound := false
	for _, authenticationType := range allAuthenticationTypes {
		if authenticationType == u.Spec.Authentication.Type {
			authenticationTypeFound = true
		}
	}

	if !authenticationTypeFound {
		err = multierror.Append(
			err,
			fmt.Errorf("Authentication Type must be in %+v, got: %s", allAuthenticationTypes, u.Spec.Authentication.Type),
		)
	}

	authorizationTypeFound := false
	for _, authorizationType := range allAuthorizationTypes {
		if authorizationType == u.Spec.Authorization.Type {
			authorizationTypeFound = true
		}
	}

	if !authorizationTypeFound {
		err = multierror.Append(
			err,
			fmt.Errorf("Authorization Type must be in %+v, got: %s", allAuthorizationTypes, u.Spec.Authorization.Type),
		)
	}

	for _, acl := range u.Spec.Authorization.ACLs {
		if _, ok := admin.ResourceTypeMap[acl.Resource.Type]; !ok {
			err = multierror.Append(
				err,
				fmt.Errorf("ACL Resource Type must be in %+v, got: %s", allResourceTypes, acl.Resource.Type),
			)
		}
		if _, ok := admin.PatternTypeMap[acl.Resource.PatternType]; !ok {
			err = multierror.Append(
				err,
				fmt.Errorf("ACL Resource PatternType must be in %+v, got: %s", allPatternTypes, acl.Resource.PatternType),
			)
		}
		for _, operation := range acl.Operations {
			if _, ok := admin.AclOperationTypeMap[operation]; !ok {
				err = multierror.Append(
					err,
					fmt.Errorf("ACL OperationType must be in %+v, got: %s", allOperationTypes, operation),
				)
			}
		}
	}

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

func (u UserConfig) ToNewACLEntries() []kafka.ACLEntry {
	acls := []kafka.ACLEntry{}

	for _, acl := range u.Spec.Authorization.ACLs {
		// Data has already been validated before calling this function so no need to check validity

		resourceType, _ := admin.ResourceTypeMap[acl.Resource.Type]
		resourcePatternType, _ := admin.PatternTypeMap[acl.Resource.PatternType]

		for _, operation := range acl.Operations {
			aclOperation, _ := admin.AclOperationTypeMap[operation]

			acls = append(acls, kafka.ACLEntry{
				ResourceType:        resourceType,
				ResourceName:        acl.Resource.Name,
				ResourcePatternType: resourcePatternType,
				Principal:           fmt.Sprintf("User:%s", u.Meta.Name),
				Host:                acl.Resource.Host,
				Operation:           aclOperation,
				PermissionType:      kafka.ACLPermissionTypeAllow,
			})
		}
	}
	return acls
}
