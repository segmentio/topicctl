package config

import (
	"errors"

	"github.com/hashicorp/go-multierror"
	"github.com/segmentio/kafka-go"
)

type ACLConfig struct {
	Meta ResourceMeta `json:"meta"`
	Spec ACLSpec      `json:"spec"`
}

type ACLSpec struct {
	ACLs []ACL `json:"acls"`
}

type ACL struct {
	Resource   ACLResource              `json:"resource"`
	Operations []kafka.ACLOperationType `json:"operations"`
}

type ACLResource struct {
	Type        kafka.ResourceType      `json:"type"`
	Name        string                  `json:"name"`
	PatternType kafka.PatternType       `json:"patternType"`
	Principal   string                  `json:"principal"`
	Host        string                  `json:"host"`
	Permission  kafka.ACLPermissionType `json:"permission"`
}

func (a ACLConfig) ToNewACLEntries() []kafka.ACLEntry {
	acls := []kafka.ACLEntry{}

	for _, acl := range a.Spec.ACLs {
		for _, operation := range acl.Operations {
			acls = append(acls, kafka.ACLEntry{
				ResourceType:        acl.Resource.Type,
				ResourceName:        acl.Resource.Name,
				ResourcePatternType: acl.Resource.PatternType,
				Principal:           acl.Resource.Principal,
				Host:                acl.Resource.Host,
				Operation:           operation,
				PermissionType:      acl.Resource.Permission,
			})
		}
	}
	return acls
}

// SetDeaults sets the default host and permission for each ACL in an ACL config
// if these aren't set
func (a *ACLConfig) SetDefaults() {
	for i, acl := range a.Spec.ACLs {
		if acl.Resource.Host == "" {
			a.Spec.ACLs[i].Resource.Host = "*"
		}
		if acl.Resource.Permission == kafka.ACLPermissionTypeUnknown {
			a.Spec.ACLs[i].Resource.Permission = kafka.ACLPermissionTypeAllow
		}
	}
}

// Validate evaluates whether the ACL config is valid.
func (a *ACLConfig) Validate() error {
	var err error

	err = a.Meta.Validate()

	for _, acl := range a.Spec.ACLs {
		if acl.Resource.Type == kafka.ResourceTypeUnknown {
			err = multierror.Append(err, errors.New("ACL resource type cannot be unknown"))
		}
		if acl.Resource.Name == "" {
			err = multierror.Append(err, errors.New("ACL resource name cannot be empty"))
		}
		if acl.Resource.PatternType == kafka.PatternTypeUnknown {
			err = multierror.Append(err, errors.New("ACL resource pattern type cannot be unknown"))
		}
		if acl.Resource.Principal == "" {
			err = multierror.Append(err, errors.New("ACL resource principal cannot be empty"))
		}

		for _, operation := range acl.Operations {
			if operation == kafka.ACLOperationTypeUnknown {
				err = multierror.Append(err, errors.New("ACL operation cannot be unknown"))
			}
		}
	}

	return err
}
