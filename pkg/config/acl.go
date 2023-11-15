package config

import (
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

// TODO: Set defaults for missing values

// TODO: Validate fields cannot be empty
