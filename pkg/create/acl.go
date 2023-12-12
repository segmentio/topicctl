package create

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/config"
	"github.com/segmentio/topicctl/pkg/util"
	log "github.com/sirupsen/logrus"
)

// ACLCreatorConfig contains the configuration for an ACL creator.
type ACLCreatorConfig struct {
	ClusterConfig config.ClusterConfig
	DryRun        bool
	SkipConfirm   bool
	ACLConfig     config.ACLConfig
}

type ACLCreator struct {
	config      ACLCreatorConfig
	adminClient admin.Client

	clusterConfig config.ClusterConfig
	aclConfig     config.ACLConfig
}

func NewACLCreator(
	ctx context.Context,
	adminClient admin.Client,
	creatorConfig ACLCreatorConfig,
) (*ACLCreator, error) {
	if !adminClient.GetSupportedFeatures().ACLs {
		return nil, fmt.Errorf("ACLs are not supported by this cluster")
	}

	return &ACLCreator{
		config:        creatorConfig,
		adminClient:   adminClient,
		clusterConfig: creatorConfig.ClusterConfig,
		aclConfig:     creatorConfig.ACLConfig,
	}, nil
}

func (a *ACLCreator) Create(ctx context.Context) error {
	log.Info("Validating configs...")

	if err := a.clusterConfig.Validate(); err != nil {
		return err
	}

	if err := a.aclConfig.Validate(); err != nil {
		return err
	}

	if err := config.CheckConsistency(a.aclConfig.Meta, a.clusterConfig); err != nil {
		return err
	}

	log.Info("Checking if ACLs already exist...")

	acls := a.aclConfig.ToNewACLEntries()

	allExistingACLs := []kafka.ACLEntry{}
	newACLs := []kafka.ACLEntry{}

	for _, acl := range acls {
		existingACLs, err := a.adminClient.GetACLs(ctx, kafka.ACLFilter{
			ResourceTypeFilter:        acl.ResourceType,
			ResourceNameFilter:        acl.ResourceName,
			ResourcePatternTypeFilter: acl.ResourcePatternType,
			PrincipalFilter:           acl.Principal,
			HostFilter:                acl.Host,
			Operation:                 acl.Operation,
			PermissionType:            acl.PermissionType,
		})
		if err != nil {
			return fmt.Errorf("error checking for existing ACL (%v): %v", acl, err)
		}
		if len(existingACLs) > 0 {
			allExistingACLs = append(allExistingACLs, acl)
		} else {
			newACLs = append(newACLs, acl)
		}
	}

	if len(allExistingACLs) > 0 {
		log.Infof(
			"Found %d existing ACLs:\n%s",
			len(allExistingACLs),
			formatNewACLsConfig(allExistingACLs),
		)
	}

	if len(newACLs) == 0 {
		log.Infof("No ACLs to create")
		return nil
	}

	if a.config.DryRun {
		log.Infof(
			"Would create ACLs with config %+v",
			formatNewACLsConfig(newACLs),
		)
		return nil
	}

	log.Infof(
		"It looks like these ACLs don't already exist. Will create them with this config:\n%s",
		formatNewACLsConfig(newACLs),
	)

	ok, _ := util.Confirm("OK to continue?", a.config.SkipConfirm)
	if !ok {
		return errors.New("Stopping because of user response")
	}

	log.Infof("Creating new ACLs for user with config %+v", formatNewACLsConfig(newACLs))

	if err := a.adminClient.CreateACLs(ctx, acls); err != nil {
		return fmt.Errorf("error creating new ACLs: %v", err)
	}

	return nil
}

// formatNewACLsConfig generates a pretty string representation of kafka-go
// ACL configurations.
func formatNewACLsConfig(config []kafka.ACLEntry) string {
	content, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		log.Warnf("Error marshalling ACLs config: %+v", err)
		return "Error"
	}

	return string(content)
}
