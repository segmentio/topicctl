package acl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/config"
	"github.com/segmentio/topicctl/pkg/util"
	log "github.com/sirupsen/logrus"
)

// ACLCreatorConfig contains the configuration for an ACL admin.
type ACLAdminConfig struct {
	ClusterConfig config.ClusterConfig
	DryRun        bool
	SkipConfirm   bool
	ACLConfig     config.ACLConfig
}

// ACLAdmin executes operations on ACLs by comparing the current ACLs with the desired ACLs.
type ACLAdmin struct {
	config      ACLAdminConfig
	adminClient admin.Client

	clusterConfig config.ClusterConfig
	aclConfig     config.ACLConfig
}

func NewACLAdmin(
	ctx context.Context,
	adminClient admin.Client,
	aclAdminConfig ACLAdminConfig,
) (*ACLAdmin, error) {
	if !adminClient.GetSupportedFeatures().ACLs {
		return nil, fmt.Errorf("ACLs are not supported by this cluster")
	}

	return &ACLAdmin{
		config:        aclAdminConfig,
		adminClient:   adminClient,
		clusterConfig: aclAdminConfig.ClusterConfig,
		aclConfig:     aclAdminConfig.ACLConfig,
	}, nil
}

// Create creates ACLs that do not already exist based on the ACL config.
func (a *ACLAdmin) Create(ctx context.Context) error {
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

	log.Info("Checking if ACLs already exists...")

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
		"It looks like these ACLs doesn't already exists. Will create them with this config:\n%s",
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

// Delete checks if ACLs exist and deletes them if they do.
func (a *ACLAdmin) Delete(ctx context.Context, filter kafka.DeleteACLsFilter) error {
	log.Infof("Checking if ACLs exists for filter:\n%+v", formatACLs(filter))

	getFilter := kafka.ACLFilter{
		ResourceTypeFilter:        filter.ResourceTypeFilter,
		ResourceNameFilter:        filter.ResourceNameFilter,
		ResourcePatternTypeFilter: filter.ResourcePatternTypeFilter,
		PrincipalFilter:           filter.PrincipalFilter,
		HostFilter:                filter.HostFilter,
		Operation:                 filter.Operation,
		PermissionType:            filter.PermissionType,
	}
	clusterACLs, err := a.adminClient.GetACLs(ctx, getFilter)

	if err != nil {
		return fmt.Errorf("Error fetching ACL info: \n%+v", err)
	}

	if len(clusterACLs) == 0 {
		return fmt.Errorf("No ACL matches filter:\n%+v", formatACLs(filter))
	}

	log.Infof("The following ACLs in the cluster are planned for deletion:\n%+v", formatACLInfos(clusterACLs))

	if a.config.DryRun {
		log.Infof("Would delete ACLs:\n%+v", formatACLInfos(clusterACLs))
		return nil
	}

	// This isn't settable by the CLI for safety measures but allows for testability
	confirm, err := util.Confirm("Delete ACLs?", a.config.SkipConfirm)
	if err != nil {
		return err
	}

	if !confirm {
		return errors.New("Stopping because of user response")
	}

	resp, err := a.adminClient.DeleteACLs(ctx, []kafka.DeleteACLsFilter{filter})

	if err != nil {
		return err
	}

	var respErrors = []error{}
	var deletedACLs = []kafka.DeleteACLsMatchingACLs{}

	for _, result := range resp.Results {
		if result.Error != nil {
			respErrors = append(respErrors, result.Error)
		}
		for _, matchingACL := range result.MatchingACLs {
			if matchingACL.Error != nil {
				respErrors = append(respErrors, result.Error)
			}
			deletedACLs = append(deletedACLs, matchingACL)
		}
	}

	if len(respErrors) > 0 {
		return fmt.Errorf("Got errors while deleting ACLs: \n%+v", respErrors)
	}

	log.Infof("ACLs successfully deleted: %+v", formatACLs(deletedACLs))

	return nil
}

func formatACLs(acls interface{}) string {
	content, err := json.MarshalIndent(acls, "", "  ")
	if err != nil {
		log.Warnf("Error marshalling acls: %+v", err)
		return "Error"
	}

	return string(content)
}

func formatACLInfos(acls []admin.ACLInfo) string {
	aclsString := []string{}

	for _, acl := range acls {
		aclsString = append(aclsString, admin.FormatACLInfo(acl))
	}

	return strings.Join(aclsString, "\n")
}
