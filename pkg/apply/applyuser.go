package apply

import (
	"context"
	"errors"

	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/config"
	log "github.com/sirupsen/logrus"
)

// TODO: dry this up with the apply.go file

// TODO: are these structs even necessary?
type UserApplierConfig struct {
	DryRun        bool
	SkipConfirm   bool
	UserConfig    config.UserConfig
	ClusterConfig config.ClusterConfig
}

type UserApplier struct {
	config      UserApplierConfig
	adminClient admin.Client

	clusterConfig config.ClusterConfig
	userConfig    config.UserConfig
	userName      string
}

func NewUserApplier(
	ctx context.Context,
	adminClient admin.Client,
	applierConfig UserApplierConfig,
) (*UserApplier, error) {
	if !adminClient.GetSupportedFeatures().Applies {
		return nil,
			errors.New(
				"Admin client does not support features needed for apply; please us zk-based client instead.",
			)
	}

	return &UserApplier{
		config:        applierConfig,
		adminClient:   adminClient,
		clusterConfig: applierConfig.ClusterConfig,
		userConfig:    applierConfig.UserConfig,
		userName:      applierConfig.UserConfig.Meta.Name,
	}, nil
}

func (u *UserApplier) Apply(ctx context.Context) error {
	log.Info("Validating configs...")

	if err := u.clusterConfig.Validate(); err != nil {
		return err
	}

	if err := u.userConfig.Validate(); err != nil {
		return err
	}

	log.Info("Checking if user already exists...")

	userInfo, err := u.adminClient.GetUsers(ctx, []string{u.userName})
	if err != nil {
		return err
	}

	if len(userInfo) == 0 {
		return u.applyNewUser(ctx)
	}
	// TODO: handle case where this returns multiple users due to multiple creds being created

	return u.applyExistingUser(ctx, userInfo[0])
}

func (u *UserApplier) applyNewUser(ctx context.Context) error {
	user, err := u.userConfig.ToNewUserScramCredentialsUpsertion()
	if err != nil {
		return err
	}

	if u.config.DryRun {
		log.Infof("Would create user with config %+v", user)
		return nil
	}
	return u.adminClient.CreateUser(ctx, user)
}

func (u *UserApplier) applyExistingUser(
	ctx context.Context,
	userInfo admin.UserInfo,
) error {
	log.Infof("Updating existing user '%s'", u.userName)
	return nil
}
