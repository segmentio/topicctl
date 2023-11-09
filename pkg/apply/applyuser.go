package apply

// // TODO: dry this up with the apply.go file

// // TODO: are these structs even necessary?
// type UserApplierConfig struct {
// 	DryRun        bool
// 	SkipConfirm   bool
// 	UserConfig    config.UserConfig
// 	ClusterConfig config.ClusterConfig
// }

// type UserApplier struct {
// 	config      UserApplierConfig
// 	adminClient admin.Client

// 	clusterConfig config.ClusterConfig
// 	userConfig    config.UserConfig
// }

// func NewUserApplier(
// 	ctx context.Context,
// 	adminClient admin.Client,
// 	applierConfig UserApplierConfig,
// ) (*UserApplier, error) {
// 	if !adminClient.GetSupportedFeatures().Applies {
// 		return nil,
// 			errors.New(
// 				"Admin client does not support features needed for apply; You need to upgrade to Kafka version >2.7.0.",
// 			)
// 	}

// 	return &UserApplier{
// 		config:        applierConfig,
// 		adminClient:   adminClient,
// 		clusterConfig: applierConfig.ClusterConfig,
// 		userConfig:    applierConfig.UserConfig,
// 	}, nil
// }

// func (u *UserApplier) Apply(ctx context.Context) error {
// 	log.Info("Validating configs...")

// 	if err := u.clusterConfig.Validate(); err != nil {
// 		return fmt.Errorf("error validating cluster config: %v", err)
// 	}

// 	if err := u.userConfig.Validate(); err != nil {
// 		return fmt.Errorf("error validating user config: %v", err)
// 	}

// 	log.Info("Checking if user already exists...")

// 	userInfo, err := u.adminClient.GetUsers(ctx, []string{u.userConfig.Meta.Name})
// 	if err != nil {
// 		return fmt.Errorf("error checking if user already exists: %v", err)
// 	}

// 	if len(userInfo) == 0 {
// 		err = u.applyNewUser(ctx)
// 	} else {
// 		// TODO: handle case where this returns multiple users due to multiple creds being created
// 		err = u.applyExistingUser(ctx, userInfo[0])
// 	}

// 	if err != nil {
// 		return fmt.Errorf("error applying existing user: %v", err)
// 	}

// 	log.Info("Checking if ACLs already exist for this user...")

// 	existingACLs, err := u.adminClient.GetACLs(ctx, kafka.ACLFilter{
// 		ResourceTypeFilter:        kafka.ResourceTypeAny,
// 		ResourcePatternTypeFilter: kafka.PatternTypeAny,
// 		PrincipalFilter:           fmt.Sprintf("User:%s", u.userConfig.Meta.Name),
// 		Operation:                 kafka.ACLOperationTypeAny,
// 		PermissionType:            kafka.ACLPermissionTypeAny,
// 	})

// 	if err != nil {
// 		return fmt.Errorf("error checking existing ACLs for user %s: %v", u.userConfig.Meta.Name, err)
// 	}

// 	// TODO: pretty print these ACLs
// 	log.Info("Found ", len(existingACLs), " existing ACLs: ", existingACLs)

// 	acls := u.userConfig.ToNewACLEntries()

// 	// Compare acls and existingACLEntries

// 	if len(acls) == 0 {
// 		return nil
// 	}

// 	if u.config.DryRun {
// 		log.Infof(
// 			"Would create ACLs with config %+v",
// 			FormatNewACLsConfig(acls),
// 		)
// 		return nil
// 	}

// 	log.Infof(
// 		"It looks like these ACLs doesn't already exists. Will create them with this config:\n%s",
// 		FormatNewACLsConfig(acls),
// 	)

// 	ok, _ := Confirm("OK to continue?", u.config.SkipConfirm)
// 	if !ok {
// 		return errors.New("Stopping because of user response")
// 	}

// 	log.Infof("Creating new ACLs for user with config %+v", acls)

// 	err = u.adminClient.CreateACLs(ctx, acls)
// 	if err != nil {
// 		return fmt.Errorf("error creating new ACLs: %v", err)
// 	}
// 	return nil
// }

// func (u *UserApplier) applyNewUser(ctx context.Context) error {
// 	user, err := u.userConfig.ToNewUserScramCredentialsUpsertion()
// 	if err != nil {
// 		return fmt.Errorf("error creating UserScramCredentialsUpsertion: %v", err)
// 	}

// 	if u.config.DryRun {
// 		log.Infof("Would create user with config %+v", user)
// 		return nil
// 	}

// 	log.Infof(
// 		"It looks like this user doesn't already exists. Will create it with this config:\n%s",
// 		FormatNewUserConfig(user),
// 	)

// 	ok, _ := Confirm("OK to continue?", u.config.SkipConfirm)
// 	if !ok {
// 		return errors.New("Stopping because of user response")
// 	}

// 	log.Infof("Creating new user with config %+v", user)

// 	err = u.adminClient.UpsertUser(ctx, user)
// 	if err != nil {
// 		return fmt.Errorf("error creating new user: %v", err)
// 	}

// 	return nil
// }

// // TODO: support this
// func (u *UserApplier) applyExistingUser(
// 	ctx context.Context,
// 	userInfo admin.UserInfo,
// ) error {
// 	log.Infof("Updating existing user: %s", u.userConfig.Meta.Name)
// 	return nil
// }
