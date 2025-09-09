package create

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"errors"
	"fmt"
	"hash"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/topicctl/pkg/admin"
	"github.com/segmentio/topicctl/pkg/config"
	"github.com/segmentio/topicctl/pkg/util"
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/pbkdf2"
)

// UserCreatorConfig contains the configuration for a user creator.
type UserCreatorConfig struct {
	ClusterConfig config.ClusterConfig
	DryRun        bool
	SkipConfirm   bool
	UserConfig    config.UserConfig
}

// UserCreator handles the creation of SCRAM users in Kafka.
type UserCreator struct {
	config        UserCreatorConfig
	adminClient   admin.Client
	clusterConfig config.ClusterConfig
	userConfig    config.UserConfig
}

// NewUserCreator creates a new UserCreator instance.
func NewUserCreator(
	ctx context.Context,
	adminClient admin.Client,
	creatorConfig UserCreatorConfig,
) (*UserCreator, error) {
	// Check if the cluster supports user management
	supportedFeatures := adminClient.GetSupportedFeatures()
	if !supportedFeatures.Users {
		return nil, fmt.Errorf("User management is not supported by this cluster")
	}

	return &UserCreator{
		config:        creatorConfig,
		adminClient:   adminClient,
		clusterConfig: creatorConfig.ClusterConfig,
		userConfig:    creatorConfig.UserConfig,
	}, nil
}

// Create creates the SCRAM user in the Kafka cluster.
func (u *UserCreator) Create(ctx context.Context) error {
	log.Info("Validating configs...")

	if err := u.clusterConfig.Validate(); err != nil {
		return err
	}

	if err := u.userConfig.Validate(); err != nil {
		return err
	}

	if err := config.CheckConsistency(u.userConfig.Meta, u.clusterConfig); err != nil {
		return err
	}

	log.Infof("Checking if user '%s' already exists...", u.userConfig.Spec.Name)

	// Check if user already exists
	existingUsers, err := u.adminClient.GetUsers(ctx, []string{u.userConfig.Spec.Name})
	if err != nil {
		return fmt.Errorf("Error checking for existing user '%s': %v", u.userConfig.Spec.Name, err)
	}

	userExists := len(existingUsers) > 0
	if userExists {
		log.Infof("User '%s' already exists with mechanisms: %+v",
			u.userConfig.Spec.Name,
			existingUsers[0].CredentialInfos)

		// Check if the same mechanism already exists
		targetMechanism := u.userConfig.ToScramMechanism()
		for _, cred := range existingUsers[0].CredentialInfos {
			if kafka.ScramMechanism(cred.ScramMechanism) == targetMechanism {
				log.Infof("User '%s' already has SCRAM credentials for mechanism %s",
					u.userConfig.Spec.Name, u.userConfig.Spec.Mechanism)
				return nil
			}
		}
		log.Infof("User '%s' exists but doesn't have credentials for mechanism %s, will add them",
			u.userConfig.Spec.Name, u.userConfig.Spec.Mechanism)
	}

	if u.config.DryRun {
		if userExists {
			log.Infof("Would add SCRAM-%s credentials to existing user '%s'",
				u.userConfig.Spec.Mechanism, u.userConfig.Spec.Name)
		} else {
			log.Infof("Would create new user '%s' with SCRAM-%s credentials",
				u.userConfig.Spec.Name, u.userConfig.Spec.Mechanism)
		}
		return nil
	}

	// Generate SCRAM credentials
	scramCredentials, err := u.generateScramCredentials()
	if err != nil {
		return fmt.Errorf("Error generating SCRAM credentials: %v", err)
	}

	action := "create"
	if userExists {
		action = "update"
	}

	log.Infof("Ready to %s user '%s' with SCRAM-%s credentials (iterations: %d)",
		action, u.userConfig.Spec.Name, u.userConfig.Spec.Mechanism, u.userConfig.Spec.Iterations)

	ok, _ := util.Confirm("OK to continue?", u.config.SkipConfirm)
	if !ok {
		return errors.New("Stopping because of user response")
	}

	log.Infof("Creating/updating user '%s' with SCRAM-%s credentials...",
		u.userConfig.Spec.Name, u.userConfig.Spec.Mechanism)

	if err := u.adminClient.UpsertUser(ctx, *scramCredentials); err != nil {
		return fmt.Errorf("Error creating/updating user '%s': %v", u.userConfig.Spec.Name, err)
	}

	log.Infof("Successfully created/updated user '%s'", u.userConfig.Spec.Name)
	return nil
}

// generateScramCredentials creates the SCRAM credentials for the user.
func (u *UserCreator) generateScramCredentials() (*kafka.UserScramCredentialsUpsertion, error) {
	// Generate a random salt
	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("Failed to generate salt: %v", err)
	}

	// Get the SCRAM mechanism
	mechanism := u.userConfig.ToScramMechanism()

	// Generate salted password based on mechanism
	var saltedPassword []byte
	var err error

	switch mechanism {
	case kafka.ScramMechanismSha256:
		saltedPassword, err = generateSaltedPassword(
			u.userConfig.Spec.Password,
			salt,
			u.userConfig.Spec.Iterations,
			"sha256",
		)
	case kafka.ScramMechanismSha512:
		saltedPassword, err = generateSaltedPassword(
			u.userConfig.Spec.Password,
			salt,
			u.userConfig.Spec.Iterations,
			"sha512",
		)
	default:
		return nil, fmt.Errorf("Unsupported SCRAM mechanism: %v", mechanism)
	}

	if err != nil {
		return nil, fmt.Errorf("Failed to generate salted password: %v", err)
	}

	return &kafka.UserScramCredentialsUpsertion{
		Name:           u.userConfig.Spec.Name,
		Mechanism:      mechanism,
		Iterations:     u.userConfig.Spec.Iterations,
		Salt:           salt,
		SaltedPassword: saltedPassword,
	}, nil
}

// generateSaltedPassword generates a salted password using PBKDF2.
// This implements the SCRAM authentication mechanism that Kafka expects.
func generateSaltedPassword(password string, salt []byte, iterations int, hashAlg string) ([]byte, error) {
	var hashFunc func() hash.Hash
	var keyLen int

	switch hashAlg {
	case "sha256":
		hashFunc = sha256.New
		keyLen = 32 // SHA-256 output length
	case "sha512":
		hashFunc = sha512.New
		keyLen = 64 // SHA-512 output length
	default:
		return nil, fmt.Errorf("unsupported hash algorithm: %s", hashAlg)
	}

	// Generate salted password using PBKDF2
	saltedPassword := pbkdf2.Key([]byte(password), salt, iterations, keyLen, hashFunc)

	return saltedPassword, nil
}
