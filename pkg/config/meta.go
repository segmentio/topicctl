package config

import (
	"errors"

	"github.com/hashicorp/go-multierror"
)

// ResourceMeta stores the (mostly immutable) metadata associated with a resource.
// Inspired by the meta structs in Kubernetes objects.
type ResourceMeta struct {
	Name        string            `json:"name"`
	Cluster     string            `json:"cluster"`
	Region      string            `json:"region"`
	Environment string            `json:"environment"`
	Description string            `json:"description"`
	Labels      map[string]string `json:"labels"`

	// Consumers is a list of consumers who are expected to consume from this
	// topic.
	Consumers []string `json:"consumers,omitempty"`
}

// Validate evalutes whether the ResourceMeta is valid.
func (rm *ResourceMeta) Validate() error {
	var err error
	if rm.Name == "" {
		err = multierror.Append(err, errors.New("Name must be set"))
	}
	if rm.Cluster == "" {
		err = multierror.Append(err, errors.New("Cluster must be set"))
	}
	if rm.Region == "" {
		err = multierror.Append(err, errors.New("Region must be set"))
	}
	if rm.Environment == "" {
		err = multierror.Append(err, errors.New("Environment must be set"))
	}
	return err
}
