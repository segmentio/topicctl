package config

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
