package admin

// SupportedFeatures provides a summary of what an admin client supports.
type SupportedFeatures struct {
	// Reads indicates whether the client supports reading basic cluster information
	// (metadata, configs, etc.).
	Reads bool

	// Applies indicates whether the client supports the functionality required for applying
	// (e.g., changing configs, electing leaders, etc.).
	Applies bool

	// Locks indicates whether the client supports locking.
	Locks bool

	// DynamicBrokerConfigs indicates whether the client can return dynamic broker configs
	// like leader.replication.throttled.rate.
	DynamicBrokerConfigs bool

	// ACLs indicates whether the client supports access control levels.
	ACLs bool

	// Users indicates whether the client supports SASL Users.
	Users bool
}
