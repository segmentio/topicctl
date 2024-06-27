package apply

// IntValueChanges stores changes in integer values (NumPartitions & ReplicationFactor)
// if a topic is being created then Updated == Current
type IntValueChanges struct {
	Current int `json:"current"`
	Updated int `json:"updated"`
}

// NewConfigEntry holds configs which are being added to a topic
type NewConfigEntry struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// ConfigEntryChanges holds configs to be updated, as well as their current and updated values
type ConfigEntryChanges struct {
	Name    string `json:"name"`
	Current string `json:"current"`
	Updated string `json:"updated"`
}

// ReplicaAssignmentChanges stores replica reassignment
// if a topic is being created then UpdatedReplicas == CurrentReplicas
// TODO: update Changes to actually support replica reassignment
type ReplicaAssignmentChanges struct {
	Partition       int   `json:"partition"`
	CurrentReplicas []int `json:"currentReplicas"`
	UpdatedReplicas []int `json:"updatedReplicas"`
}

// enum for possible Action values in ChangesTracker
type ActionEnum string

const (
	ActionEnumCreate ActionEnum = "create"
	ActionEnumUpdate ActionEnum = "update"
)

// NewChangesTracker stores the structure of a topic being created in an apply run
// to eventually be printed to stdout as a JSON blob in subcmd/apply.go
type NewChangesTracker struct {
	// Action records whether this is a topic being created or updated
	Action            ActionEnum        `json:"action"`
	Topic             string            `json:"topic"`
	NumPartitions     int               `json:"numPartitions"`
	ReplicationFactor int               `json:"replicationFactor"`
	ConfigEntries     *[]NewConfigEntry `json:"configEntries"`
}

// UpdateChangesTracker stores the same data as NewChangesTracker,
// but
// to eventually be printed to stdout as a JSON blob in subcmd/apply.go
type UpdateChangesTracker struct {
	// Action records whether this is a topic being created or updated
	Action ActionEnum `json:"action"`
	Topic  string     `json:"topic"`

	// tracks changes in partition count
	// TODO: implement this
	// NumPartitions *IntValueChanges `json:"numPartitions"`

	// tracks changes in replication factor
	// TODO: implement this
	// ReplicationFactor *IntValueChanges `json:"replicationFactor"`

	// ReplicaAssignments among kafka brokers for this topic partitions. If this
	// is set num_partitions and replication_factor must be unset.
	// TODO: implement this
	// ReplicaAssignments []*ReplicaAssignmentChanges `json:"replicaAssignments"`

	// tracks configs being added to the topic
	NewConfigEntries *[]NewConfigEntry `json:"newConfigEntries"`

	// tracks changes in existing config entries
	UpdatedConfigEntries *[]ConfigEntryChanges `json:"updatedConfigEntries"`

	// MissingKeys stores configs which are set in the cluster but not in the topicctl config
	MissingKeys []string `json:"missingKeys"`
	// Error stores if an error occurred during topic update
	Error bool `json:"error"`
}

// Union of NewChangesTracker and UpdateChangesTracker
// used as a return type for the Apply function (which forks into applyNewTopic or applyExistingTopic)
type NewOrUpdatedChanges struct {
	NewChanges    *NewChangesTracker
	UpdateChanges *UpdateChangesTracker
}
