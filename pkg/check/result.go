package check

type CheckName string

const (
	CheckNameConfigsConsistent        CheckName = "configs consistent"
	CheckNameConfigCorrect            CheckName = "config correct"
	CheckNameLeadersCorrect           CheckName = "leaders correct"
	CheckNamePartitionCountCorrect    CheckName = "partition count correct"
	CheckNameReplicasInSync           CheckName = "replicas in-sync"
	CheckNameReplicationFactorCorrect CheckName = "replication factor correct"
	CheckNameRetentionCorrect         CheckName = "retention correct"
	CheckNameThrottlesClear           CheckName = "throttles clear"
	CheckNameTopicExists              CheckName = "topic exists"
)

// TopicCheckResults stores the result of checking a single topic.
type TopicCheckResults struct {
	Results []TopicCheckResult
}

// TopicCheckResult contains the name and status of a single check.
type TopicCheckResult struct {
	Name        CheckName
	OK          bool
	Description string
}

// AllOK returns true if all subresults are OK, otherwise it returns false.
func (r *TopicCheckResults) AllOK() bool {
	for _, result := range r.Results {
		if !result.OK {
			return false
		}
	}

	return true
}

// AppendResult adds a new check result to the results.
func (r *TopicCheckResults) AppendResult(result TopicCheckResult) {
	r.Results = append(r.Results, result)
}

// UpdateLastResult updates the details of the most recently added result.
func (r *TopicCheckResults) UpdateLastResult(ok bool, description string) {
	r.Results[len(r.Results)-1].OK = ok
	r.Results[len(r.Results)-1].Description = description
}
