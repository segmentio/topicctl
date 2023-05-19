package util

import (
	"time"
	"encoding/json"
	log "github.com/sirupsen/logrus"
)

const (
	metricDuration = 5
)

// Rebalance Metric Config
type RebalanceMetricConfig struct {
	TopicName          string  `json:"topicName"`
	ClusterName        string  `json:"clusterName"`
	ClusterEnvironment string  `json:"clusterEnvironment"`
	ToRemove           []int   `json:"toRemove"`
	RebalanceStatus    string  `json:"rebalanceStatus"`
}

// Apply Metric Config
type ApplyMetricConfig struct {
	CurrRound          int     `json:"round"`
	TotalRounds        int     `json:"totalRounds"`
	TopicName          string  `json:"topicName"`
	ClusterName        string  `json:"clusterName"`
	ClusterEnvironment string  `json:"clusterEnvironment"`
	ToRemove           []int   `json:"toRemove"`
}

// Print Metrics for each iteration
// This can be configured but that will need lots of TopicConfig struct changes
// For now, emitting metrics every 5seconds. refer metricDuration
// Output log can be grepped and parsed for monitoring sake
func PrintMetrics(metricStr string, stop chan bool){
	// ticker waits for metricDuration. Hence we print first and then ticker to do its job
	log.Infof("Metric: %s", metricStr)

	ticker := time.NewTicker(metricDuration * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Infof("Metric: %s", metricStr)
		case <-stop:
			return
		}
	}
}

func MetricConfigStr(metricConfig interface{}) (string, error) {
	jsonBytes, err := json.Marshal(metricConfig)
	metricStr := "{}"
	if err != nil {
		return metricStr, err
	} else {
		metricStr = string(jsonBytes)
	}
	return metricStr, nil
}
