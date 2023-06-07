package util

import (
	"context"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"time"
)

// Rebalance topic progress Config
type RebalanceTopicProgressConfig struct {
	TopicName          string `json:"topic"`
	ClusterName        string `json:"cluster"`
	ClusterEnvironment string `json:"environment"`
	ToRemove           []int  `json:"to_remove"`
	RebalanceError     bool   `json:"rebalance_error"`
}

// Rebalance overall progress Config
type RebalanceProgressConfig struct {
	SuccessTopics      int    `json:"success_topics"`
	ErrorTopics        int    `json:"error_topics"`
	ClusterName        string `json:"cluster"`
	ClusterEnvironment string `json:"environment"`
	ToRemove           []int  `json:"to_remove"`
}

// Rebalance Topic Round progress Config
type RebalanceRoundProgressConfig struct {
	TopicName          string `json:"topic"`
	ClusterName        string `json:"cluster"`
	ClusterEnvironment string `json:"environment"`
	ToRemove           []int  `json:"to_remove"`
	CurrRound          int    `json:"round"`
	TotalRounds        int    `json:"total_rounds"`
}

// context map for rebalance
type RebalanceCtxMap struct {
	Enabled  bool          `json:"enabled"`
	Interval time.Duration `json:"interval"`
}

// shows progress of a config repeatedly during an interval
func ShowProgress(
	ctx context.Context,
	progressConfig interface{},
	interval time.Duration,
	stop chan bool,
) {
	progressStr, err := DictToStr(progressConfig)
	if err != nil {
		log.Errorf("Got error: %+v", err)
	} else {
		// print first before ticker starts
		log.Infof("Progress: %s", progressStr)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err == nil {
				log.Infof("Progress: %s", progressStr)
			}
		case <-stop:
			return
		}
	}
}

// convert any dict to json string
func DictToStr(dict interface{}) (string, error) {
	jsonBytes, err := json.Marshal(dict)
	if err != nil {
		return "{}", err
	}

	return string(jsonBytes), nil
}
