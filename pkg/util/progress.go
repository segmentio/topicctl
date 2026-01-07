package util

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
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

// Rebalance context struct
type RebalanceCtxStruct struct {
	Enabled  bool          `json:"enabled"`
	Interval time.Duration `json:"interval"`
}

// shows progress of a config repeatedly during an interval
func ShowProgress(
	ctx context.Context,
	progressConfig interface{},
	interval time.Duration,
	stopChan chan bool,
) {
	progressStr, err := StructToStr(progressConfig)
	if err != nil {
		log.Errorf("progress struct to string error: %+v", err)
	} else {
		// print first before ticker starts
		log.Infof("Rebalance Progress: %s", progressStr)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err == nil {
				log.Infof("Rebalance Progress: %s", progressStr)
			}
		case <-stopChan:
			return
		}
	}
}
