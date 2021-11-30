package zk

import (
	szk "github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
)

// DebugLogger is a logger that satisfies the szk.Logger interface.
type DebugLogger struct{}

var _ szk.Logger = (*DebugLogger)(nil)

// Printf sends samuel zk log messages to logrus at the debug level.
func (l *DebugLogger) Printf(format string, args ...interface{}) {
	log.Debugf(format, args...)
}
