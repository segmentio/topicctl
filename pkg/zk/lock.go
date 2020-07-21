package zk

import (
	szk "github.com/samuel/go-zookeeper/zk"
)

// Lock is a lock interface that's satified by the samuel zk Lock struct.
type Lock interface {
	Unlock() error
}

var _ Lock = (*szk.Lock)(nil)
