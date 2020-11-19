package zk

import (
	"encoding/json"
	"testing"

	szk "github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// PathTuple is a <path, object> combination used for generating nodes in zk. For testing purposes
// only.
type PathTuple struct {
	Path string
	Obj  interface{}
}

// CreateNode creates a single node at the argument path. For testing purposes only.
func CreateNode(t *testing.T, zkConn *szk.Conn, path string, obj interface{}) {
	var data []byte
	var err error

	if obj != nil {
		data, err = json.Marshal(obj)
		require.NoError(t, err)
	}

	log.Infof("Creating path %+v", path)

	_, err = zkConn.Create(path, data, 0, szk.WorldACL(szk.PermAll))
	require.NoError(t, err)
}

// CreateNodes creates nodes according to the argument PathTuples. For testing purposes only.
func CreateNodes(t *testing.T, zkConn *szk.Conn, pathTuples []PathTuple) {
	for _, tuple := range pathTuples {
		CreateNode(t, zkConn, tuple.Path, tuple.Obj)
	}
}
