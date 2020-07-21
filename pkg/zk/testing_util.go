package zk

import (
	"encoding/json"
	"testing"

	szk "github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

type PathTuple struct {
	Path string
	Obj  interface{}
}

func CreateNode(t *testing.T, zkConn *szk.Conn, path string, obj interface{}) {
	var data []byte
	var err error

	if obj != nil {
		data, err = json.Marshal(obj)
		require.Nil(t, err)
	}

	log.Infof("Creating path %+v", path)

	_, err = zkConn.Create(path, data, 0, szk.WorldACL(szk.PermAll))
	require.Nil(t, err)
}

func CreateNodes(t *testing.T, zkConn *szk.Conn, pathTuples []PathTuple) {
	for _, tuple := range pathTuples {
		CreateNode(t, zkConn, tuple.Path, tuple.Obj)
	}
}
