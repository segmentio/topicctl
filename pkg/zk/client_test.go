package zk

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	szk "github.com/samuel/go-zookeeper/zk"
	"github.com/segmentio/topicctl/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testZkAddress string

func init() {
	var ok bool

	// Inside docker-compose (i.e., in CI), we need to use a different
	// address
	testZkAddress, ok = os.LookupEnv("KAFKA_TOPICS_TEST_ZK_ADDR")
	if !ok {
		testZkAddress = "localhost:2181"
	}
}

func TestPooledClientRead(t *testing.T) {
	zkConn, _, err := szk.Connect(
		[]string{testZkAddress},
		5*time.Second,
	)
	require.NoError(t, err)

	prefix := testPrefix("pooled-client-read")

	CreateNodes(
		t,
		zkConn,
		[]PathTuple{
			{
				Path: fmt.Sprintf("/%s", prefix),
				Obj:  nil,
			},
			{
				Path: fmt.Sprintf("/%s/parent1", prefix),
				Obj:  nil,
			},
			{
				Path: fmt.Sprintf("/%s/parent1/parent2", prefix),
				Obj:  nil,
			},
			{
				Path: fmt.Sprintf("/%s/parent1/parent2/child1", prefix),
				Obj:  "value1",
			},
			{
				Path: fmt.Sprintf("/%s/parent1/parent2/child1/subchild1", prefix),
				Obj:  nil,
			},
			{
				Path: fmt.Sprintf("/%s/parent1/parent2/child2", prefix),
				Obj:  "value2",
			},
			{
				Path: fmt.Sprintf("/%s/parent1/parent2/child2/subchild2", prefix),
				Obj:  nil,
			},
			{
				Path: fmt.Sprintf("/%s/parent1/parent2/child3", prefix),
				Obj:  "value3",
			},
			{
				Path: fmt.Sprintf("/%s/parent1/parent2/child3/subchild3", prefix),
				Obj:  nil,
			},
			{
				Path: fmt.Sprintf("/%s/parent1/parent2/child4", prefix),
				Obj:  "value4",
			},
			{
				Path: fmt.Sprintf("/%s/parent1/parent2/child4/subchild4", prefix),
				Obj:  nil,
			},
		},
	)

	pooledClient, err := NewPooledClient(
		[]string{testZkAddress},
		5*time.Second,
		&DebugLogger{},
		2,
		true,
	)
	defer pooledClient.Close()
	require.NoError(t, err)

	doneChan := make(chan struct{})

	ctx := context.Background()

	for i := 0; i < 4; i++ {
		go func(index int) {
			defer func() {
				doneChan <- struct{}{}
			}()

			getResult, _, err := pooledClient.Get(
				ctx,
				fmt.Sprintf("/%s/parent1/parent2/child%d", prefix, index+1),
			)
			require.NoError(t, err)
			assert.Equal(t, fmt.Sprintf(`"value%d"`, index+1), string(getResult))

			childrenResult, _, err := pooledClient.Children(
				ctx,
				fmt.Sprintf("/%s/parent1/parent2/child%d", prefix, index+1),
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				[]string{
					fmt.Sprintf("subchild%d", index+1),
				},
				childrenResult,
			)
		}(i)
	}

	timeout := time.NewTimer(10 * time.Second)

	for i := 0; i < 4; i++ {
		select {
		case <-doneChan:
		case <-timeout.C:
			require.FailNow(t, "Timed out waiting for results")
		}
	}

	exists, _, err := pooledClient.Exists(
		ctx,
		fmt.Sprintf("/%s/parent1/parent2/child1/subchild1", prefix),
	)
	assert.NoError(t, err)
	assert.True(t, exists)

	exists, _, err = pooledClient.Exists(
		ctx,
		fmt.Sprintf("/%s/parent1/parent2/child1/non-existent-path", prefix),
	)
	assert.NoError(t, err)
	assert.False(t, exists)

	// Writes not allowed
	err = pooledClient.Create(ctx, fmt.Sprintf("/%s/parent4", prefix), []byte("test"), true)
	assert.Error(t, err)
}

func TestPooledClientWrites(t *testing.T) {
	zkConn, _, err := szk.Connect(
		[]string{testZkAddress},
		5*time.Second,
	)
	require.NoError(t, err)

	prefix := testPrefix("pooled-client-write")

	CreateNodes(
		t,
		zkConn,
		[]PathTuple{
			{
				Path: fmt.Sprintf("/%s", prefix),
				Obj:  nil,
			},
		},
	)

	pooledClient, err := NewPooledClient(
		[]string{testZkAddress},
		5*time.Second,
		&DebugLogger{},
		2,
		false,
	)
	defer pooledClient.Close()
	require.NoError(t, err)

	ctx := context.Background()

	testPath := fmt.Sprintf("/%s/test1", prefix)

	err = pooledClient.Create(
		ctx,
		testPath,
		[]byte(`"hello"`),
		false,
	)
	require.NoError(t, err)

	var testStr string
	_, err = pooledClient.GetJSON(ctx, testPath, &testStr)
	assert.NoError(t, err)
	assert.Equal(t, "hello", testStr)

	stats, err := pooledClient.SetJSON(
		ctx,
		testPath,
		map[string]string{
			"key1": "value1",
		},
		0,
	)
	require.NoError(t, err)
	assert.Equal(t, int32(1), stats.Version)

	testObj := map[string]string{}
	_, err = pooledClient.GetJSON(ctx, testPath, &testObj)
	assert.NoError(t, err)
	assert.Equal(
		t,
		map[string]string{
			"key1": "value1",
		},
		testObj,
	)
}

func TestPooledClientSequentialWrites(t *testing.T) {
	zkConn, _, err := szk.Connect(
		[]string{testZkAddress},
		5*time.Second,
	)
	require.NoError(t, err)

	prefix := testPrefix("pooled-client-write-sequential")

	CreateNodes(
		t,
		zkConn,
		[]PathTuple{
			{
				Path: fmt.Sprintf("/%s", prefix),
				Obj:  nil,
			},
		},
	)

	pooledClient, err := NewPooledClient(
		[]string{testZkAddress},
		5*time.Second,
		&DebugLogger{},
		2,
		false,
	)
	defer pooledClient.Close()
	require.NoError(t, err)

	ctx := context.Background()
	testPath := fmt.Sprintf("/%s/test2_", prefix)

	for i := 0; i < 5; i++ {
		err = pooledClient.Create(
			ctx,
			testPath,
			[]byte(`"hello"`),
			true,
		)
		require.NoError(t, err)
	}

	children, _, err := pooledClient.Children(ctx, fmt.Sprintf("/%s", prefix))
	assert.NoError(t, err)
	assert.Equal(t, 5, len(children))
}

func TestPooledClientLocks(t *testing.T) {
	zkConn, _, err := szk.Connect(
		[]string{testZkAddress},
		5*time.Second,
	)
	require.NoError(t, err)

	prefix := testPrefix("pooled-client-locks")
	CreateNodes(
		t,
		zkConn,
		[]PathTuple{
			{
				Path: fmt.Sprintf("/%s", prefix),
				Obj:  nil,
			},
		},
	)

	pooledClient, err := NewPooledClient(
		[]string{testZkAddress},
		5*time.Second,
		&DebugLogger{},
		2,
		false,
	)
	defer pooledClient.Close()
	require.NoError(t, err)

	ctx := context.Background()

	lockPath := fmt.Sprintf("/%s/locks/test-lock", prefix)

	lock, err := pooledClient.AcquireLock(ctx, lockPath)
	require.NoError(t, err)
	require.NotNil(t, lock)

	children, _, err := pooledClient.Children(ctx, lockPath)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(children))

	require.Nil(t, lock.Unlock())

	children, _, err = pooledClient.Children(ctx, lockPath)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(children))
}

func testPrefix(name string) string {
	return util.RandomString(fmt.Sprintf("zk-test-%s-", name), 6)
}
