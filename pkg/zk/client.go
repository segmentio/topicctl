package zk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	szk "github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
)

// Client exposes some common, zk operations. Unlike the underlying
// samuel zk client, it allows passing a context into most calls.
type Client interface {
	// Read-only operations
	Get(ctx context.Context, path string) ([]byte, *szk.Stat, error)
	GetJSON(ctx context.Context, path string, obj interface{}) (*szk.Stat, error)
	Children(ctx context.Context, path string) ([]string, *szk.Stat, error)
	Exists(
		ctx context.Context,
		path string,
	) (bool, *szk.Stat, error)

	// Write operations
	Create(ctx context.Context, path string, data []byte, sequential bool) error
	CreateJSON(ctx context.Context, path string, obj interface{}, sequential bool) error
	Set(
		ctx context.Context,
		path string,
		data []byte,
		version int32,
	) (*szk.Stat, error)
	SetJSON(
		ctx context.Context,
		path string,
		obj interface{},
		version int32,
	) (*szk.Stat, error)

	// Lock operations
	AcquireLock(ctx context.Context, path string) (Lock, error)

	Close() error
}

var _ Client = (*PooledClient)(nil)

type pooledRequest struct {
	path     string
	method   string
	respChan chan pooledResp
}

type pooledResp struct {
	content  []byte
	exists   bool
	children []string
	stats    *szk.Stat
	err      error
}

// PooledClient is a Client implementation that uses a pool of connections
// instead of a single one for read-only operations. It can be subtantially faster than the base
// samuel client, particularly when getting zookeeper nodes from multiple goroutines.
type PooledClient struct {
	connections []*szk.Conn
	requestChan chan pooledRequest
	readOnly    bool
}

// NewPooledClient returns a new PooledClient instance.
func NewPooledClient(
	zkAddrs []string,
	timeout time.Duration,
	logger szk.Logger,
	poolSize int,
	readOnly bool,
) (*PooledClient, error) {
	connections := []*szk.Conn{}
	log.Debugf("Creating zk client with addresses %+v", zkAddrs)

	for i := 0; i < poolSize; i++ {
		conn, _, err := szk.Connect(
			zkAddrs,
			time.Minute,
			szk.WithLogger(logger),
		)
		if err != nil {
			return nil, fmt.Errorf("Error connecting to zkAddr %+v: %+v", zkAddrs, err)
		}

		connections = append(
			connections,
			conn,
		)
	}

	requestChan := make(chan pooledRequest)

	for i := 0; i < poolSize; i++ {
		go func(index int, conn *szk.Conn) {
			log.Debugf("Starting connection %d", index)

			for {
				request, ok := <-requestChan
				if !ok {
					return
				}

				resp := pooledResp{}

				switch request.method {
				case "get":
					resp.content, resp.stats, resp.err = conn.Get(request.path)
				case "children":
					resp.children, resp.stats, resp.err = conn.Children(request.path)
				case "exists":
					resp.exists, resp.stats, resp.err = conn.Exists(request.path)
				default:
					resp.err = fmt.Errorf("Unrecognized method: %s", request.method)
				}

				request.respChan <- resp
			}
		}(i, connections[i])
	}

	return &PooledClient{
		connections: connections,
		requestChan: requestChan,
		readOnly:    readOnly,
	}, nil
}

// Get returns the value at the argument zk path.
func (c *PooledClient) Get(
	ctx context.Context,
	path string,
) ([]byte, *szk.Stat, error) {
	respChan := make(chan pooledResp)
	log.Debugf("Getting path %s", path)

	c.requestChan <- pooledRequest{
		path:     path,
		method:   "get",
		respChan: respChan,
	}

	select {
	case resp := <-respChan:
		return resp.content, resp.stats, resp.err
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

// GetJSON unmarshals the JSON content at the argument zk path into an object.
func (c *PooledClient) GetJSON(
	ctx context.Context,
	path string,
	obj interface{},
) (*szk.Stat, error) {
	data, stats, err := c.Get(ctx, path)
	if err != nil {
		return stats, err
	}

	err = json.Unmarshal(data, obj)
	return stats, err
}

// Children gets all children of the node at the argument zk path.
func (c *PooledClient) Children(
	ctx context.Context,
	path string,
) ([]string, *szk.Stat, error) {
	respChan := make(chan pooledResp)
	log.Debugf("Getting children at %s", path)

	c.requestChan <- pooledRequest{
		path:     path,
		method:   "children",
		respChan: respChan,
	}

	select {
	case resp := <-respChan:
		return resp.children, resp.stats, resp.err
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

// Exists returns whether a node exists at the argument zk path.
func (c *PooledClient) Exists(
	ctx context.Context,
	path string,
) (bool, *szk.Stat, error) {
	respChan := make(chan pooledResp)

	c.requestChan <- pooledRequest{
		path:     path,
		method:   "exists",
		respChan: respChan,
	}

	select {
	case resp := <-respChan:
		return resp.exists, resp.stats, resp.err
	case <-ctx.Done():
		return false, nil, ctx.Err()
	}
}

// Create adds a new node at the argument zk path.
func (c *PooledClient) Create(
	ctx context.Context,
	path string,
	data []byte,
	sequential bool,
) error {
	if c.readOnly {
		return errors.New("Cannot write in read-only mode")
	}

	errChan := make(chan error)

	go func() {
		var err error

		if sequential {
			_, err = c.connections[0].Create(
				path,
				data,
				szk.FlagSequence,
				szk.WorldACL(szk.PermAll),
			)
		} else {
			_, err = c.connections[0].Create(
				path,
				data,
				0,
				szk.WorldACL(szk.PermAll),
			)
		}

		errChan <- err
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errChan:
		return err
	}
}

// CreateJSON creates a new node at the argument zk path using the JSON-marshalled contents of
// the argument object.
func (c *PooledClient) CreateJSON(
	ctx context.Context,
	path string,
	obj interface{},
	sequential bool,
) error {
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	return c.Create(ctx, path, data, sequential)
}

type setResp struct {
	stats *szk.Stat
	err   error
}

// Set updates the contents of the node at the argument zk path.
func (c *PooledClient) Set(
	ctx context.Context,
	path string,
	data []byte,
	version int32,
) (*szk.Stat, error) {
	if c.readOnly {
		return nil, errors.New("Cannot write in read-only mode")
	}

	resultsChan := make(chan setResp)

	go func() {
		stats, err := c.connections[0].Set(path, data, version)
		resultsChan <- setResp{stats, err}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-resultsChan:
		return result.stats, result.err
	}
}

// SetJSON updates the contents of the node at the argument zk path using the JSON marshalling
// of the argument object.
func (c *PooledClient) SetJSON(
	ctx context.Context,
	path string,
	obj interface{},
	version int32,
) (*szk.Stat, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}

	return c.Set(ctx, path, data, version)
}

// AcquireLock tries to acquire a lock using the argument zk path.
func (c *PooledClient) AcquireLock(ctx context.Context, path string) (Lock, error) {
	if c.readOnly {
		return nil, errors.New("Cannot create lock in read-only mode")
	}

	lock := szk.NewLock(c.connections[0], path, szk.WorldACL(szk.PermAll))
	errChan := make(chan error)

	go func() {
		errChan <- lock.Lock()
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-errChan:
		return lock, err
	}
}

// Close closes the current client and frees the associated resources.
func (c *PooledClient) Close() error {
	close(c.requestChan)

	closeChan := make(chan struct{}, len(c.connections))

	for index, conn := range c.connections {
		log.Debugf("Closing zk connection %d/%d", index+1, len(c.connections))

		go func(innerConn *szk.Conn) {
			innerConn.Close()
			closeChan <- struct{}{}
		}(conn)
	}

	for i := 0; i < len(c.connections); i++ {
		<-closeChan
	}

	return nil
}
