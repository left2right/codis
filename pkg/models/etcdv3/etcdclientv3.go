// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package etcdclientv3

import (
	"strings"
	"sync"
	"time"

	"context"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/clientv3util"
	"github.com/coreos/etcd/mvcc/mvccpb"

	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

var (
	ErrClosedClient = errors.New("use of closed etcd client")
	ErrTxnFailed    = errors.New("failed to commit transaction")
	ErrKeyNotExists = errors.New("etcd: key not exists")

	ErrNotDir  = errors.New("etcd: not a dir")
	ErrNotFile = errors.New("etcd: not a file")
)

type Client struct {
	sync.Mutex
	client *clientv3.Client

	closed  bool
	timeout time.Duration

	cancel  context.CancelFunc
	context context.Context
}

func New(addrlist string, auth string, timeout time.Duration) (*Client, error) {
	endpoints := strings.Split(addrlist, ",")
	for i, s := range endpoints {
		if s != "" && !strings.HasPrefix(s, "http://") {
			endpoints[i] = "http://" + s
		}
	}
	if timeout <= 0 {
		timeout = time.Second * 5
	}

	config := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Second * 5,
	}

	if auth != "" {
		split := strings.SplitN(auth, ":", 2)
		if len(split) != 2 || split[0] == "" {
			return nil, errors.Errorf("invalid auth")
		}
		config.Username = split[0]
		config.Password = split[1]
	}

	c, err := clientv3.New(config)
	if err != nil {
		return nil, errors.Trace(err)
	}

	client := &Client{
		client: c, timeout: timeout,
	}
	client.context, client.cancel = context.WithCancel(context.Background())
	return client, nil
}

func (c *Client) Close() error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil
	}
	c.client.Close()
	c.closed = true
	c.cancel()
	return nil
}

func (c *Client) newContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(c.context, c.timeout)
}

func (c *Client) Create(path string, data []byte) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return errors.Trace(ErrClosedClient)
	}
	cntx, cancel := c.newContext()
	defer cancel()

	log.Debugf("etcdv3 create node %s", path)
	resp, err := c.client.Txn(cntx).If(clientv3util.KeyMissing(path)).Then(clientv3.OpPut(path, string(data))).Commit()
	if err != nil {
		log.Debugf("etcdv3 create node %s failed: %s", path, err)
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.Trace(ErrTxnFailed)
	}
	log.Debugf("etcd create OK")
	return nil
}

func (c *Client) Update(path string, data []byte) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return errors.Trace(ErrClosedClient)
	}
	cntx, cancel := c.newContext()
	defer cancel()
	log.Debugf("etcdv3 update node %s", path)
	_, err := c.client.Put(cntx, path, string(data))
	if err != nil {
		log.Debugf("etcdv3 update node %s failed: %s", path, err)
		return errors.Trace(err)
	}
	log.Debugf("etcdv3 update OK")
	return nil
}

func (c *Client) Delete(path string, prefix bool) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return errors.Trace(ErrClosedClient)
	}
	cntx, cancel := c.newContext()
	defer cancel()
	if prefix {
		log.Debugf("etcdv3 delete keys with prefix %s", path)
		_, err := c.client.Delete(cntx, path, clientv3.WithPrefix())
		if err != nil {
			log.Debugf("etcdv3 delete keys with prefix %s failed: %s", path, err)
			return errors.Trace(err)
		}
		log.Debugf("etcdv3 delete keys with prefix %s OK", path)
	} else {
		log.Debugf("etcdv3 delete key %s", path)
		_, err := c.client.Delete(cntx, path)
		if err != nil {
			log.Debugf("etcdv3 delete key %s failed: %s", path, err)
			return errors.Trace(err)
		}
		log.Debugf("etcdv3 delete key %s OK", path)
	}
	return nil
}

func (c *Client) Read(path string, must bool) ([]byte, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, errors.Trace(ErrClosedClient)
	}
	cntx, cancel := c.newContext()
	defer cancel()
	resp, err := c.client.Get(cntx, path)
	if err != nil {
		log.Debugf("etcdv3 read node %s failed: %s", path, err)
		return nil, errors.Trace(err)
	}
	if resp.Count > 1 {
		log.Debugf("etcdv3 read node %s failed: not a file", path)
		return nil, errors.Trace(ErrNotFile)
	} else if resp.Count == 1 {
		return resp.Kvs[0].Value, nil
	}
	if !must {
		return nil, nil
	}
	//return nil, errors.Trace(ErrKeyNotExists)
	return nil, nil
}

func (c *Client) List(path string, must bool) ([]string, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, errors.Trace(ErrClosedClient)
	}
	cntx, cancel := c.newContext()
	defer cancel()
	resp, err := c.client.Get(cntx, path, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		log.Debugf("etcdv3 list node %s failed: %s", path, err)
		return nil, errors.Trace(err)
	}
	if resp.Count == 0 {
		if !must {
			return nil, nil
		} else {
			return nil, errors.Trace(ErrNotDir)
		}
	}

	pmap := make(map[string]bool)
	for _, kv := range resp.Kvs {
		key := strings.Split(string(kv.Key)[len(path):], "/")[1]
		//key := string(kv.Key)
		pmap[key] = true
	}
	var paths []string
	for key := range pmap {
		paths = append(paths, key)
	}
	return paths, nil
}

func (c *Client) CreateEphemeral(path string, data []byte) (<-chan struct{}, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, errors.Trace(ErrClosedClient)
	}
	cntx, cancel := c.newContext()
	defer cancel()
	log.Debugf("etcd create-ephemeral node %s", path)
	resp, err := c.client.Grant(cntx, int64(c.timeout))
	if err != nil {
		log.Debugf("etcd grant create-ephemeral node %s failed: %s", path, err)
		return nil, errors.Trace(err)
	}
	_, err = c.client.Put(cntx, path, string(data), clientv3.WithLease(resp.ID))
	if err != nil {
		log.Debugf("etcd put create-ephemeral node %s failed: %s", path, err)
		return nil, errors.Trace(err)
	}
	log.Debugf("etcd create-ephemeral OK")

	signal := make(chan struct{})
	keepAlive, err := c.client.KeepAlive(cntx, resp.ID)
	if err != nil || keepAlive == nil {
		close(signal)
		return nil, err
	}
	// keep the lease alive until client error or cancelled context
	go func() {
		defer close(signal)
		for range keepAlive {
			// eat messages until keep alive channel closes
		}
	}()
	return signal, nil
}

func (c *Client) CreateEphemeralInOrder(path string, data []byte) (<-chan struct{}, string, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, "", errors.Trace(ErrClosedClient)
	}
	cntx, cancel := c.newContext()
	defer cancel()
	log.Debugf("etcd create-ephemeral node %s", path)
	resp, err := c.client.Grant(cntx, int64(c.timeout))
	if err != nil {
		log.Debugf("etcd grant create-ephemeral node %s failed: %s", path, err)
		return nil, "", errors.Trace(err)
	}
	_, err = c.client.Put(cntx, path, string(data), clientv3.WithLease(resp.ID))
	if err != nil {
		log.Debugf("etcd put create-ephemeral node %s failed: %s", path, err)
		return nil, "", errors.Trace(err)
	}
	log.Debugf("etcd create-ephemeral OK")

	signal := make(chan struct{})
	keepAlive, err := c.client.KeepAlive(cntx, resp.ID)
	if err != nil || keepAlive == nil {
		close(signal)
		return nil, "", err
	}
	// keep the lease alive until client error or cancelled context
	go func() {
		defer close(signal)
		for range keepAlive {
			// eat messages until keep alive channel closes
		}
	}()
	return signal, path, nil
}

func (c *Client) WatchInOrder(path string) (<-chan struct{}, []string, error) {
	watcher := clientv3.NewWatcher(c.client)
	defer watcher.Close()

	cntx, cancel := c.newContext()
	defer cancel()

	paths, err := c.List(path, false)
	if err != nil {
		return nil, nil, err
	}

	signal := make(chan struct{})
	go func() {
		defer close(signal)
		for {
			rch := watcher.Watch(cntx, path, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
			for wresp := range rch {
				if wresp.Canceled {
					log.Debugf("etch watch-inorder node %s canceled", path)
					return
				}

				for _, ev := range wresp.Events {
					if ev.Type == mvccpb.DELETE || ev.Type == mvccpb.PUT {
						log.Debugf("etcd watch-inorder node %s update", path)
						return
					}
				}
			}
		}
	}()
	log.Debugf("etcd watch-inorder OK")
	return signal, paths, nil
}
