// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package topom

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/etcdutil"
	"github.com/CodisLabs/codis/pkg/utils/log"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

var (
	errNoLeader         = errors.New("no leader")
	resignLeaderTimeout = time.Second * 5
	nextLeaderTTL       = 10 // in seconds
)

const (
	topomLeaderPath = "/topom/leader"
	requestTimeout  = 10 * time.Second
	slowRequestTime = etcdutil.DefaultSlowRequestTime
)

// IsLeader returns whether server is leader or not.
func (s *Topom) IsLeader() bool {
	return atomic.LoadInt64(&s.isLeader) == 1
}

func (s *Topom) enableLeader(b bool) {
	value := int64(0)
	if b {
		value = 1
	}

	atomic.StoreInt64(&s.isLeader, value)
}

func (s *Topom) getLeaderPath() string {
	return filepath.Join(models.TopomRootPath, "leader")
}

// getLeader gets server leader from etcd.
func getLeader(c models.Client, leaderPath string) (string, error) {
	leader, err := c.Read(leaderPath, false)
	if err != nil {
		return "", err
	}
	return string(leader), nil
}

// GetLeader gets pd cluster leader.
func (s *Topom) GetLeader() (string, error) {
	if s.closed {
		return "", errors.New("server is closed")
	}
	leader, err := getLeader(s.store.Client(), s.getLeaderPath())
	if err != nil {
		return "", errors.Trace(err)
	}
	return leader, nil
}

func (s *Topom) isSameLeader(leader string) bool {
	return leader == s.model.AdminAddr
}

func (s *Topom) startLeaderLoop() {
	s.leaderLoopCtx, s.leaderLoopCancel = context.WithCancel(context.Background())
	s.leaderLoopWg.Add(1)
	go s.leaderLoop()
}

func (s *Topom) stopLeaderLoop() {
	s.leaderLoopCancel()
	s.leaderLoopWg.Wait()
}

func (s *Topom) leaderLoop() {
	defer log.LogPanic()
	defer s.leaderLoopWg.Done()

	for {
		if s.closed {
			log.Infof("server is closed, stop leader loop and return")
			return
		}

		leader, err := getLeader(s.store.Client(), s.getLeaderPath())
		if err != nil {
			log.Errorf("get leader err %v", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if leader != "" {
			if s.isSameLeader(leader) {
				// oh, we are already leader, we may meet something wrong
				// in previous campaignLeader. we can delete and campaign again.
				log.Warnf("leader is still %s, delete and campaign again", leader)
				if err = s.deleteLeaderKey(); err != nil {
					log.Errorf("delete leader key err %s", err)
					time.Sleep(200 * time.Millisecond)
					continue
				}
			} else {
				log.Infof("leader is %s watch it", leader)
				s.watchLeader(leader)
				log.Info("leader changed, try to campaign leader")
			}
		}

		etcdLeader := s.etcd.Server.Lead()
		if etcdLeader != s.ID() {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		if err = s.campaignLeader(); err != nil {
			log.Errorf("campaign leader err %s", err)
		}
	}
}

func (s *Topom) campaignLeader() error {
	log.Debugf("begin to campaign leader %s", s.Name())

	lessor := clientv3.NewLease(s.cliv3)
	defer lessor.Close()

	start := time.Now()
	ctx, cancel := context.WithTimeout(s.cliv3.Ctx(), requestTimeout)
	leaseResp, err := lessor.Grant(ctx, s.config.LeaderLease)
	cancel()

	if cost := time.Since(start); cost > slowRequestTime {
		log.Warnf("lessor grants too slow, cost %s", cost)
	}

	if err != nil {
		return errors.Trace(err)
	}

	leaderKey := s.getLeaderPath()
	// The leader key must not exist, so the CreateRevision is 0.
	resp, err := s.cliv3.Txn(s.cliv3.Ctx()).
		If(clientv3.Compare(clientv3.CreateRevision(leaderKey), "=", 0)).
		Then(clientv3.OpPut(leaderKey, s.model.AdminAddr, clientv3.WithLease(clientv3.LeaseID(leaseResp.ID)))).
		Commit()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.New("campaign leader failed, other server may campaign ok")
	}

	// Make the leader keepalived.
	ctx, cancel = context.WithCancel(s.leaderLoopCtx)
	defer cancel()

	ch, err := lessor.KeepAlive(ctx, clientv3.LeaseID(leaseResp.ID))
	if err != nil {
		return errors.Trace(err)
	}
	log.Debugf("campaign leader ok %s", s.Name())

	s.enableLeader(true)
	defer s.enableLeader(false)

	s.setLeaderTopom(s.model.AdminAddr)
	defer s.emptyLeaderTopom()

	if err := s.startProducts(); err != nil {
		log.PanicErrorf(err, "topom start products error")
		return errors.Trace(err)
	}
	defer s.stopProducts()

	log.Infof("Topom cluster leader %s %s is ready to serve", s.Name(), s.leaderTopom)

	for {
		select {
		case _, ok := <-ch:
			if !ok {
				log.Info("keep alive channel is closed")
				return nil
			}
		case <-ctx.Done():
			return errors.New("server closed")
		}
	}
}

func (s *Topom) watchLeader(leader string) {
	watcher := clientv3.NewWatcher(s.cliv3)
	defer watcher.Close()

	s.setLeaderTopom(leader)
	defer s.emptyLeaderTopom()

	ctx, cancel := context.WithCancel(s.leaderLoopCtx)
	defer cancel()

	for {
		rch := watcher.Watch(ctx, s.getLeaderPath())
		for wresp := range rch {
			if wresp.Canceled {
				return
			}

			for _, ev := range wresp.Events {
				if ev.Type == mvccpb.DELETE {
					log.Warn("leader is deleted")
					return
				}
			}
		}

		select {
		case <-ctx.Done():
			// server closed, return
			return
		default:
		}
	}
}

func (s *Topom) setLeaderTopom(leader string) {
	s.leaderTopom = leader
}

func (s *Topom) emptyLeaderTopom() {
	s.leaderTopom = ""
}

func (s *Topom) deleteLeaderKey() error {
	// delete leader itself and let others start a new election again.
	leaderKey := s.getLeaderPath()
	resp, err := s.cliv3.Txn(s.cliv3.Ctx()).Then(clientv3.OpDelete(leaderKey)).Commit()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.New("resign leader failed, we are not leader already")
	}

	return nil
}

func (s *Topom) leaderCmp() clientv3.Cmp {
	return clientv3.Compare(clientv3.Value(s.getLeaderPath()), "=", s.model.AdminAddr)
}
