// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"time"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/math2"
	"github.com/CodisLabs/codis/pkg/utils/redis"
	"github.com/CodisLabs/codis/pkg/utils/sync2"
)

func (s *Topom) AddSentinel(product string, productAuth string, addr string) error {
	if err := s.productVerify(product, productAuth); err != nil {
		return err
	}

	s.products[product].mu.Lock()
	defer s.products[product].mu.Unlock()

	ctx, err := s.newProductContext(product)
	if err != nil {
		return err
	}

	if addr == "" {
		return errors.Errorf("invalid sentinel address")
	}
	p := ctx.sentinel

	for _, x := range p.Servers {
		if x == addr {
			return errors.Errorf("sentinel-[%s] already exists", addr)
		}
	}

	sentinel := redis.NewSentinel(product, ctx.auth)
	if err := sentinel.FlushConfig(addr, s.config.SentinelClientTimeout.Duration()); err != nil {
		return err
	}
	defer s.dirtySentinelCache(product)

	p.Servers = append(p.Servers, addr)
	p.OutOfSync = true
	return s.storeUpdateSentinel(product, p)
}

func (s *Topom) DelSentinel(product string, productAuth string, addr string, force bool) error {
	if err := s.productVerify(product, productAuth); err != nil {
		return err
	}

	s.products[product].mu.Lock()
	defer s.products[product].mu.Unlock()

	ctx, err := s.newProductContext(product)
	if err != nil {
		return err
	}

	if addr == "" {
		return errors.Errorf("invalid sentinel address")
	}
	p := ctx.sentinel

	var slice []string
	for _, x := range p.Servers {
		if x != addr {
			slice = append(slice, x)
		}
	}
	if len(slice) == len(p.Servers) {
		return errors.Errorf("sentinel-[%s] not found", addr)
	}
	defer s.dirtySentinelCache(product)

	p.OutOfSync = true
	if err := s.storeUpdateSentinel(product, p); err != nil {
		return err
	}

	sentinel := redis.NewSentinel(product, ctx.auth)
	if err := sentinel.RemoveGroupsAll([]string{addr}, s.config.SentinelClientTimeout.Duration()); err != nil {
		log.WarnErrorf(err, "remove sentinel %s failed", addr)
		if !force {
			return errors.Errorf("remove sentinel %s failed", addr)
		}
	}

	p.Servers = slice
	return s.storeUpdateSentinel(product, p)
}

func (s *Topom) SwitchMasters(product string, masters map[int]string) error {
	if s.closed {
		return ErrClosedTopom
	}

	if !s.productExist(product) {
		return ErrProductNotExist
	}

	s.products[product].mu.Lock()
	defer s.products[product].mu.Unlock()

	s.products[product].ha.masters = masters

	if len(masters) != 0 {
		cache := &redis.InfoCache{
			Auth: s.config.TopomAuth, Timeout: time.Millisecond * 100,
		}
		for gid, master := range masters {
			if err := s.trySwitchGroupMaster(product, gid, master, cache); err != nil {
				log.WarnErrorf(err, "sentinel switch group master failed")
			}
		}
	}
	return nil
}

func (s *Topom) rewatchSentinels(product string, servers []string) {
	if s.products[product].ha.monitor != nil {
		s.products[product].ha.monitor.Cancel()
		s.products[product].ha.monitor = nil
	}
	if len(servers) == 0 {
		s.products[product].ha.masters = nil
	} else {
		s.products[product].ha.monitor = redis.NewSentinel(product, s.products[product].cache.product.Auth)
		s.products[product].ha.monitor.LogFunc = log.Warnf
		s.products[product].ha.monitor.ErrFunc = log.WarnErrorf
		go func(p *redis.Sentinel) {
			var trigger = make(chan struct{}, 1)
			delayUntil := func(deadline time.Time) {
				for !p.IsCanceled() {
					var d = deadline.Sub(time.Now())
					if d <= 0 {
						return
					}
					time.Sleep(math2.MinDuration(d, time.Second))
				}
			}
			go func() {
				defer close(trigger)
				callback := func() {
					select {
					case trigger <- struct{}{}:
					default:
					}
				}
				for !p.IsCanceled() {
					timeout := time.Minute * 15
					retryAt := time.Now().Add(time.Second * 10)
					if !p.Subscribe(servers, timeout, callback) {
						delayUntil(retryAt)
					} else {
						callback()
					}
				}
			}()
			go func() {
				for range trigger {
					var success int
					for i := 0; i != 10 && !p.IsCanceled() && success != 2; i++ {
						timeout := time.Second * 5
						masters, err := p.Masters(servers, timeout)
						if err != nil {
							log.WarnErrorf(err, "fetch group masters failed")
						} else {
							if !p.IsCanceled() {
								s.SwitchMasters(product, masters)
							}
							success += 1
						}
						delayUntil(time.Now().Add(time.Second * 5))
					}
				}
			}()
		}(s.products[product].ha.monitor)
	}
	log.Warnf("rewatch sentinels = %v", servers)
}

func (s *Topom) ResyncSentinels(product string, productAuth string) error {
	if err := s.productVerify(product, productAuth); err != nil {
		return err
	}

	s.products[product].mu.Lock()
	defer s.products[product].mu.Unlock()

	ctx, err := s.newProductContext(product)
	if err != nil {
		return err
	}
	defer s.dirtySentinelCache(product)

	p := ctx.sentinel
	p.OutOfSync = true
	if err := s.storeUpdateSentinel(product, p); err != nil {
		return err
	}

	config := &redis.MonitorConfig{
		Quorum:               s.config.SentinelQuorum,
		ParallelSyncs:        s.config.SentinelParallelSyncs,
		DownAfter:            s.config.SentinelDownAfter.Duration(),
		FailoverTimeout:      s.config.SentinelFailoverTimeout.Duration(),
		NotificationScript:   s.config.SentinelNotificationScript,
		ClientReconfigScript: s.config.SentinelClientReconfigScript,
	}

	sentinel := redis.NewSentinel(product, s.config.TopomAuth)
	if err := sentinel.RemoveGroupsAll(p.Servers, s.config.SentinelClientTimeout.Duration()); err != nil {
		log.WarnErrorf(err, "remove sentinels failed")
	}
	if err := sentinel.MonitorGroups(p.Servers, s.config.SentinelClientTimeout.Duration(), config, ctx.getGroupMasters()); err != nil {
		log.WarnErrorf(err, "resync sentinels failed")
		return err
	}
	s.rewatchSentinels(product, p.Servers)

	var fut sync2.Future
	for _, p := range ctx.proxy {
		fut.Add()
		go func(p *models.Proxy) {
			err := s.newProxyClient(product, productAuth, p).SetSentinels(ctx.sentinel)
			if err != nil {
				log.ErrorErrorf(err, "proxy-[%s] resync sentinel failed", p.Token)
			}
			fut.Done(p.Token, err)
		}(p)
	}
	for t, v := range fut.Wait() {
		switch err := v.(type) {
		case error:
			if err != nil {
				return errors.Errorf("proxy-[%s] sentinel failed", t)
			}
		}
	}

	p.OutOfSync = false
	return s.storeUpdateSentinel(product, p)
}
