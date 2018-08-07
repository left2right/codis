// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"container/list"
	"context"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/math2"
	"github.com/CodisLabs/codis/pkg/utils/redis"
	"github.com/CodisLabs/codis/pkg/utils/rpc"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
)

var (
	ErrNotOnline       = errors.New("topom is not online")
	ErrClosedTopom     = errors.New("use of closed topom")
	ErrProductNotExist = errors.New("product not exist")
)

type product struct {
	mu        sync.Mutex
	productWg sync.WaitGroup
	stop      bool

	cache struct {
		hooks   list.List
		product *models.Product
		slots   []*models.SlotMapping
		group   map[int]*models.Group
		proxy   map[string]*models.Proxy

		sentinel *models.Sentinel
	}

	action struct {
		redisp *redis.Pool

		interval atomic2.Int64
		disabled atomic2.Bool

		progress struct {
			status atomic.Value
		}
		executor atomic2.Int64
	}

	stats struct {
		redisp *redis.Pool

		servers map[string]*RedisStats
		proxies map[string]*ProxyStats
	}

	ha struct {
		redisp *redis.Pool

		monitor *redis.Sentinel
		masters map[int]string
	}
}

type Topom struct {
	mu sync.Mutex

	xauth    string
	model    *models.Topom //take out
	store    *models.Store
	products map[string]*product

	exit struct {
		C chan struct{}
	}

	ladmin net.Listener

	config *Config
	online bool
	closed bool

	// Etcd and cluster informations.
	id      uint64 // etcd server id.
	etcdCfg *embed.Config

	etcd  *embed.Etcd
	cliv3 *clientv3.Client

	isLeader         int64
	leaderTopom      string
	leaderLoopCtx    context.Context
	leaderLoopCancel func()
	leaderLoopWg     sync.WaitGroup
}

func New(config *Config) (*Topom, error) {
	if err := config.Validate(); err != nil {
		return nil, errors.Trace(err)
	}
	s := &Topom{}
	s.config = config

	s.config.CoordinatorName = "etcdv3"
	s.config.CoordinatorAddr = s.config.AdvertiseClientUrls
	etcdCfg, err := s.config.genEmbedEtcdConfig()
	if err != nil {
		return nil, errors.Trace(err)
	}
	s.etcdCfg = etcdCfg

	s.exit.C = make(chan struct{})

	// init model
	s.model = &models.Topom{
		StartTime: time.Now().String(),
	}
	s.model.Pid = os.Getpid()
	s.model.Pwd, _ = os.Getwd()
	if b, err := exec.Command("uname", "-a").Output(); err != nil {
		log.WarnErrorf(err, "run command uname failed")
	} else {
		s.model.Sys = strings.TrimSpace(string(b))
	}

	if err := s.setup(config); err != nil {
		log.WarnErrorf(err, "topom setup failed")
		s.Close()
		return nil, err
	}

	log.Warnf("create new topom:\n%s", s.model.Encode())

	go s.serveAdmin()

	return s, nil
}

func (s *Topom) Start(routines bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedTopom
	}
	if s.online {
		return nil
	} else {
		s.online = true
	}

	if !routines {
		return nil
	}

	s.startLeaderLoop()
	for s.leaderTopom == "" {
		log.Warnf("topom leader is not set, need wait!!")
		time.Sleep(200 * time.Millisecond)
	}

	// if the topom is leader init products
	/*if err := s.startProducts(); err != nil {
		log.PanicErrorf(err, "topom start products error")
		return errors.Trace(err)
	}*/

	return nil
}

func (s *Topom) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	close(s.exit.C)

	if s.ladmin != nil {
		s.ladmin.Close()
	}
	/*for name, _ := range s.products {
		s.stopProduct(name)
	}*/

	s.stopLeaderLoop()

	defer s.store.Close()

	return nil
}

func (s *Topom) XAuth() string {
	return s.xauth
}

func (s *Topom) Model() *models.Topom {
	return s.model
}

// ID returns the unique etcd ID for this server in etcd cluster.
func (s *Topom) ID() uint64 {
	return s.id
}

// Name returns the unique etcd Name for this server in etcd cluster.
func (s *Topom) Name() string {
	return s.config.Name
}

func (s *Topom) setup(config *Config) error {
	if l, err := net.Listen("tcp", config.AdminAddr); err != nil {
		return errors.Trace(err)
	} else {
		s.ladmin = l

		x, err := utils.ReplaceUnspecifiedIP("tcp", l.Addr().String(), s.config.HostAdmin)
		if err != nil {
			return err
		}
		s.model.AdminAddr = x
	}

	s.model.Token = rpc.NewToken(
		config.TopomAuth,
		s.ladmin.Addr().String(),
	)
	s.xauth = rpc.NewXAuth(config.TopomAuth)

	if err := s.startEtcd(); err != nil {
		log.Errorf("topom start etcd  %v", err)
		return errors.Trace(err)
	}

	cli, err := models.NewClient(config.CoordinatorName, config.CoordinatorAddr, config.CoordinatorAuth, time.Minute)
	log.Infof("coordinator name %s addr %s", config.CoordinatorName, config.CoordinatorAddr)
	if err != nil {
		log.PanicErrorf(err, "create '%s' client to '%s' failed", config.CoordinatorName, config.CoordinatorAddr)
		return errors.Trace(err)
	}
	s.store = models.NewStore(cli)

	return nil
}

func (s *Topom) startProducts() error {
	if !s.IsLeader() {
		return nil
	}
	pdt, err := s.store.ListProduct()
	if err != nil {
		log.PanicErrorf(err, "get products from etcd error")
		return err
	}
	s.products = make(map[string]*product, len(pdt))
	for _, p := range pdt {
		if err = s.startProduct(p); err != nil {
			log.PanicErrorf(err, "start product p error")
			return err
		}
	}
	return nil
}

func (s *Topom) startProduct(product string) error {
	if err := s.initProduct(product); err != nil {
		return err
	}
	if err := s.runProduct(product); err != nil {
		return err
	}
	return nil
}

func (s *Topom) stopProducts() error {
	if !s.IsLeader() {
		return nil
	}
	for product, _ := range s.products {
		if err := s.stopProduct(product); err != nil {
			log.ErrorErrorf(err, "start product p error")
			return err
		}
	}
	return nil
}

func (s *Topom) stopProduct(product string) error {
	if _, ok := s.products[product]; !ok {
		log.Warnf("product %s is not in topom cache", product)
		return nil
	}
	for _, p := range []*redis.Pool{
		s.products[product].action.redisp, s.products[product].stats.redisp, s.products[product].ha.redisp,
	} {
		if p != nil {
			p.Close()
		}
	}
	s.products[product].stop = true
	s.products[product].productWg.Wait()
	delete(s.products, product)
	return nil
}

func (s *Topom) initProduct(name string) error {
	if !s.IsLeader() {
		return nil
	}
	if s.closed {
		return ErrClosedTopom
	}
	if s.online {
		if s.products == nil {
			s.products = make(map[string]*product, 1)
		}
		p, ok := s.products[name]
		if !ok {
			s.products[name] = &product{}
			p = s.products[name]
		}

		if err := s.refillProductCache(name); err != nil {
			return err
		}

		p.action.redisp = redis.NewPool(s.config.TopomAuth, s.config.MigrationTimeout.Duration())
		p.action.progress.status.Store("")

		p.stats.redisp = redis.NewPool(s.config.TopomAuth, time.Second*5)
		p.stats.servers = make(map[string]*RedisStats)
		p.stats.proxies = make(map[string]*ProxyStats)

		p.ha.redisp = redis.NewPool("", time.Second*5)
		return nil
	} else {
		return ErrNotOnline
	}
}

func (s *Topom) runProduct(product string) error {
	if !s.IsLeader() {
		return nil
	}

	ctx, err := s.newProductContext(product)
	if err != nil {
		return err
	}
	s.rewatchSentinels(product, ctx.sentinel.Servers)
	log.Warnf("product %s is running!", product)
	go func() {
		log.Warnf("product %s start RefreshRedisStats", product)
		s.products[product].productWg.Add(1)
		defer s.products[product].productWg.Done()
		for !s.IsClosed() {
			if s.IsOnline() {
				w, _ := s.RefreshRedisStats(product, time.Second)
				if w != nil {
					w.Wait()
				}
			}
			time.Sleep(time.Second)
			if s.products[product].stop {
				log.Warnf("product %s stop RefreshRedisStats", product)
				return
			}
		}
	}()

	go func() {
		log.Warnf("product %s start RefreshProxyStats", product)
		s.products[product].productWg.Add(1)
		defer s.products[product].productWg.Done()
		for !s.IsClosed() {
			if s.IsOnline() {
				w, _ := s.RefreshProxyStats(product, time.Second)
				if w != nil {
					w.Wait()
				}
			}
			time.Sleep(time.Second)
			if s.products[product].stop {
				log.Warnf("product %s stop RefreshProxyStats", product)
				return
			}
		}
	}()

	go func() {
		log.Warnf("product %s start ProcessSlotAction", product)
		s.products[product].productWg.Add(1)
		defer s.products[product].productWg.Done()
		for !s.IsClosed() {
			if s.IsOnline() {
				if err := s.ProcessSlotAction(product); err != nil {
					log.WarnErrorf(err, "process slot action failed")
					time.Sleep(time.Second * 5)
				}
			}
			time.Sleep(time.Second)
			if s.products[product].stop {
				log.Warnf("product %s stop ProcessSlotAction", product)
				return
			}
		}
	}()

	go func() {
		log.Warnf("product %s start ProcessSyncAction", product)
		s.products[product].productWg.Add(1)
		defer s.products[product].productWg.Done()
		for !s.IsClosed() {
			if s.IsOnline() {
				if err := s.ProcessSyncAction(product); err != nil {
					log.WarnErrorf(err, "process sync action failed")
					time.Sleep(time.Second * 5)
				}
			}
			time.Sleep(time.Second)
			if s.products[product].stop {
				log.Warnf("product %s stop ProcessSyncAction", product)
				return
			}
		}
	}()

	return nil
}

func (s *Topom) Stats(product string, productAuth string) (*Stats, error) {
	if err := s.productVerify(product, productAuth); err != nil {
		return nil, err
	}

	s.products[product].mu.Lock()
	defer s.products[product].mu.Unlock()

	log.Infof("Stats productContext refill product %s", product)
	ctx, err := s.newProductContext(product)
	if err != nil {
		return nil, err
	}

	stats := &Stats{}
	stats.Closed = s.closed

	stats.Slots = ctx.slots

	stats.Group.Models = models.SortGroup(ctx.group)
	stats.Group.Stats = map[string]*RedisStats{}
	for _, g := range ctx.group {
		for _, x := range g.Servers {
			if v := s.products[product].stats.servers[x.Addr]; v != nil {
				stats.Group.Stats[x.Addr] = v
			}
		}
	}

	stats.Proxy.Models = models.SortProxy(ctx.proxy)
	stats.Proxy.Stats = s.products[product].stats.proxies

	stats.SlotAction.Interval = s.products[product].action.interval.Int64()
	stats.SlotAction.Disabled = s.products[product].action.disabled.Bool()
	stats.SlotAction.Progress.Status = s.products[product].action.progress.status.Load().(string)
	stats.SlotAction.Executor = s.products[product].action.executor.Int64()

	stats.HA.Model = ctx.sentinel
	stats.HA.Stats = map[string]*RedisStats{}
	for _, server := range ctx.sentinel.Servers {
		if v := s.products[product].stats.servers[server]; v != nil {
			stats.HA.Stats[server] = v
		}
	}
	stats.HA.Masters = make(map[string]string)
	if s.products[product].ha.masters != nil {
		for gid, addr := range s.products[product].ha.masters {
			stats.HA.Masters[strconv.Itoa(gid)] = addr
		}
	}
	return stats, nil
}

type Stats struct {
	Closed bool `json:"closed"`

	Slots []*models.SlotMapping `json:"slots"`

	Group struct {
		Models []*models.Group        `json:"models"`
		Stats  map[string]*RedisStats `json:"stats"`
	} `json:"group"`

	Proxy struct {
		Models []*models.Proxy        `json:"models"`
		Stats  map[string]*ProxyStats `json:"stats"`
	} `json:"proxy"`

	SlotAction struct {
		Interval int64 `json:"interval"`
		Disabled bool  `json:"disabled"`

		Progress struct {
			Status string `json:"status"`
		} `json:"progress"`

		Executor int64 `json:"executor"`
	} `json:"slot_action"`

	HA struct {
		Model   *models.Sentinel       `json:"model"`
		Stats   map[string]*RedisStats `json:"stats"`
		Masters map[string]string      `json:"masters"`
	} `json:"sentinels"`
}

func (s *Topom) Config() *Config {
	return s.config
}

func (s *Topom) IsOnline() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.online && !s.closed
}

func (s *Topom) IsClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *Topom) GetSlotActionInterval(product string) int {
	return s.products[product].action.interval.AsInt()
}

func (s *Topom) SetSlotActionInterval(product string, productAuth string, us int) {
	us = math2.MinMaxInt(us, 0, 1000*1000)
	s.products[product].action.interval.Set(int64(us))
	log.Warnf("set product %s action interval = %d", product, us)
}

func (s *Topom) GetSlotActionDisabled(product string) bool {
	return s.products[product].action.disabled.Bool()
}

func (s *Topom) SetSlotActionDisabled(product string, productAuth string, value bool) {
	s.products[product].action.disabled.Set(value)
	log.Warnf("set product %s action disabled = %t", product, value)
}

func (s *Topom) Slots(product string, productAuth string) ([]*models.Slot, error) {
	if err := s.productVerify(product, productAuth); err != nil {
		return nil, err
	}

	s.products[product].mu.Lock()
	defer s.products[product].mu.Unlock()

	ctx, err := s.newProductContext(product)
	if err != nil {
		return nil, err
	}
	return ctx.toSlotSlice(ctx.slots, nil), nil
}

func (s *Topom) Reload() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ps, err := s.store.ListProduct()
	if err != nil {
		return err
	}
	for _, p := range ps {
		if _, err = s.newProductContext(p); err != nil {
			return err
		}
		defer s.dirtyCacheProduct(p)
	}
	return nil
}

func (s *Topom) serveAdmin() {
	if s.IsClosed() {
		return
	}
	defer s.Close()

	log.Warnf("admin start service on %s", s.ladmin.Addr())

	eh := make(chan error, 1)
	go func(l net.Listener) {
		h := http.NewServeMux()
		h.Handle("/", newApiServer(s))
		hs := &http.Server{Handler: h}
		eh <- hs.Serve(l)
	}(s.ladmin)

	select {
	case <-s.exit.C:
		log.Warnf("admin shutdown")
	case err := <-eh:
		log.ErrorErrorf(err, "admin exit on error")
	}
}

type Overview struct {
	Version      string            `json:"version"`
	Compile      string            `json:"compile"`
	Config       *Config           `json:"config,omitempty"`
	Model        *models.Topom     `json:"model,omitempty"`
	ProductStats map[string]*Stats `json:"product_stats,omitempty"`
}

func (s *Topom) Overview() (*Overview, error) {
	pstats := make(map[string]*Stats)
	for _, p := range s.products {
		if stats, err := s.Stats(p.cache.product.Name, p.cache.product.Auth); err != nil {
			return nil, err
		} else {
			pstats[p.cache.product.Name] = stats
		}
	}

	return &Overview{
		Version:      utils.Version,
		Compile:      utils.Compile,
		Config:       s.Config(),
		Model:        s.Model(),
		ProductStats: pstats,
	}, nil
}
