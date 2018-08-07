// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/etcdutil"
	"github.com/CodisLabs/codis/pkg/utils/log"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/coreos/etcd/pkg/types"
)

const (
	etcdTimeout = time.Second * 3
	//TopomRootPath = "/topom"
)

func (s *Topom) startEtcd() error {
	log.Info("start embed etcd")
	etcd, err := embed.StartEtcd(s.etcdCfg)
	if err != nil {
		log.Errorf("topom start embed etcd  %v", err)
		return errors.Trace(err)
	}

	// Check cluster ID
	urlmap, err := types.NewURLsMap(s.config.InitialCluster)
	if err != nil {
		log.Errorf("create URLsMap err  %v", err)
		return errors.Trace(err)
	}
	tlsConfig, err := s.config.ToTLSConfig()
	if err != nil {
		log.Errorf("create tlsconfig err  %v", err)
		return errors.Trace(err)
	}
	if err = etcdutil.CheckClusterID(etcd.Server.Cluster().ID(), urlmap, tlsConfig); err != nil {
		log.Errorf("check cluster ID err  %v", err)
		return errors.Trace(err)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	select {
	// Wait etcd until it is ready to use
	case <-etcd.Server.ReadyNotify():
	case sig := <-sc:
		return errors.Errorf("receive signal %v when waiting embed etcd to be ready", sig)
	}

	endpoints := []string{s.etcdCfg.ACUrls[0].String()}
	log.Infof("create etcd v3 client with endpoints %v", endpoints)

	cliv3, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: etcdTimeout,
		TLS:         tlsConfig,
	})
	if err != nil {
		log.Errorf("create etcd v3 client err  %v", err)
		return errors.Trace(err)
	}

	etcdServerID := uint64(etcd.Server.ID())

	// update advertise peer urls.
	etcdMembers, err := etcdutil.ListEtcdMembers(cliv3)
	if err != nil {
		return errors.Trace(err)
	}
	for _, m := range etcdMembers.Members {
		if etcdServerID == m.ID {
			etcdPeerURLs := strings.Join(m.PeerURLs, ",")
			if s.config.AdvertisePeerUrls != etcdPeerURLs {
				log.Infof("update advertise peer urls from %s to %s", s.config.AdvertisePeerUrls, etcdPeerURLs)
				s.config.AdvertisePeerUrls = etcdPeerURLs
			}
		}
	}

	s.etcd = etcd
	s.id = etcdServerID
	s.cliv3 = cliv3
	return nil
}
