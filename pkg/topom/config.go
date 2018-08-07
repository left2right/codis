// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/coreos/etcd/embed"
	"github.com/coreos/etcd/pkg/transport"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/bytesize"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/timesize"
)

const (
	defaultName                = "topom"
	defaultClientUrls          = "http://127.0.0.1:2379"
	defaultPeerUrls            = "http://127.0.0.1:2380"
	defualtInitialClusterState = embed.ClusterStateFlagNew

	defaultLeaderLease             = int64(3)
	defaultAutoCompactionRetention = 1

	// etcd use 100ms for heartbeat and 1s for election timeout.
	// We can enlarge both a little to reduce the network aggression.
	// now embed etcd use TickMs for heartbeat, we will update
	// after embed etcd decouples tick and heartbeat.
	defaultTickInterval = 500 * time.Millisecond
	// embed etcd has a check that `5 * tick > election`
	defaultElectionInterval = 3000 * time.Millisecond

	defaultLeaderPriorityCheckInterval = time.Minute
)

const DefaultConfig = `
##################################################
#                                                #
#                  Codis-Dashboard               #
#                                                #
##################################################

# Set Coordinator, only accept "etcdv3".
# for zookeeper/etcd, coorinator_auth accept "user:password" 
# Quick Start
coordinator_name = "etcdv3"
coordinator_addr = "127.0.0.1:2379"
coordinator_auth = ""

# Set Codis Topom Auth.
topom_auth = ""

# Set bind address for admin(rpc), tcp only.
admin_addr = "0.0.0.0:18080"

# Set arguments for data migration (only accept 'sync' & 'semi-async').
migration_method = "semi-async"
migration_parallel_slots = 100
migration_async_maxbulks = 200
migration_async_maxbytes = "32mb"
migration_async_numkeys = 500
migration_timeout = "30s"

# Set configs for redis sentinel.
sentinel_client_timeout = "10s"
sentinel_quorum = 2
sentinel_parallel_syncs = 1
sentinel_down_after = "30s"
sentinel_failover_timeout = "5m"
sentinel_notification_script = ""
sentinel_client_reconfig_script = ""

# topom cluster configurations
name = "topom"
data-dir = "default.topom"

client-urls = "http://127.0.0.1:2379"
# if not set, use ${client-urls}
advertise-client-urls = ""

peer-urls = "http://127.0.0.1:2380"
# if not set, use ${peer-urls}
advertise-peer-urls = ""

initial-cluster = "topom=http://127.0.0.1:2380"
initial-cluster-state = "new"

lease = 3

cacert-path = ""
cert-path = ""
key-path = ""
`

type Config struct {
	CoordinatorName string `toml:"coordinator_name" json:"coordinator_name"`
	CoordinatorAddr string `toml:"coordinator_addr" json:"coordinator_addr"`
	CoordinatorAuth string `toml:"coordinator_auth" json:"coordinator_auth"`

	AdminAddr string `toml:"admin_addr" json:"admin_addr"`

	HostAdmin string `toml:"-" json:"-"`

	TopomAuth string `toml:"topom_auth" json:"-"`

	// Migration Method should be set according to product, not topom
	MigrationMethod        string            `toml:"migration_method" json:"migration_method"`
	MigrationParallelSlots int               `toml:"migration_parallel_slots" json:"migration_parallel_slots"`
	MigrationAsyncMaxBulks int               `toml:"migration_async_maxbulks" json:"migration_async_maxbulks"`
	MigrationAsyncMaxBytes bytesize.Int64    `toml:"migration_async_maxbytes" json:"migration_async_maxbytes"`
	MigrationAsyncNumKeys  int               `toml:"migration_async_numkeys" json:"migration_async_numkeys"`
	MigrationTimeout       timesize.Duration `toml:"migration_timeout" json:"migration_timeout"`

	SentinelClientTimeout        timesize.Duration `toml:"sentinel_client_timeout" json:"sentinel_client_timeout"`
	SentinelQuorum               int               `toml:"sentinel_quorum" json:"sentinel_quorum"`
	SentinelParallelSyncs        int               `toml:"sentinel_parallel_syncs" json:"sentinel_parallel_syncs"`
	SentinelDownAfter            timesize.Duration `toml:"sentinel_down_after" json:"sentinel_down_after"`
	SentinelFailoverTimeout      timesize.Duration `toml:"sentinel_failover_timeout" json:"sentinel_failover_timeout"`
	SentinelNotificationScript   string            `toml:"sentinel_notification_script" json:"sentinel_notification_script"`
	SentinelClientReconfigScript string            `toml:"sentinel_client_reconfig_script" json:"sentinel_client_reconfig_script"`

	Name    string `toml:"name" json:"name"`
	DataDir string `toml:"data-dir" json:"data-dir"`

	ClientUrls          string `toml:"client-urls" json:"client-urls"`
	PeerUrls            string `toml:"peer-urls" json:"peer-urls"`
	AdvertiseClientUrls string `toml:"advertise-client-urls" json:"advertise-client-urls"`
	AdvertisePeerUrls   string `toml:"advertise-peer-urls" json:"advertise-peer-urls"`

	InitialCluster      string `toml:"initial-cluster" json:"initial-cluster"`
	InitialClusterState string `toml:"initial-cluster-state" json:"initial-cluster-state"`

	// Join to an existing pd cluster, a string of endpoints.
	Join string `toml:"join" json:"join"`

	// LeaderLease time, if leader doesn't update its TTL
	// in etcd after lease time, etcd will expire the leader key
	// and other servers can campaign the leader again.
	// Etcd onlys support seoncds TTL, so here is second too.
	LeaderLease int64 `toml:"lease" json:"lease"`

	// QuotaBackendBytes Raise alarms when backend size exceeds the given quota. 0 means use the default quota.
	// the default size is 2GB, the maximum is 8GB.
	QuotaBackendBytes bytesize.Int64 `toml:"quota-backend-bytes" json:"quota-backend-bytes"`
	// AutoCompactionRetention for mvcc key value store in hour. 0 means disable auto compaction.
	// the default retention is 1 hour
	AutoCompactionRetention int `toml:"auto-compaction-retention" json:"auto-compaction-retention"`

	// TickInterval is the interval for etcd Raft tick.
	TickInterval time.Duration `toml:"tick-interval"`
	// ElectionInterval is the interval for etcd Raft election.
	ElectionInterval            time.Duration `toml:"election-interval"`
	leaderPriorityCheckInterval time.Duration
	disableStrictReconfigCheck  bool

	// CAPath is the path of file that contains list of trusted SSL CAs. if set, following settings shouldn't be empty
	CAPath string `toml:"cacert-path" json:"cacert-path"`
	// CertPath is the path of file that contains X509 certificate in PEM format.
	CertPath string `toml:"cert-path" json:"cert-path"`
	// KeyPath is the path of file that contains X509 key in PEM format.
	KeyPath string `toml:"key-path" json:"key-path"`
}

// ToTLSConfig generatres tls config.
func (c *Config) ToTLSConfig() (*tls.Config, error) {
	if len(c.CertPath) == 0 && len(c.KeyPath) == 0 {
		return nil, nil
	}
	tlsInfo := transport.TLSInfo{
		CertFile:      c.CertPath,
		KeyFile:       c.KeyPath,
		TrustedCAFile: c.CAPath,
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return tlsConfig, nil
}

func NewDefaultConfig() *Config {
	c := &Config{}
	if _, err := toml.Decode(DefaultConfig, c); err != nil {
		log.PanicErrorf(err, "decode toml failed")
	}
	if err := c.adjust(); err != nil {
		log.PanicErrorf(err, "validate config failed")
	}
	return c
}

func (c *Config) LoadFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	if err != nil {
		return errors.Trace(err)
	}
	return c.adjust()
}

func (c *Config) String() string {
	var b bytes.Buffer
	e := toml.NewEncoder(&b)
	e.Indent = "    "
	e.Encode(c)
	return b.String()
}

func (c *Config) Validate() error {
	if c.CoordinatorName == "" {
		return errors.New("invalid coordinator_name")
	}
	if c.AdminAddr == "" {
		return errors.New("invalid admin_addr")
	}
	if _, ok := models.ParseForwardMethod(c.MigrationMethod); !ok {
		return errors.New("invalid migration_method")
	}
	if c.MigrationParallelSlots <= 0 {
		return errors.New("invalid migration_parallel_slots")
	}
	if c.MigrationAsyncMaxBulks <= 0 {
		return errors.New("invalid migration_async_maxbulks")
	}
	if c.MigrationAsyncMaxBytes <= 0 {
		return errors.New("invalid migration_async_maxbytes")
	}
	if c.MigrationAsyncNumKeys <= 0 {
		return errors.New("invalid migration_async_numkeys")
	}
	if c.MigrationTimeout <= 0 {
		return errors.New("invalid migration_timeout")
	}
	if c.SentinelClientTimeout <= 0 {
		return errors.New("invalid sentinel_client_timeout")
	}
	if c.SentinelQuorum <= 0 {
		return errors.New("invalid sentinel_quorum")
	}
	if c.SentinelParallelSyncs <= 0 {
		return errors.New("invalid sentinel_parallel_syncs")
	}
	if c.SentinelDownAfter <= 0 {
		return errors.New("invalid sentinel_down_after")
	}
	if c.SentinelFailoverTimeout <= 0 {
		return errors.New("invalid sentinel_failover_timeout")
	}
	return nil
}

func (c *Config) adjust() error {
	if err := c.Validate(); err != nil {
		return errors.Trace(err)
	}

	adjustString(&c.Name, defaultName)
	adjustString(&c.DataDir, fmt.Sprintf("default.%s", c.Name))

	adjustString(&c.ClientUrls, defaultClientUrls)
	adjustString(&c.AdvertiseClientUrls, c.ClientUrls)
	adjustString(&c.PeerUrls, defaultPeerUrls)
	adjustString(&c.AdvertisePeerUrls, c.PeerUrls)

	if len(c.InitialCluster) == 0 {
		// The advertise peer urls may be http://127.0.0.1:2380,http://127.0.0.1:2381
		// so the initial cluster is pd=http://127.0.0.1:2380,pd=http://127.0.0.1:2381
		items := strings.Split(c.AdvertisePeerUrls, ",")

		sep := ""
		for _, item := range items {
			c.InitialCluster += fmt.Sprintf("%s%s=%s", sep, c.Name, item)
			sep = ","
		}
	}

	adjustString(&c.InitialClusterState, defualtInitialClusterState)

	if len(c.Join) > 0 {
		if _, err := url.Parse(c.Join); err != nil {
			return errors.Errorf("failed to parse join addr:%s, err:%v", c.Join, err)
		}
	}

	adjustInt64(&c.LeaderLease, defaultLeaderLease)

	if c.AutoCompactionRetention == 0 {
		c.AutoCompactionRetention = defaultAutoCompactionRetention
	}

	//adjustDuration(c.TickInterval, defaultTickInterval)
	c.TickInterval = defaultTickInterval
	//adjustDuration(c.ElectionInterval, defaultElectionInterval)
	c.ElectionInterval = defaultElectionInterval
	//adjustDuration(c.leaderPriorityCheckInterval, defaultLeaderPriorityCheckInterval)
	c.leaderPriorityCheckInterval = defaultLeaderPriorityCheckInterval
	return nil
}

func adjustString(v *string, defValue string) {
	if len(*v) == 0 {
		*v = defValue
	}
}

func adjustInt64(v *int64, defValue int64) {
	if *v == 0 {
		*v = defValue
	}
}

func adjustDuration(v time.Duration, defValue time.Duration) {
	if v == 0 {
		v = defValue
	}
}

// ParseUrls parse a string into multiple urls.
// Export for api.
func ParseUrls(s string) ([]url.URL, error) {
	items := strings.Split(s, ",")
	urls := make([]url.URL, 0, len(items))
	for _, item := range items {
		u, err := url.Parse(item)
		if err != nil {
			return nil, errors.Trace(err)
		}

		urls = append(urls, *u)
	}

	return urls, nil
}

// generates a configuration for embedded etcd.
func (c *Config) genEmbedEtcdConfig() (*embed.Config, error) {
	cfg := embed.NewConfig()
	cfg.Name = c.Name
	cfg.Dir = c.DataDir
	cfg.WalDir = ""
	cfg.InitialCluster = c.InitialCluster
	cfg.ClusterState = c.InitialClusterState
	cfg.EnablePprof = true
	cfg.StrictReconfigCheck = !c.disableStrictReconfigCheck
	cfg.TickMs = uint(c.TickInterval / time.Millisecond)
	cfg.ElectionMs = uint(c.ElectionInterval / time.Millisecond)
	cfg.AutoCompactionRetention = c.AutoCompactionRetention
	cfg.QuotaBackendBytes = int64(c.QuotaBackendBytes)

	cfg.ClientTLSInfo.ClientCertAuth = len(c.CAPath) != 0
	cfg.ClientTLSInfo.TrustedCAFile = c.CAPath
	cfg.ClientTLSInfo.CertFile = c.CertPath
	cfg.ClientTLSInfo.KeyFile = c.KeyPath
	cfg.PeerTLSInfo.TrustedCAFile = c.CAPath
	cfg.PeerTLSInfo.CertFile = c.CertPath
	cfg.PeerTLSInfo.KeyFile = c.KeyPath

	var err error

	cfg.LPUrls, err = ParseUrls(c.PeerUrls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.APUrls, err = ParseUrls(c.AdvertisePeerUrls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.LCUrls, err = ParseUrls(c.ClientUrls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.ACUrls, err = ParseUrls(c.AdvertiseClientUrls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return cfg, nil
}
