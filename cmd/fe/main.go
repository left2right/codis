// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/docopt/docopt-go"
	"github.com/go-martini/martini"
	"github.com/martini-contrib/render"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/rpc"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"
)

var roundTripper http.RoundTripper

func init() {
	var dials atomic2.Int64
	tr := &http.Transport{}
	tr.Dial = func(network, addr string) (net.Conn, error) {
		c, err := net.DialTimeout(network, addr, time.Second*10)
		if err == nil {
			log.Debugf("rpc: dial new connection to [%d] %s - %s",
				dials.Incr()-1, network, addr)
		}
		return c, err
	}
	go func() {
		for {
			time.Sleep(time.Minute)
			tr.CloseIdleConnections()
		}
	}()
	roundTripper = tr
}

func main() {
	const usage = `
Usage:
	codis-fe [--ncpu=N] [--log=FILE] [--log-level=LEVEL] [--assets-dir=PATH] [--pidfile=FILE] (--codis-topoms=ADDR |--etcdv3=ADDR [--etcd-auth=USR:PWD]) [--topom-auth=AUTH]  --listen=ADDR
	codis-fe  --version

Options:
	--ncpu=N                        set runtime.GOMAXPROCS to N, default is runtime.NumCPU().
	--codis-topoms=ADDR   			set topom cluster servers.
	-l FILE, --log=FILE             set path/name of daliy rotated log file.
	--log-level=LEVEL               set the log-level, should be INFO,WARN,DEBUG or ERROR, default is INFO.
	--listen=ADDR                   set the listen address.
`
	d, err := docopt.Parse(usage, nil, true, "", false)
	if err != nil {
		log.PanicError(err, "parse arguments failed")
	}

	if d["--version"].(bool) {
		fmt.Println("version:", utils.Version)
		fmt.Println("compile:", utils.Compile)
		return
	}

	if s, ok := utils.Argument(d, "--log"); ok {
		w, err := log.NewRollingFile(s, log.DailyRolling)
		if err != nil {
			log.PanicErrorf(err, "open log file %s failed", s)
		} else {
			log.StdLog = log.New(w, "")
		}
	}
	log.SetLevel(log.LevelInfo)

	if s, ok := utils.Argument(d, "--log-level"); ok {
		if !log.SetLevelString(s) {
			log.Panicf("option --log-level = %s", s)
		}
	}

	if n, ok := utils.ArgumentInteger(d, "--ncpu"); ok {
		runtime.GOMAXPROCS(n)
	} else {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}
	log.Warnf("set ncpu = %d", runtime.GOMAXPROCS(0))

	listen := utils.ArgumentMust(d, "--listen")
	log.Warnf("set listen = %s", listen)

	var assets string
	if s, ok := utils.Argument(d, "--assets-dir"); ok {
		abspath, err := filepath.Abs(s)
		if err != nil {
			log.PanicErrorf(err, "get absolute path of %s failed", s)
		}
		assets = abspath
	} else {
		binpath, err := filepath.Abs(filepath.Dir(os.Args[0]))
		if err != nil {
			log.PanicErrorf(err, "get path of binary failed")
		}
		assets = filepath.Join(binpath, "assets")
	}
	log.Warnf("set assets = %s", assets)

	indexFile := filepath.Join(assets, "index.html")
	if _, err := os.Stat(indexFile); err != nil {
		log.PanicErrorf(err, "get stat of %s failed", indexFile)
	}

	var loader ConfigLoader
	var coordinator struct {
		name string
		addr string
		auth string
	}

	switch {

	case d["--etcdv3"] != nil:
		coordinator.name = "etcdv3"
		coordinator.addr = utils.ArgumentMust(d, "--etcdv3")
		if d["--etcd-auth"] != nil {
			coordinator.auth = utils.ArgumentMust(d, "--etcd-auth")
		}

	default:
		log.Panicf("invalid coordinator")
	}

	var topomAuth string
	if s, ok := d["--topom-auth"].(string); ok {
		topomAuth = s
		log.Debugf("topom auth is %s", s)
	} else {
		topomAuth = ""
	}

	log.Warnf("set --%s = %s", coordinator.name, coordinator.addr)

	c, err := models.NewClient(coordinator.name, coordinator.addr, coordinator.auth, time.Minute)
	if err != nil {
		log.PanicErrorf(err, "create '%s' client to '%s' failed", coordinator.name, coordinator.addr)
	}
	defer c.Close()

	loader = &DynamicLoader{c}

	router := NewReverseProxy(loader, topomAuth)

	m := martini.New()
	m.Use(martini.Recovery())
	m.Use(render.Renderer())
	m.Use(martini.Static(assets, martini.StaticOptions{SkipLogging: true}))

	r := martini.NewRouter()
	r.Get("/products", func() (int, string) {
		names := router.GetProductNames()
		sort.Sort(sort.StringSlice(names))
		return rpc.ApiResponseJson(names)
	})

	r.Get("/topom-auth", func() (int, string) {
		auth := router.GetTopomAuth()
		return rpc.ApiResponseJson(auth)
	})

	r.Get("/product-auth/**", func(req *http.Request) (int, string) {
		name := req.URL.Query().Get("product")
		auth := router.GetProductAuth(name)
		return rpc.ApiResponseJson(auth)
	})

	r.Any("/**", func(w http.ResponseWriter, req *http.Request) {
		name := req.URL.Query().Get("product")
		if p := router.GetProxy(name); p != nil {
			p.ServeHTTP(w, req)
		} else {
			w.WriteHeader(http.StatusForbidden)
		}
	})

	m.MapTo(r, (*martini.Routes)(nil))
	m.Action(r.Handle)

	l, err := net.Listen("tcp", listen)
	if err != nil {
		log.PanicErrorf(err, "listen %s failed", listen)
	}
	defer l.Close()

	if s, ok := utils.Argument(d, "--pidfile"); ok {
		if pidfile, err := filepath.Abs(s); err != nil {
			log.WarnErrorf(err, "parse pidfile = '%s' failed", s)
		} else if err := ioutil.WriteFile(pidfile, []byte(strconv.Itoa(os.Getpid())), 0644); err != nil {
			log.WarnErrorf(err, "write pidfile = '%s' failed", pidfile)
		} else {
			defer func() {
				if err := os.Remove(pidfile); err != nil {
					log.WarnErrorf(err, "remove pidfile = '%s' failed", pidfile)
				}
			}()
			log.Warnf("option --pidfile = %s", pidfile)
		}
	}

	h := http.NewServeMux()
	h.Handle("/", m)
	hs := &http.Server{Handler: h}
	if err := hs.Serve(l); err != nil {
		log.PanicErrorf(err, "serve %s failed", listen)
	}
}

type ConfigLoader interface {
	Reload() (map[string]string, string, error)
}

type DynamicLoader struct {
	client models.Client
}

func (l *DynamicLoader) Reload() (map[string]string, string, error) {
	var m = make(map[string]string)
	var leader string
	list, err := l.client.List(models.CodisProductsDir, false)
	if err != nil {
		return nil, "", errors.Trace(err)
	}
	var pdt struct {
		Name string `json:"name"`
		Auth string `json:"auth"`
	}
	for _, product := range list {
		if b, err := l.client.Read(models.CodisProductsDir+"/"+product, false); err != nil {
			log.WarnErrorf(err, "read topom of product %s failed", product)
		} else if b != nil {
			if err := json.Unmarshal(b, &pdt); err != nil {
				return nil, "", errors.Trace(err)
			}
			m[product] = "product-auth-" + string(pdt.Auth)
			log.Debugf("product %s auth is %s", product, pdt.Auth)
		}
	}

	if b, err := l.client.Read("/topom/leader", false); err != nil {
		log.WarnErrorf(err, "read topom leader failed")
	} else if b != nil {
		leader = string(b)
		log.Debugf("topom leader is %s", leader)
	}

	return m, leader, nil
}

type ReverseProxy struct {
	sync.Mutex
	loadAt       time.Time
	loader       ConfigLoader
	topomAuth    string
	productAuths map[string]string
	routes       map[string]*httputil.ReverseProxy
}

func NewReverseProxy(loader ConfigLoader, auth string) *ReverseProxy {
	r := &ReverseProxy{}
	r.topomAuth = auth
	r.loader = loader
	r.productAuths = make(map[string]string)
	r.routes = make(map[string]*httputil.ReverseProxy)
	return r
}

func (r *ReverseProxy) reload(d time.Duration) {
	if time.Now().Sub(r.loadAt) < d {
		return
	}
	r.routes = make(map[string]*httputil.ReverseProxy)
	if m, l, err := r.loader.Reload(); err != nil {
		log.WarnErrorf(err, "reload reverse proxy failed")
	} else {
		log.Debugf("topom leader is %s, and products is %v", l, m)
		for name, auth := range m {
			if name == "" {
				continue
			}
			r.productAuths[name] = auth
			u := &url.URL{Scheme: "http", Host: l}
			p := httputil.NewSingleHostReverseProxy(u)
			p.Transport = roundTripper
			r.routes[name] = p
		}
	}
	r.loadAt = time.Now()
}

func (r *ReverseProxy) GetProxy(name string) *httputil.ReverseProxy {
	r.Lock()
	defer r.Unlock()
	return r.routes[name]
}

func (r *ReverseProxy) GetTopomAuth() string {
	r.Lock()
	defer r.Unlock()
	return r.topomAuth
}

func (r *ReverseProxy) GetProductAuth(name string) string {
	r.Lock()
	defer r.Unlock()
	return r.productAuths[name]
}

func (r *ReverseProxy) GetProductNames() []string {
	r.Lock()
	defer r.Unlock()
	r.reload(time.Second * 5)
	var names []string
	for name, _ := range r.routes {
		names = append(names, name)
	}
	return names
}
