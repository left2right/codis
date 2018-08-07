// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	_ "net/http/pprof"

	"github.com/go-martini/martini"
	"github.com/martini-contrib/binding"
	"github.com/martini-contrib/gzip"
	"github.com/martini-contrib/render"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/redis"
	"github.com/CodisLabs/codis/pkg/utils/rpc"
)

const (
	redirectorHeader      = "topom-Redirector"
	productAuthTempPrefix = "product-auth-"
)

const (
	errRedirectFailed      = "redirect failed"
	errRedirectToNotLeader = "redirect to not leader"
)

type customReverseProxies struct {
	urls   []url.URL
	client *http.Client
}

func newCustomReverseProxies(t *Topom, urls []url.URL) *customReverseProxies {
	tls, _ := t.config.ToTLSConfig()
	dialClient := &http.Client{Transport: &http.Transport{
		TLSClientConfig:   tls,
		DisableKeepAlives: true,
	}}
	p := &customReverseProxies{
		client: dialClient,
	}

	p.urls = append(p.urls, urls...)

	return p
}

func (p *customReverseProxies) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	for _, url := range p.urls {
		r.RequestURI = ""
		r.URL.Host = url.Host
		r.URL.Scheme = url.Scheme

		resp, err := p.client.Do(r)
		if err != nil {
			log.Error(err)
			continue
		}

		b, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.Error(err)
			continue
		}

		copyHeader(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		if _, err := w.Write(b); err != nil {
			log.Error(err)
			continue
		}

		return
	}

	http.Error(w, errRedirectFailed, http.StatusInternalServerError)
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

type apiServer struct {
	topom *Topom
}

func newApiServer(t *Topom) http.Handler {
	m := martini.New()
	m.Use(martini.Recovery())
	m.Use(render.Renderer())
	m.Use(func(w http.ResponseWriter, req *http.Request, c martini.Context) {
		path := req.URL.Path
		if req.Method != "GET" && strings.HasPrefix(path, "/api/") {
			var remoteAddr = req.RemoteAddr
			var headerAddr string
			for _, key := range []string{"X-Real-IP", "X-Forwarded-For"} {
				if val := req.Header.Get(key); val != "" {
					headerAddr = val
					break
				}
			}
			log.Warnf("[%p] API call %s from %s [%s]", t, path, remoteAddr, headerAddr)
		}
		c.Next()
	})
	m.Use(func(w http.ResponseWriter, r *http.Request, c martini.Context) {
		if !t.IsLeader() {
			// Prevent more than one redirection.
			if name := r.Header.Get(redirectorHeader); len(name) != 0 {
				log.Errorf("redirect from %v, but %v is not leader", name, t.Name())
				http.Error(w, errRedirectToNotLeader, http.StatusInternalServerError)
				return
			}

			r.Header.Set(redirectorHeader, t.Name())

			/*
				leader, err := t.GetLeader()
				log.Infof("redirect topom cluster leader is %s", t.leaderTopom)
				if err != nil {
					log.Errorf("get redirect leader %s err %v", t.leaderTopom, err)
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
			*/

			log.Infof("redirect topom cluster leader is %s", t.leaderTopom)
			//TODO need optimization, (first path segment in URL cannot contain colon)
			urls, err := ParseUrls("http://" + t.leaderTopom)
			if err != nil {
				log.Errorf("get redirect leader urls %v err %v", urls, err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			newCustomReverseProxies(t, urls).ServeHTTP(w, r)
		}
		c.Next()
	})
	m.Use(gzip.All())
	m.Use(func(c martini.Context, w http.ResponseWriter) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	})

	api := &apiServer{topom: t}

	r := martini.NewRouter()

	r.Get("/", func(r render.Render) {
		r.Redirect("/topom")
	})
	r.Any("/debug/**", func(w http.ResponseWriter, req *http.Request) {
		http.DefaultServeMux.ServeHTTP(w, req)
	})

	r.Group("/topom", func(r martini.Router) {
		r.Get("", api.Overview)
		r.Get("/model", api.Model)
		//r.Get("/stats", api.StatsNoXAuth)
		//r.Get("/slots", api.SlotsNoXAuth)
	})
	r.Group("/api/topom", func(r martini.Router) {
		r.Get("/model", api.Model)
		r.Get("/xping/:xauth", api.XPing)
		r.Get("/stats/:xauth/:productname/:productauth", api.Stats)
		r.Get("/slots/:xauth/:productname/:productauth", api.Slots)
		r.Put("/reload/:xauth", api.Reload)
		r.Put("/shutdown/:xauth", api.Shutdown)
		r.Put("/loglevel/:xauth/:value", api.LogLevel)
		r.Group("/product", func(r martini.Router) {
			r.Put("/create/:xauth/:productname/:productauth", api.CreateProduct)
			r.Put("/update/:xauth/:productname/:productauth", api.UpdateProduct)
			r.Put("/remove/:xauth/:productname/:productauth/:force", api.RemoveProduct)
		})
		r.Group("/proxy", func(r martini.Router) {
			r.Put("/create/:xauth/:productname/:productauth/:addr", api.CreateProxy)
			r.Put("/online/:xauth/:productname/:productauth/:addr", api.OnlineProxy)
			r.Put("/reinit/:xauth/:productname/:productauth/:token", api.ReinitProxy)
			r.Put("/remove/:xauth/:productname/:productauth/:token/:force", api.RemoveProxy)
		})
		r.Group("/group", func(r martini.Router) {
			r.Put("/create/:xauth/:productname/:productauth/:gid", api.CreateGroup)
			r.Put("/remove/:xauth/:productname/:productauth/:gid", api.RemoveGroup)
			r.Put("/resync/:xauth/:productname/:productauth/:gid", api.ResyncGroup)
			r.Put("/resync-all/:xauth/:productname/:productauth", api.ResyncGroupAll)
			r.Put("/add/:xauth/:productname/:productauth/:gid/:addr", api.GroupAddServer)
			r.Put("/add/:xauth/:productname/:productauth/:gid/:addr/:datacenter", api.GroupAddServer)
			r.Put("/del/:xauth/:productname/:productauth/:gid/:addr", api.GroupDelServer)
			r.Put("/promote/:xauth/:productname/:productauth/:gid/:addr", api.GroupPromoteServer)
			r.Put("/replica-groups/:xauth/:productname/:productauth/:gid/:addr/:value", api.EnableReplicaGroups)
			r.Put("/replica-groups-all/:xauth/:productname/:productauth/:value", api.EnableReplicaGroupsAll)
			r.Group("/action", func(r martini.Router) {
				r.Put("/create/:xauth/:productname/:productauth/:addr", api.SyncCreateAction)
				r.Put("/remove/:xauth/:productname/:productauth/:addr", api.SyncRemoveAction)
			})
			r.Get("/info/:addr", api.InfoServer)
		})
		r.Group("/slots", func(r martini.Router) {
			r.Group("/action", func(r martini.Router) {
				r.Put("/create/:xauth/:productname/:productauth/:sid/:gid", api.SlotCreateAction)
				r.Put("/create-some/:xauth/:productname/:productauth/:src/:dst/:num", api.SlotCreateActionSome)
				r.Put("/create-range/:xauth/:productname/:productauth/:beg/:end/:gid", api.SlotCreateActionRange)
				r.Put("/remove/:xauth/:productname/:productauth/:sid", api.SlotRemoveAction)
				r.Put("/interval/:xauth/:productname/:productauth/:value", api.SetSlotActionInterval)
				r.Put("/disabled/:xauth/:productname/:productauth/:value", api.SetSlotActionDisabled)
			})
			r.Put("/assign/:xauth/:productname/:productauth", binding.Json([]*models.SlotMapping{}), api.SlotsAssignGroup)
			r.Put("/assign/:xauth/:productname/:productauth/offline", binding.Json([]*models.SlotMapping{}), api.SlotsAssignOffline)
			r.Put("/rebalance/:xauth/:productname/:productauth/:confirm", api.SlotsRebalance)
		})
		r.Group("/sentinels", func(r martini.Router) {
			r.Put("/add/:xauth/:productname/:productauth/:addr", api.AddSentinel)
			r.Put("/del/:xauth/:productname/:productauth/:addr/:force", api.DelSentinel)
			r.Put("/resync-all/:xauth/:productname/:productauth", api.ResyncSentinels)
			r.Get("/info/:addr", api.InfoSentinel)
			r.Get("/info/:addr/monitored", api.InfoSentinelMonitored)
		})
	})

	m.MapTo(r, (*martini.Routes)(nil))
	m.Action(r.Handle)
	return m
}

func (s *apiServer) verifyXAuth(params martini.Params) error {
	if s.topom.IsClosed() {
		return ErrClosedTopom
	}
	xauth := params["xauth"]
	if xauth == "" {
		return errors.New("missing xauth, please check product name & auth")
	}
	if xauth != s.topom.XAuth() {
		return errors.New("invalid xauth, please check product name & auth")
	}
	return nil
}

func (s *apiServer) Overview() (int, string) {
	o, err := s.topom.Overview()
	if err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson(o)
	}
}

func (s *apiServer) Model() (int, string) {
	return rpc.ApiResponseJson(s.topom.Model())
}

func (s *apiServer) XPing(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) Stats(params martini.Params) (int, string) {
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		if stats, err := s.topom.Stats(product, productAuth); err != nil {
			return rpc.ApiResponseError(err)
		} else {
			return rpc.ApiResponseJson(stats)
		}
	}
}

func (s *apiServer) Slots(params martini.Params) (int, string) {
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		if slots, err := s.topom.Slots(product, productAuth); err != nil {
			return rpc.ApiResponseError(err)
		} else {
			return rpc.ApiResponseJson(slots)
		}
	}
}

func (s *apiServer) Reload(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.Reload(); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) parseProductName(params martini.Params) (string, error) {
	product := params["productname"]
	if product == "" {
		return "", errors.New("missing product name")
	}
	return product, nil
}

func (s *apiServer) parseProductAuth(params martini.Params) (string, error) {
	auth := params["productauth"]
	if !strings.HasPrefix(auth, productAuthTempPrefix) {
		return "", errors.New("encode product auth error")
	}
	return decodeProductAuth(auth), nil
}

func (s *apiServer) parseAddr(params martini.Params) (string, error) {
	addr := params["addr"]
	if addr == "" {
		return "", errors.New("missing addr")
	}
	return addr, nil
}

func (s *apiServer) parseToken(params martini.Params) (string, error) {
	token := params["token"]
	if token == "" {
		return "", errors.New("missing token")
	}
	return token, nil
}

func (s *apiServer) parseInteger(params martini.Params, entry string) (int, error) {
	text := params[entry]
	if text == "" {
		return 0, fmt.Errorf("missing %s", entry)
	}
	v, err := strconv.Atoi(text)
	if err != nil {
		return 0, fmt.Errorf("invalid %s", entry)
	}
	return v, nil
}

func (s *apiServer) CreateProduct(params martini.Params) (int, string) {
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.CreateProduct(product, productAuth); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) UpdateProduct(params martini.Params) (int, string) {
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.UpdateProduct(product, productAuth); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) RemoveProduct(params martini.Params) (int, string) {
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	/*
		force, err := s.parseInteger(params, "force")
		if err != nil {
			return rpc.ApiResponseError(err)
		}
	*/
	if err := s.topom.RemoveProduct(product, productAuth); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) CreateProxy(params martini.Params) (int, string) {
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.CreateProxy(product, productAuth, addr); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) OnlineProxy(params martini.Params) (int, string) {
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.OnlineProxy(product, productAuth, addr); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) ReinitProxy(params martini.Params) (int, string) {
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	token, err := s.parseToken(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.ReinitProxy(product, productAuth, token); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) RemoveProxy(params martini.Params) (int, string) {
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	token, err := s.parseToken(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	force, err := s.parseInteger(params, "force")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.RemoveProxy(product, productAuth, token, force != 0); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) CreateGroup(params martini.Params) (int, string) {
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	gid, err := s.parseInteger(params, "gid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.CreateGroup(product, productAuth, gid); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) RemoveGroup(params martini.Params) (int, string) {
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	gid, err := s.parseInteger(params, "gid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.RemoveGroup(product, productAuth, gid); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) ResyncGroup(params martini.Params) (int, string) {
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	gid, err := s.parseInteger(params, "gid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.ResyncGroup(product, productAuth, gid); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) ResyncGroupAll(params martini.Params) (int, string) {
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.ResyncGroupAll(product, productAuth); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) GroupAddServer(params martini.Params) (int, string) {
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	gid, err := s.parseInteger(params, "gid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	dc := params["datacenter"]
	c, err := redis.NewClient(addr, productAuth, time.Second)
	if err != nil {
		log.WarnErrorf(err, "create redis client to %s failed", addr)
		return rpc.ApiResponseError(err)
	}
	defer c.Close()
	if _, err := c.SlotsInfo(); err != nil {
		log.WarnErrorf(err, "redis %s check slots-info failed", addr)
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.GroupAddServer(product, productAuth, gid, dc, addr); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) GroupDelServer(params martini.Params) (int, string) {
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	gid, err := s.parseInteger(params, "gid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.GroupDelServer(product, productAuth, gid, addr); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) GroupPromoteServer(params martini.Params) (int, string) {
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	gid, err := s.parseInteger(params, "gid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.GroupPromoteServer(product, productAuth, gid, addr); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) EnableReplicaGroups(params martini.Params) (int, string) {
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	gid, err := s.parseInteger(params, "gid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	n, err := s.parseInteger(params, "value")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.EnableReplicaGroups(product, productAuth, gid, addr, n != 0); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) EnableReplicaGroupsAll(params martini.Params) (int, string) {
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	n, err := s.parseInteger(params, "value")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.EnableReplicaGroupsAll(product, productAuth, n != 0); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) AddSentinel(params martini.Params) (int, string) {
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.AddSentinel(product, productAuth, addr); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) DelSentinel(params martini.Params) (int, string) {
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	force, err := s.parseInteger(params, "force")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.DelSentinel(product, productAuth, addr, force != 0); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) ResyncSentinels(params martini.Params) (int, string) {
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.ResyncSentinels(product, productAuth); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) InfoServer(params martini.Params) (int, string) {
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	c, err := redis.NewClient(addr, s.topom.Config().TopomAuth, time.Second)
	if err != nil {
		log.WarnErrorf(err, "create redis client to %s failed", addr)
		return rpc.ApiResponseError(err)
	}
	defer c.Close()
	if info, err := c.InfoFull(); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson(info)
	}
}

func (s *apiServer) InfoSentinel(params martini.Params) (int, string) {
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	c, err := redis.NewClientNoAuth(addr, time.Second)
	if err != nil {
		log.WarnErrorf(err, "create redis client to %s failed", addr)
		return rpc.ApiResponseError(err)
	}
	defer c.Close()
	if info, err := c.Info(); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson(info)
	}
}

func (s *apiServer) InfoSentinelMonitored(params martini.Params) (int, string) {
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	sentinel := redis.NewSentinel(product, s.topom.Config().TopomAuth)
	if info, err := sentinel.MastersAndSlaves(addr, s.topom.Config().SentinelClientTimeout.Duration()); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson(info)
	}
}

func (s *apiServer) SyncCreateAction(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.SyncCreateAction(product, productAuth, addr); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) SyncRemoveAction(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	addr, err := s.parseAddr(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.SyncRemoveAction(product, productAuth, addr); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) SlotCreateAction(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	sid, err := s.parseInteger(params, "sid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	gid, err := s.parseInteger(params, "gid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.SlotCreateAction(product, productAuth, sid, gid); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) SlotCreateActionSome(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	groupFrom, err := s.parseInteger(params, "src")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	groupTo, err := s.parseInteger(params, "dst")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	numSlots, err := s.parseInteger(params, "num")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.SlotCreateActionSome(product, productAuth, groupFrom, groupTo, numSlots); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) SlotCreateActionRange(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	beg, err := s.parseInteger(params, "beg")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	end, err := s.parseInteger(params, "end")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	gid, err := s.parseInteger(params, "gid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.SlotCreateActionRange(product, productAuth, beg, end, gid, true); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) SlotRemoveAction(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	sid, err := s.parseInteger(params, "sid")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.SlotRemoveAction(product, productAuth, sid); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) LogLevel(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	v := params["value"]
	if v == "" {
		return rpc.ApiResponseError(errors.New("missing loglevel"))
	}
	if !log.SetLevelString(v) {
		return rpc.ApiResponseError(errors.New("invalid loglevel"))
	} else {
		log.Warnf("set loglevel to %s", v)
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) Shutdown(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.Close(); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) SetSlotActionInterval(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	value, err := s.parseInteger(params, "value")
	if err != nil {
		return rpc.ApiResponseError(err)
	} else {
		s.topom.SetSlotActionInterval(product, productAuth, value)
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) SetSlotActionDisabled(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	value, err := s.parseInteger(params, "value")
	if err != nil {
		return rpc.ApiResponseError(err)
	} else {
		s.topom.SetSlotActionDisabled(product, productAuth, value != 0)
		return rpc.ApiResponseJson("OK")
	}
}

func (s *apiServer) SlotsAssignGroup(slots []*models.SlotMapping, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.SlotsAssignGroup(product, productAuth, slots); err != nil {
		return rpc.ApiResponseError(err)
	}
	return rpc.ApiResponseJson("OK")
}

func (s *apiServer) SlotsAssignOffline(slots []*models.SlotMapping, params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if err := s.topom.SlotsAssignOffline(product, productAuth, slots); err != nil {
		return rpc.ApiResponseError(err)
	}
	return rpc.ApiResponseJson("OK")
}

func (s *apiServer) SlotsRebalance(params martini.Params) (int, string) {
	if err := s.verifyXAuth(params); err != nil {
		return rpc.ApiResponseError(err)
	}
	product, err := s.parseProductName(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	productAuth, err := s.parseProductAuth(params)
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	confirm, err := s.parseInteger(params, "confirm")
	if err != nil {
		return rpc.ApiResponseError(err)
	}
	if plans, err := s.topom.SlotsRebalance(product, productAuth, confirm != 0); err != nil {
		return rpc.ApiResponseError(err)
	} else {
		m := make(map[string]int)
		for sid, gid := range plans {
			m[strconv.Itoa(sid)] = gid
		}
		return rpc.ApiResponseJson(m)
	}
}

type ApiClient struct {
	addr  string
	xauth string
}

func NewApiClient(addr string) *ApiClient {
	return &ApiClient{addr: addr}
}

func (c *ApiClient) SetXAuth(name string) {
	c.xauth = rpc.NewXAuth(name)
}

func (c *ApiClient) encodeURL(format string, args ...interface{}) string {
	return rpc.EncodeURL(c.addr, format, args...)
}

func encodeProductAuth(auth string) string {
	return productAuthTempPrefix + auth
}

func decodeProductAuth(auth string) string {
	return auth[len(productAuthTempPrefix):]
}

func (c *ApiClient) Overview() (*Overview, error) {
	url := c.encodeURL("/topom")
	var o = &Overview{}
	if err := rpc.ApiGetJson(url, o); err != nil {
		return nil, err
	}
	return o, nil
}

func (c *ApiClient) Model() (*models.Topom, error) {
	url := c.encodeURL("/api/topom/model")
	model := &models.Topom{}
	if err := rpc.ApiGetJson(url, model); err != nil {
		return nil, err
	}
	return model, nil
}

func (c *ApiClient) XPing() error {
	url := c.encodeURL("/api/topom/xping/%s", c.xauth)
	return rpc.ApiGetJson(url, nil)
}

func (c *ApiClient) Stats(product string, productAuth string) (*Stats, error) {
	url := c.encodeURL("/api/topom/stats/%s/%s/%s", c.xauth, product, encodeProductAuth(productAuth))
	stats := &Stats{}
	if err := rpc.ApiGetJson(url, stats); err != nil {
		return nil, err
	}
	return stats, nil
}

func (c *ApiClient) Slots(product string, productAuth string) ([]*models.Slot, error) {
	url := c.encodeURL("/api/topom/slots/%s/%s/%s", c.xauth, product, encodeProductAuth(productAuth))
	slots := []*models.Slot{}
	if err := rpc.ApiGetJson(url, &slots); err != nil {
		return nil, err
	}
	return slots, nil
}

func (c *ApiClient) Reload() error {
	url := c.encodeURL("/api/topom/reload/%s", c.xauth)
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) LogLevel(level log.LogLevel) error {
	url := c.encodeURL("/api/topom/loglevel/%s/%s", c.xauth, level)
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) Shutdown() error {
	url := c.encodeURL("/api/topom/shutdown/%s", c.xauth)
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) CreateProduct(product string, productAuth string) error {
	url := c.encodeURL("/api/topom/product/create/%s/%s/%s", c.xauth, product, encodeProductAuth(productAuth))
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) UpdateProduct(product string, productAuth string) error {
	url := c.encodeURL("/api/topom/product/update/%s/%s/%s", c.xauth, product, encodeProductAuth(productAuth))
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) RemoveProduct(product string, productAuth string, force bool) error {
	var value int
	if force {
		value = 1
	}
	url := c.encodeURL("/api/topom/product/remove/%s/%s/%s/%d", c.xauth, product, encodeProductAuth(productAuth), value)
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) CreateProxy(product string, productAuth string, addr string) error {
	url := c.encodeURL("/api/topom/proxy/create/%s/%s/%s/%s", c.xauth, product, encodeProductAuth(productAuth), addr)
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) OnlineProxy(product string, productAuth string, addr string) error {
	url := c.encodeURL("/api/topom/proxy/online/%s/%s/%s/%s", c.xauth, product, encodeProductAuth(productAuth), addr)
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) ReinitProxy(product string, productAuth string, token string) error {
	url := c.encodeURL("/api/topom/proxy/reinit/%s/%s/%s/%s", c.xauth, product, encodeProductAuth(productAuth), token)
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) RemoveProxy(product string, productAuth string, token string, force bool) error {
	var value int
	if force {
		value = 1
	}
	url := c.encodeURL("/api/topom/proxy/remove/%s/%s/%s/%s/%d", c.xauth, product, encodeProductAuth(productAuth), token, value)
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) CreateGroup(product string, productAuth string, gid int) error {
	url := c.encodeURL("/api/topom/group/create/%s/%s/%s/%d", c.xauth, product, encodeProductAuth(productAuth), gid)
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) RemoveGroup(product string, productAuth string, gid int) error {
	url := c.encodeURL("/api/topom/group/remove/%s/%s/%s/%d", c.xauth, product, encodeProductAuth(productAuth), gid)
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) ResyncGroup(product string, productAuth string, gid int) error {
	url := c.encodeURL("/api/topom/group/resync/%s/%s/%s/%d", c.xauth, product, encodeProductAuth(productAuth), gid)
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) ResyncGroupAll(product string, productAuth string) error {
	url := c.encodeURL("/api/topom/group/resync-all/%s/%s/%s", c.xauth, product, encodeProductAuth(productAuth))
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) GroupAddServer(product string, productAuth string, gid int, dc, addr string) error {
	var url string
	if dc != "" {
		url = c.encodeURL("/api/topom/group/add/%s/%s/%s/%d/%s/%s", c.xauth, product, encodeProductAuth(productAuth), gid, addr, dc)
	} else {
		url = c.encodeURL("/api/topom/group/add/%s/%s/%s/%d/%s", c.xauth, product, encodeProductAuth(productAuth), gid, addr)
	}
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) GroupDelServer(product string, productAuth string, gid int, addr string) error {
	url := c.encodeURL("/api/topom/group/del/%s/%s/%s/%d/%s", c.xauth, product, encodeProductAuth(productAuth), gid, addr)
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) GroupPromoteServer(product string, productAuth string, gid int, addr string) error {
	url := c.encodeURL("/api/topom/group/promote/%s/%s/%s/%d/%s", c.xauth, product, encodeProductAuth(productAuth), gid, addr)
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) EnableReplicaGroups(product string, productAuth string, gid int, addr string, value bool) error {
	var n int
	if value {
		n = 1
	}
	url := c.encodeURL("/api/topom/group/replica-groups/%s/%s/%s/%d/%s/%d", c.xauth, product, encodeProductAuth(productAuth), gid, addr, n)
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) EnableReplicaGroupsAll(product string, productAuth string, value bool) error {
	var n int
	if value {
		n = 1
	}
	url := c.encodeURL("/api/topom/group/replica-groups-all/%s/%s/%s/%d", c.xauth, product, encodeProductAuth(productAuth), n)
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) AddSentinel(product string, productAuth string, addr string) error {
	url := c.encodeURL("/api/topom/sentinels/add/%s/%s/%s/%s", c.xauth, product, encodeProductAuth(productAuth), addr)
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) DelSentinel(product string, productAuth string, addr string, force bool) error {
	var value int
	if force {
		value = 1
	}
	url := c.encodeURL("/api/topom/sentinels/del/%s/%s/%s/%s/%d", c.xauth, product, encodeProductAuth(productAuth), addr, value)
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) ResyncSentinels(product string, productAuth string) error {
	url := c.encodeURL("/api/topom/sentinels/resync-all/%s/%s/%s", c.xauth, product, encodeProductAuth(productAuth))
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) SyncCreateAction(product string, productAuth string, addr string) error {
	url := c.encodeURL("/api/topom/group/action/create/%s/%s/%s/%s", c.xauth, product, encodeProductAuth(productAuth), addr)
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) SyncRemoveAction(product string, productAuth string, addr string) error {
	url := c.encodeURL("/api/topom/group/action/remove/%s/%s/%s/%s", c.xauth, product, encodeProductAuth(productAuth), addr)
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) SlotCreateAction(product string, productAuth string, sid int, gid int) error {
	url := c.encodeURL("/api/topom/slots/action/create/%s/%s/%s/%d/%d", c.xauth, product, encodeProductAuth(productAuth), sid, gid)
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) SlotCreateActionSome(product string, productAuth string, groupFrom, groupTo int, numSlots int) error {
	url := c.encodeURL("/api/topom/slots/action/create-some/%s/%s/%s/%d/%d/%d", c.xauth, product, encodeProductAuth(productAuth), groupFrom, groupTo, numSlots)
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) SlotCreateActionRange(product string, productAuth string, beg, end int, gid int) error {
	url := c.encodeURL("/api/topom/slots/action/create-range/%s/%s/%s/%d/%d/%d", c.xauth, product, encodeProductAuth(productAuth), beg, end, gid)
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) SlotRemoveAction(product string, productAuth string, sid int) error {
	url := c.encodeURL("/api/topom/slots/action/remove/%s/%s/%s/%d", c.xauth, product, encodeProductAuth(productAuth), sid)
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) SetSlotActionInterval(product string, productAuth string, usecs int) error {
	url := c.encodeURL("/api/topom/slots/action/interval/%s/%s/%s/%d", c.xauth, product, encodeProductAuth(productAuth), usecs)
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) SetSlotActionDisabled(product string, productAuth string, disabled bool) error {
	var value int
	if disabled {
		value = 1
	}
	url := c.encodeURL("/api/topom/slots/action/disabled/%s/%s/%s/%d", c.xauth, product, encodeProductAuth(productAuth), value)
	return rpc.ApiPutJson(url, nil, nil)
}

func (c *ApiClient) SlotsAssignGroup(product string, productAuth string, slots []*models.SlotMapping) error {
	url := c.encodeURL("/api/topom/slots/assign/%s/%s/%s", c.xauth, product, encodeProductAuth(productAuth))
	return rpc.ApiPutJson(url, slots, nil)
}

func (c *ApiClient) SlotsAssignOffline(product string, productAuth string, slots []*models.SlotMapping) error {
	url := c.encodeURL("/api/topom/slots/assign/%s/%s/%s/offline", c.xauth, product, encodeProductAuth(productAuth))
	return rpc.ApiPutJson(url, slots, nil)
}

func (c *ApiClient) SlotsRebalance(product string, productAuth string, confirm bool) (map[int]int, error) {
	var value int
	if confirm {
		value = 1
	}
	url := c.encodeURL("/api/topom/slots/rebalance/%s/%s/%s/%d", c.xauth, product, encodeProductAuth(productAuth), value)
	var plans = make(map[string]int)
	if err := rpc.ApiPutJson(url, nil, &plans); err != nil {
		return nil, err
	} else {
		var m = make(map[int]int)
		for sid, gid := range plans {
			n, err := strconv.Atoi(sid)
			if err != nil {
				return nil, errors.Trace(err)
			}
			m[n] = gid
		}
		return m, nil
	}
}
