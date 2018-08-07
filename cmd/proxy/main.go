// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/docopt/docopt-go"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/proxy"
	"github.com/CodisLabs/codis/pkg/topom"
	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/math2"
)

func main() {
	const usage = `
Usage:
	codis-proxy [--ncpu=N [--max-ncpu=MAX]] [--config=CONF] [--log=FILE] [--log-level=LEVEL] [--host-admin=ADDR] [--host-proxy=ADDR] [--codis-topom=ADDR] [--topom-auth=AUTH] [--ulimit=NLIMIT] [--pidfile=FILE] [--product_name=NAME] [--product_auth=AUTH] [--session_auth=AUTH]
	codis-proxy  --default-config
	codis-proxy  --version

Options:
	--ncpu=N                    set runtime.GOMAXPROCS to N, default is runtime.NumCPU().
	-c CONF, --config=CONF      run with the specific configuration.
	-l FILE, --log=FILE         set path/name of daliy rotated log file.
	--log-level=LEVEL           set the log-level, should be INFO,WARN,DEBUG or ERROR, default is INFO.
	--ulimit=NLIMIT             run 'ulimit -n' to check the maximum number of open file descriptors.
`

	d, err := docopt.Parse(usage, nil, true, "", false)
	if err != nil {
		log.PanicError(err, "parse arguments failed")
	}

	switch {

	case d["--default-config"]:
		fmt.Println(proxy.DefaultConfig)
		return

	case d["--version"].(bool):
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

	if n, ok := utils.ArgumentInteger(d, "--ulimit"); ok {
		b, err := exec.Command("/bin/sh", "-c", "ulimit -n").Output()
		if err != nil {
			log.PanicErrorf(err, "run ulimit -n failed")
		}
		if v, err := strconv.Atoi(strings.TrimSpace(string(b))); err != nil || v < n {
			log.PanicErrorf(err, "ulimit too small: %d, should be at least %d", v, n)
		}
	}

	var ncpu int
	if n, ok := utils.ArgumentInteger(d, "--ncpu"); ok {
		ncpu = n
	} else {
		ncpu = 4
	}
	runtime.GOMAXPROCS(ncpu)

	var maxncpu int
	if n, ok := utils.ArgumentInteger(d, "--max-ncpu"); ok {
		maxncpu = math2.MaxInt(ncpu, n)
	} else {
		maxncpu = math2.MaxInt(ncpu, runtime.NumCPU())
	}
	log.Warnf("set ncpu = %d, max-ncpu = %d", ncpu, maxncpu)

	if ncpu < maxncpu {
		go AutoGOMAXPROCS(ncpu, maxncpu)
	}

	config := proxy.NewDefaultConfig()
	if s, ok := utils.Argument(d, "--config"); ok {
		if err := config.LoadFromFile(s); err != nil {
			log.PanicErrorf(err, "load config %s failed", s)
		}
	}
	if s, ok := utils.Argument(d, "--host-admin"); ok {
		config.HostAdmin = s
		log.Warnf("option --host-admin = %s", s)
	}
	if s, ok := utils.Argument(d, "--host-proxy"); ok {
		config.HostProxy = s
		log.Warnf("option --host-proxy = %s", s)
	}

	if s, ok := utils.Argument(d, "--codis-topom"); ok {
		config.TopomAddr = s
	}
	if s, ok := utils.Argument(d, "--topom-auth"); ok {
		config.TopomAuth = s
	}

	var slots []*models.Slot
	if s, ok := utils.Argument(d, "--fillslots"); ok {
		b, err := ioutil.ReadFile(s)
		if err != nil {
			log.PanicErrorf(err, "load slots from file failed")
		}
		if err := json.Unmarshal(b, &slots); err != nil {
			log.PanicErrorf(err, "decode slots from json failed")
		}
	}

	if s, ok := utils.Argument(d, "--product_name"); ok {
		config.ProductName = s
		log.Warnf("option --product_name = %s", s)
	}
	if s, ok := utils.Argument(d, "--product_auth"); ok {
		config.ProductAuth = s
		log.Warnf("option --product_auth = %s", s)
	}
	if s, ok := utils.Argument(d, "--session_auth"); ok {
		config.SessionAuth = s
		log.Warnf("option --session_auth = %s", s)
	}

	s, err := proxy.New(config)
	if err != nil {
		log.PanicErrorf(err, "create proxy with config file failed\n%s", config)
	}
	defer s.Close()

	log.Warnf("create proxy with config\n%s", config)

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

	go func() {
		defer s.Close()
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM)

		sig := <-c
		log.Warnf("[%p] proxy receive signal = '%v'", s, sig)
	}()

	switch {
	case config.TopomAddr != "":
		go AutoOnlineWithTopom(s)
	case slots != nil:
		go AutoOnlineWithFillSlots(s, slots)
	}

	for !s.IsClosed() && !s.IsOnline() {
		log.Warnf("[%p] proxy waiting online ...", s)
		time.Sleep(time.Second)
	}

	log.Warnf("[%p] proxy is working ...", s)

	for !s.IsClosed() {
		time.Sleep(time.Second)
	}

	log.Warnf("[%p] proxy is exiting ...", s)
}

func AutoGOMAXPROCS(min, max int) {
	for {
		var ncpu = runtime.GOMAXPROCS(0)
		var less, more int
		var usage [10]float64
		for i := 0; i < len(usage) && more == 0; i++ {
			u, _, err := utils.CPUUsage(time.Second)
			if err != nil {
				log.WarnErrorf(err, "get cpu usage failed")
				time.Sleep(time.Second * 30)
				continue
			}
			u /= float64(ncpu)
			switch {
			case u < 0.30 && ncpu > min:
				less++
			case u > 0.70 && ncpu < max:
				more++
			}
			usage[i] = u
		}
		var nn = ncpu
		switch {
		case more != 0:
			nn = ncpu + ((max - ncpu + 3) / 4)
		case less == len(usage):
			nn = ncpu - 1
		}
		if nn != ncpu {
			runtime.GOMAXPROCS(nn)
			var b bytes.Buffer
			for i, u := range usage {
				if i != 0 {
					fmt.Fprintf(&b, ", ")
				}
				fmt.Fprintf(&b, "%.3f", u)
			}
			log.Warnf("ncpu = %d -> %d, usage = [%s]", ncpu, nn, b.Bytes())
		}
	}
}

func AutoOnlineWithTopom(p *proxy.Proxy) {
	for i := 0; i < 10; i++ {
		if p.IsClosed() || p.IsOnline() {
			return
		}
		if OnlineProxy(p) {
			return
		}
		time.Sleep(time.Second * 3)
	}
	log.Panicf("online proxy failed")
}

func AutoOnlineWithFillSlots(p *proxy.Proxy, slots []*models.Slot) {
	if err := p.FillSlots(slots); err != nil {
		log.PanicErrorf(err, "fill slots failed")
	}
	if err := p.Start(); err != nil {
		log.PanicErrorf(err, "start proxy failed")
	}
}

func OnlineProxy(p *proxy.Proxy) bool {
	for _, addr := range strings.Split(p.Config().TopomAddr, ",") {
		client := topom.NewApiClient(addr)
		_, err := client.Model()
		if err != nil {
			log.WarnErrorf(err, "rpc fetch model failed")
			continue
		}
		client.SetXAuth(p.Config().TopomAuth)

		if err := client.OnlineProxy(p.Config().ProductName, p.Config().ProductAuth, p.Model().AdminAddr); err != nil {
			log.WarnErrorf(err, "rpc online proxy failed")
			return false
		} else {
			log.Warnf("rpc online proxy seems OK")
			return true
		}
	}
	log.Warnf("rpc online proxy failed, no accessible topom server %s", p.Config().TopomAddr)
	return false
}
