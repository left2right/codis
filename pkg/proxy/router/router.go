// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"net"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"

	topo "github.com/wandoulabs/codis/pkg/proxy/router/topology"

	"github.com/wandoulabs/codis/pkg/models"

	"github.com/juju/errors"
	stats "github.com/ngaut/gostats"
	log "github.com/ngaut/logging"
)

type Server struct {
	slots  [models.DEFAULT_SLOT_NUM]*Slot
	evtbus chan interface{}

	conf *Config
	topo *topo.Topology
	info models.ProxyInfo
	addr string

	lastActionSeq int

	counter   *stats.Counters
	OnSuicide func() error
}

func (s *Server) clearSlot(i int) {
	panic("todo")
	/*
		if !validSlot(i) {
			return
		}

		if s.slots[i] != nil {
			s.slots[i].dst = nil
			s.slots[i].migrateFrom = nil
			s.slots[i] = nil
		}
	*/
}

func (s *Server) fillSlot(i int, force bool) {
	panic("todo")
	/*
		if !validSlot(i) {
			return
		}

		if !force && s.slots[i] != nil { //check
			log.Fatalf("slot %d already filled, slot: %+v", i, s.slots[i])
			return
		}

		s.clearSlot(i)

		slotInfo, groupInfo, err := s.topo.GetSlotByIndex(i)
		if err != nil {
			log.Fatal(errors.ErrorStack(err))
		}

		slot := &Slot{
			slotInfo:  slotInfo,
			dst:       group.NewGroup(*groupInfo),
			groupInfo: groupInfo,
		}

		log.Infof("fill slot %d, force %v, %+v", i, force, slot.dst)

		s.pools.AddPool(slot.dst.Master())

		if slot.slotInfo.State.Status == models.SLOT_STATUS_MIGRATE {
			//get migrate src group and fill it
			from, err := s.topo.GetGroup(slot.slotInfo.State.MigrateStatus.From)
			if err != nil { //todo: retry ?
				log.Fatal(err)
			}
			slot.migrateFrom = group.NewGroup(*from)
			s.pools.AddPool(slot.migrateFrom.Master())
		}

		s.slots[i] = slot
		s.counter.Add("FillSlot", 1)
	*/
}

func (s *Server) handleMigrateState(slotIndex int, key []byte) error {
	panic("todo")
	/*
		shd := s.slots[slotIndex]
		if shd.slotInfo.State.Status != models.SLOT_STATUS_MIGRATE {
			return nil
		}

		if shd.migrateFrom == nil {
			log.Fatalf("migrateFrom not exist %+v", shd)
		}

		if shd.dst.Master() == shd.migrateFrom.Master() {
			log.Fatalf("the same migrate src and dst, %+v", shd)
		}

		redisConn, err := s.pools.GetConn(shd.migrateFrom.Master())
		if err != nil {
			return errors.Trace(err)
		}

		defer s.pools.ReleaseConn(redisConn)

		redisReader := redisConn.(*redispool.PooledConn).BufioReader()

		err = WriteMigrateKeyCmd(redisConn.(*redispool.PooledConn), shd.dst.Master(), 30*1000, key)
		if err != nil {
			redisConn.Close()
			log.Warningf("migrate key %s error, from %s to %s",
				string(key), shd.migrateFrom.Master(), shd.dst.Master())
			return errors.Trace(err)
		}

		//handle migrate result
		resp, err := parser.Parse(redisReader)
		if err != nil {
			redisConn.Close()
			return errors.Trace(err)
		}

		result, err := resp.Bytes()

		log.Debug("migrate", string(key), "from", shd.migrateFrom.Master(), "to", shd.dst.Master(),
			string(result))

		if resp.Type == parser.ErrorResp {
			redisConn.Close()
			log.Error(string(key), string(resp.Raw), "migrateFrom", shd.migrateFrom.Master())
			return errors.New(string(resp.Raw))
		}

		s.counter.Add("Migrate", 1)
		return nil
	*/
}

/*
func (s *Server) sendBack(c *session, op []byte, keys [][]byte, resp *parser.Resp, result []byte) {
	c.pipelineSeq++
	pr := &PipelineRequest{
		op:    op,
		keys:  keys,
		seq:   c.pipelineSeq,
		backQ: c.backQ,
		req:   resp,
	}

	resp, err := parser.Parse(bufio.NewReader(bytes.NewReader(result)))
	//just send to backQ
	c.backQ <- &PipelineResponse{ctx: pr, err: err, resp: resp}
}

func (s *Server) redisTunnel(c *session) error {
	resp, op, keys, err := getRespOpKeys(c)
	if err != nil {
		return errors.Trace(err)
	}
	k := keys[0]

	opstr := strings.ToUpper(string(op))
	buf, next, err := filter(opstr, keys, c, s.conf.netTimeout)
	if err != nil {
		if len(buf) > 0 { //quit command
			s.sendBack(c, op, keys, resp, buf)
		}
		return errors.Trace(err)
	}

	start := time.Now()
	defer func() {
		recordResponseTime(s.counter, time.Since(start)/1000/1000)
	}()

	s.counter.Add(opstr, 1)
	s.counter.Add("ops", 1)
	if !next {
		s.sendBack(c, op, keys, resp, buf)
		return nil
	}

	if isMulOp(opstr) {
		if len(keys) > 1 { //can not send to redis directly
			var result []byte
			err := s.moper.handleMultiOp(opstr, keys, &result)
			if err != nil {
				return errors.Trace(err)
			}

			s.sendBack(c, op, keys, resp, result)
			return nil
		}
	}

	i := mapKey2Slot(k)

	//pipeline
	c.pipelineSeq++
	pr := &PipelineRequest{
		slotIdx: i,
		op:      op,
		keys:    keys,
		seq:     c.pipelineSeq,
		backQ:   c.backQ,
		req:     resp,
		wg:      &sync.WaitGroup{},
	}
	pr.wg.Add(1)

	s.reqCh <- pr
	pr.wg.Wait()

	return nil
}
*/

/*
func (s *Server) handleConn(c net.Conn) {
	log.Info("new connection", c.RemoteAddr())

	s.counter.Add("connections", 1)
	client := &session{
		Conn:        c,
		r:           bufio.NewReaderSize(c, 32*1024),
		w:           bufio.NewWriterSize(c, 32*1024),
		CreateAt:    time.Now(),
		backQ:       make(chan *PipelineResponse, 1000),
		closeSignal: &sync.WaitGroup{},
	}
	client.closeSignal.Add(1)

	go client.WritingLoop()

	var err error
	defer func() {
		client.closeSignal.Wait() //waiting for writer goroutine

		if err != nil { //todo: fix this ugly error check
			if GetOriginError(err.(*errors.Err)).Error() != io.EOF.Error() {
				log.Warningf("close connection %v, %v", client, errors.ErrorStack(err))
			} else {
				log.Infof("close connection  %v", client)
			}
		} else {
			log.Infof("close connection %v", client)
		}

		s.counter.Add("connections", -1)
	}()

	for {
		err = s.redisTunnel(client)
		if err != nil {
			close(client.backQ)
			return
		}
		client.Ops++
	}
}
*/

func (s *Server) OnSlotRangeChange(param *models.SlotMultiSetParam) {
	log.Warningf("slotRangeChange %+v", param)
	if !validSlot(param.From) || !validSlot(param.To) {
		log.Errorf("invalid slot number, %+v", param)
		return
	}

	for i := param.From; i <= param.To; i++ {
		switch param.Status {
		case models.SLOT_STATUS_OFFLINE:
			s.clearSlot(i)
		case models.SLOT_STATUS_ONLINE:
			s.fillSlot(i, true)
		default:
			log.Errorf("can not handle status %v", param.Status)
		}
	}
}

func (s *Server) OnGroupChange(groupId int) {
	log.Warning("group changed", groupId)

	for i, slot := range s.slots {
		if slot.slotInfo.GroupId == groupId {
			s.fillSlot(i, true)
		}
	}
}

func (s *Server) registerSignal() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, os.Kill)
	go func() {
		<-c
		log.Info("ctrl-c or SIGTERM found, mark offline server")
		done := make(chan error)
		s.evtbus <- &killEvent{done: done}
		<-done
	}()
}

func (s *Server) Run() {
	log.Infof("listening %s on %s", s.conf.proto, s.addr)
	listener, err := net.Listen(s.conf.proto, s.addr)
	if err != nil {
		log.Fatal(err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Warning(errors.ErrorStack(err))
			continue
		}
		go s.handleConn(conn)
	}
}

func (s *Server) responseAction(seq int64) {
	log.Info("send response", seq)
	err := s.topo.DoResponse(int(seq), &s.pi)
	if err != nil {
		log.Error(errors.ErrorStack(err))
	}
}

func (s *Server) getProxyInfo() models.ProxyInfo {
	//todo:send request to evtbus, and get response
	var pi = s.pi
	return pi
}

func (s *Server) getActionObject(seq int, target interface{}) {
	act := &models.Action{Target: target}
	err := s.topo.GetActionWithSeqObject(int64(seq), act)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	log.Infof("%+v", act)
}

func (s *Server) checkAndDoTopoChange(seq int) bool {
	act, err := s.topo.GetActionWithSeq(int64(seq))
	if err != nil { //todo: error is not "not exist"
		log.Fatal(errors.ErrorStack(err), "action seq", seq)
	}

	if !needResponse(act.Receivers, s.pi) { //no need to response
		return false
	}

	log.Warningf("action %v receivers %v", seq, act.Receivers)

	// s.stopTaskRunners()

	switch act.Type {
	case models.ACTION_TYPE_SLOT_MIGRATE, models.ACTION_TYPE_SLOT_CHANGED,
		models.ACTION_TYPE_SLOT_PREMIGRATE:
		slot := &models.Slot{}
		s.getActionObject(seq, slot)
		s.fillSlot(slot.Id, true)
	case models.ACTION_TYPE_SERVER_GROUP_CHANGED:
		serverGroup := &models.ServerGroup{}
		s.getActionObject(seq, serverGroup)
		s.OnGroupChange(serverGroup.Id)
	case models.ACTION_TYPE_SERVER_GROUP_REMOVE:
	//do not care
	case models.ACTION_TYPE_MULTI_SLOT_CHANGED:
		param := &models.SlotMultiSetParam{}
		s.getActionObject(seq, param)
		s.OnSlotRangeChange(param)
	default:
		log.Fatalf("unknown action %+v", act)
	}

	// s.createTaskRunners()

	return true
}

func (s *Server) handleMarkOffline() {
	s.topo.Close(s.pi.Id)
	if s.OnSuicide == nil {
		s.OnSuicide = func() error {
			log.Fatalf("suicide %+v", s.pi)
			return nil
		}
	}

	s.OnSuicide()
}

func (s *Server) handleProxyCommand() {
	pi, err := s.topo.GetProxyInfo(s.pi.Id)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	if pi.State == models.PROXY_STATE_MARK_OFFLINE {
		s.handleMarkOffline()
	}
}

func (s *Server) processAction(e interface{}) {
	if strings.Index(GetEventPath(e), models.GetProxyPath(s.topo.ProductName)) == 0 {
		//proxy event, should be order for me to suicide
		s.handleProxyCommand()
		return
	}

	//re-watch
	nodes, err := s.topo.WatchChildren(models.GetWatchActionPath(s.topo.ProductName), s.evtbus)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	seqs, err := models.ExtraSeqList(nodes)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	if len(seqs) == 0 || !s.topo.IsChildrenChangedEvent(e) {
		return
	}

	//get last pos
	index := -1
	for i, seq := range seqs {
		if s.lastActionSeq < seq {
			index = i
			break
		}
	}

	if index < 0 {
		return
	}

	actions := seqs[index:]
	for _, seq := range actions {
		exist, err := s.topo.Exist(path.Join(s.topo.GetActionResponsePath(seq), s.pi.Id))
		if err != nil {
			log.Fatal(errors.ErrorStack(err))
		}

		if exist {
			continue
		}

		if s.checkAndDoTopoChange(seq) {
			s.responseAction(int64(seq))
		}
	}

	s.lastActionSeq = seqs[len(seqs)-1]
}

func (s *Server) dispatch(r *PipelineRequest) {
	/*
		s.handleMigrateState(r.slotIdx, r.keys[0])
		tr, ok := s.pipeConns[s.slots[r.slotIdx].dst.Master()]
		if !ok {
			//try recreate taskrunner
			if err := s.createTaskRunner(s.slots[r.slotIdx]); err != nil {
				r.backQ <- &PipelineResponse{ctx: r, resp: nil, err: err}
				return
			}

			tr = s.pipeConns[s.slots[r.slotIdx].dst.Master()]
		}
		tr.in <- r
	*/
}

func (s *Server) handleTopoEvent() {
	for {
		select {
		case e := <-s.evtbus:
			switch e.(type) {
			case *killEvent:
				s.handleMarkOffline()
				e.(*killEvent).done <- nil
			default:
				evtPath := GetEventPath(e)
				log.Infof("got event %s, %v, lastActionSeq %d", s.pi.Id, e, s.lastActionSeq)
				if strings.Index(evtPath, models.GetActionResponsePath(s.conf.productName)) == 0 {
					seq, err := strconv.Atoi(path.Base(evtPath))
					if err != nil {
						log.Warning(err)
					} else {
						if seq < s.lastActionSeq {
							log.Info("ignore", seq)
							continue
						}
					}

				}

				log.Infof("got event %s, %v, lastActionSeq %d", s.pi.Id, e, s.lastActionSeq)
				s.processAction(e)
			}
		}
	}
}

func (s *Server) waitOnline() {
	for {
		pi, err := s.topo.GetProxyInfo(s.pi.Id)
		if err != nil {
			log.Fatal(errors.ErrorStack(err))
		}

		if pi.State == models.PROXY_STATE_MARK_OFFLINE {
			s.handleMarkOffline()
		}

		if pi.State == models.PROXY_STATE_ONLINE {
			s.pi.State = pi.State
			println("good, we are on line", s.pi.Id)
			log.Info("we are online", s.pi.Id)
			_, err := s.topo.WatchNode(path.Join(models.GetProxyPath(s.topo.ProductName), s.pi.Id), s.evtbus)
			if err != nil {
				log.Fatal(errors.ErrorStack(err))
			}

			return
		}

		select {
		case e := <-s.evtbus:
			switch e.(type) {
			case *killEvent:
				s.handleMarkOffline()
				e.(*killEvent).done <- nil
			}
		default: //otherwise ignore it
		}

		println("wait to be online ", s.pi.Id)
		log.Warning(s.pi.Id, "wait to be online")

		time.Sleep(3 * time.Second)
	}
}

func (s *Server) FillSlots() {
	for i := 0; i < models.DEFAULT_SLOT_NUM; i++ {
		s.fillSlot(i, false)
	}
}

func (s *Server) RegisterAndWait() {
	_, err := s.topo.CreateProxyInfo(&s.pi)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	_, err = s.topo.CreateProxyFenceNode(&s.pi)
	if err != nil {
		log.Warning(errors.ErrorStack(err))
	}

	s.registerSignal()
	s.waitOnline()
}

func NewServer(addr string, debugVarAddr string, conf *Config) *Server {
	log.Infof("start proxy with config: %+v", conf)
	s := &Server{
		conf:          conf,
		evtbus:        make(chan interface{}, 1000),
		top:           topo.NewTopo(conf.productName, conf.zkAddr, conf.f, conf.provider),
		counter:       stats.NewCounters("router"),
		lastActionSeq: -1,
		addr:          addr,
	}

	proxyHost := strings.Split(addr, ":")[0]
	if len(proxyHost) != 2 {
		log.Panicf("invalid proxy host = %s", addr)
	}
	debugHost := strings.Split(debugVarAddr, ":")[0]
	if len(debugHost) != 2 {
		log.Panicf("invalid debug host = %s", debugVarAddr)
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.PanicErrorf(err, "get host name failed")
	}
	if proxyHost == "0.0.0.0" || strings.HasPrefix(proxyHost, "127.0.0.") {
		proxyHost = hostname
	}
	if debugHost == "0.0.0.0" || strings.HasPrefix(debugHost, "127.0.0.") {
		debugHost = hostname
	}

	s.pi.Id = conf.proxyId
	s.pi.State = models.PROXY_STATE_OFFLINE
	s.pi.Addr = proxyHost + ":" + strings.Split(addr, ":")[1]
	s.pi.DebugVarAddr = debugHost + ":" + strings.Split(debugVarAddr, ":")[1]
	s.pi.Pid = os.Getpid()

	log.Infof("proxy info = %+v", s.pi)

	stats.Publish("evtbus", stats.StringFunc(func() string {
		return strconv.Itoa(len(s.evtbus))
	}))

	s.RegisterAndWait()

	_, err = s.topo.WatchChildren(models.GetWatchActionPath(conf.productName), s.evtbus)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	s.FillSlots()

	//start event handler
	go s.handleTopoEvent()

	log.Info("proxy start ok")

	return s
}
