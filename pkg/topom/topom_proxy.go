// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/proxy"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/sync2"
)

func (s *Topom) CreateProxy(product string, productAuth string, addr string) error {
	if err := s.productVerify(product, productAuth); err != nil {
		return err
	}

	s.products[product].mu.Lock()
	defer s.products[product].mu.Unlock()

	ctx, err := s.newProductContext(product)
	if err != nil {
		return err
	}

	p, err := proxy.NewApiClient(addr).Model()
	if err != nil {
		return errors.Errorf("proxy@%s fetch model failed, %s", addr, err)
	}
	c := s.newProxyClient(product, productAuth, p)

	if err := c.XPing(); err != nil {
		return errors.Errorf("proxy@%s check xauth failed, %s", addr, err)
	}
	if ctx.proxy[p.Token] != nil {
		return errors.Errorf("proxy-[%s] already exists", p.Token)
	} else {
		p.Id = ctx.maxProxyId() + 1
	}
	defer s.dirtyProxyCache(product, p.Token)

	if err := s.storeCreateProxy(product, p); err != nil {
		return err
	} else {
		return s.reinitProxy(ctx, p, c)
	}
}

func (s *Topom) OnlineProxy(product string, productAuth string, addr string) error {
	if err := s.productVerify(product, productAuth); err != nil {
		return err
	}

	s.products[product].mu.Lock()
	defer s.products[product].mu.Unlock()

	ctx, err := s.newProductContext(product)
	if err != nil {
		return err
	}

	p, err := proxy.NewApiClient(addr).Model()
	if err != nil {
		return errors.Errorf("proxy@%s fetch model failed", addr)
	}
	c := s.newProxyClient(product, productAuth, p)

	if err := c.XPing(); err != nil {
		return errors.Errorf("proxy@%s check xauth failed", addr)
	}
	defer s.dirtyProxyCache(product, p.Token)

	if d := ctx.proxy[p.Token]; d != nil {
		p.Id = d.Id
		if err := s.storeUpdateProxy(product, p); err != nil {
			return err
		}
	} else {
		p.Id = ctx.maxProxyId() + 1
		if err := s.storeCreateProxy(product, p); err != nil {
			return err
		}
	}
	return s.reinitProxy(ctx, p, c)
}

func (s *Topom) RemoveProxy(product string, productAuth string, token string, force bool) error {
	if err := s.productVerify(product, productAuth); err != nil {
		return err
	}

	s.products[product].mu.Lock()
	defer s.products[product].mu.Unlock()

	ctx, err := s.newProductContext(product)
	if err != nil {
		return err
	}

	p, err := ctx.getProxy(token)
	if err != nil {
		return err
	}
	c := s.newProxyClient(product, productAuth, p)

	if err := c.Shutdown(); err != nil {
		log.WarnErrorf(err, "proxy-[%s] shutdown failed, force remove = %t", token, force)
		if !force {
			return errors.Errorf("proxy-[%s] shutdown failed", p.Token)
		}
	}
	defer s.dirtyProxyCache(product, p.Token)

	return s.storeRemoveProxy(product, p)
}

func (s *Topom) ReinitProxy(product string, productAuth string, token string) error {
	if err := s.productVerify(product, productAuth); err != nil {
		return err
	}

	s.products[product].mu.Lock()
	defer s.products[product].mu.Unlock()

	ctx, err := s.newProductContext(product)
	if err != nil {
		return err
	}

	p, err := ctx.getProxy(token)
	if err != nil {
		return err
	}
	c := s.newProxyClient(product, productAuth, p)

	return s.reinitProxy(ctx, p, c)
}

func (s *Topom) newProxyClient(product string, productAuth string, p *models.Proxy) *proxy.ApiClient {
	c := proxy.NewApiClient(p.AdminAddr)
	c.SetXAuth(product, productAuth, p.Token)
	return c
}

func (s *Topom) reinitProxy(ctx *productContext, p *models.Proxy, c *proxy.ApiClient) error {
	log.Warnf("proxy-[%s] reinit:\n%s", p.Token, p.Encode())
	if err := c.FillSlots(ctx.toSlotSlice(ctx.slots, p)...); err != nil {
		log.ErrorErrorf(err, "proxy-[%s] fillslots failed", p.Token)
		return errors.Errorf("proxy-[%s] fillslots failed", p.Token)
	}
	if err := c.Start(); err != nil {
		log.ErrorErrorf(err, "proxy-[%s] start failed", p.Token)
		return errors.Errorf("proxy-[%s] start failed", p.Token)
	}
	if err := c.SetSentinels(ctx.sentinel); err != nil {
		log.ErrorErrorf(err, "proxy-[%s] set sentinels failed", p.Token)
		return errors.Errorf("proxy-[%s] set sentinels failed", p.Token)
	}
	return nil
}

func (s *Topom) resyncSlotMappingsByGroupId(ctx *productContext, gid int) error {
	return s.resyncSlotMappings(ctx, ctx.getSlotMappingsByGroupId(gid)...)
}

func (s *Topom) resyncSlotMappings(ctx *productContext, slots ...*models.SlotMapping) error {
	if len(slots) == 0 {
		return nil
	}
	var fut sync2.Future
	for _, p := range ctx.proxy {
		fut.Add()
		go func(p *models.Proxy) {
			err := s.newProxyClient(ctx.product, ctx.auth, p).FillSlots(ctx.toSlotSlice(slots, p)...)
			if err != nil {
				log.ErrorErrorf(err, "proxy-[%s] resync slots failed", p.Token)
			}
			fut.Done(p.Token, err)
		}(p)
	}
	for t, v := range fut.Wait() {
		switch err := v.(type) {
		case error:
			if err != nil {
				return errors.Errorf("proxy-[%s] resync slots failed", t)
			}
		}
	}
	return nil
}
