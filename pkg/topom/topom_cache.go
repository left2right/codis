// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

func (s *Topom) dirtySlotsCache(product string, sid int) {
	s.products[product].cache.hooks.PushBack(func() {
		if s.products[product].cache.slots != nil {
			s.products[product].cache.slots[sid] = nil
		}
	})
}

func (s *Topom) dirtyGroupCache(product string, gid int) {
	s.products[product].cache.hooks.PushBack(func() {
		if s.products[product].cache.group != nil {
			s.products[product].cache.group[gid] = nil
		}
	})
}

func (s *Topom) dirtyProxyCache(product string, token string) {
	s.products[product].cache.hooks.PushBack(func() {
		if s.products[product].cache.proxy != nil {
			s.products[product].cache.proxy[token] = nil
		}
	})
}

func (s *Topom) dirtySentinelCache(product string) {
	s.products[product].cache.hooks.PushBack(func() {
		s.products[product].cache.sentinel = nil
	})
}

func (s *Topom) dirtyCacheProduct(product string) {
	s.products[product].cache.hooks.PushBack(func() {
		s.products[product].cache.slots = nil
		s.products[product].cache.group = nil
		s.products[product].cache.proxy = nil
		s.products[product].cache.sentinel = nil
	})
}

func (s *Topom) refillProductCache(name string) error {
	p, ok := s.products[name]
	if !ok {
		log.Warn("product %s nil", name)
		s.products[name] = &product{}
		p = s.products[name]
	}
	for i := p.cache.hooks.Len(); i != 0; i-- {
		e := p.cache.hooks.Front()
		p.cache.hooks.Remove(e).(func())()
	}
	if pdt, err := s.refillCacheProduct(name, p.cache.product); err != nil {
		log.ErrorErrorf(err, "store: load product %s info failed", name)
		return errors.Errorf("store: load product %s info failed", name)
	} else {
		p.cache.product = pdt
	}
	if slots, err := s.refillCacheSlots(name, p.cache.slots); err != nil {
		log.ErrorErrorf(err, "store: load product %s slots failed", name)
		return errors.Errorf("store: load product %s slots failed", name)
	} else {
		p.cache.slots = slots
	}
	if group, err := s.refillCacheGroup(name, p.cache.group); err != nil {
		log.ErrorErrorf(err, "store: load product %s group failed", name)
		return errors.Errorf("store: load product %s group failed", name)
	} else {
		p.cache.group = group
	}
	if proxy, err := s.refillCacheProxy(name, p.cache.proxy); err != nil {
		log.ErrorErrorf(err, "store: load product %s proxy failed", name)
		return errors.Errorf("store: load product %s proxy failed", name)
	} else {
		p.cache.proxy = proxy
	}
	if sentinel, err := s.refillCacheSentinel(name, p.cache.sentinel); err != nil {
		log.ErrorErrorf(err, "store: load product %s sentinel failed", name)
		return errors.Errorf("store: load product %s sentinel failed", name)
	} else {
		p.cache.sentinel = sentinel
	}

	return nil
}

func (s *Topom) refillCacheProduct(name string, product *models.Product) (*models.Product, error) {
	if product != nil {
		return product, nil
	}
	p, err := s.store.LoadProduct(name, false)
	if err != nil {
		return nil, err
	}
	if p != nil {
		return p, nil
	}
	return &models.Product{}, nil
}

func (s *Topom) refillCacheSlots(product string, slots []*models.SlotMapping) ([]*models.SlotMapping, error) {
	if slots == nil {
		return s.store.SlotMappings(product)
	}
	for i, _ := range slots {
		if slots[i] != nil {
			continue
		}
		m, err := s.store.LoadSlotMapping(product, i, false)
		if err != nil {
			return nil, err
		}
		if m != nil {
			slots[i] = m
		} else {
			slots[i] = &models.SlotMapping{Id: i}
		}
	}
	return slots, nil
}

func (s *Topom) refillCacheGroup(product string, group map[int]*models.Group) (map[int]*models.Group, error) {
	if group == nil {
		return s.store.ListGroup(product)
	}
	for i, _ := range group {
		if group[i] != nil {
			continue
		}
		g, err := s.store.LoadGroup(product, i, false)
		if err != nil {
			return nil, err
		}
		if g != nil {
			group[i] = g
		} else {
			delete(group, i)
		}
	}
	return group, nil
}

func (s *Topom) refillCacheProxy(product string, proxy map[string]*models.Proxy) (map[string]*models.Proxy, error) {
	if proxy == nil {
		return s.store.ListProxy(product)
	}
	for t, _ := range proxy {
		if proxy[t] != nil {
			continue
		}
		p, err := s.store.LoadProxy(product, t, false)
		if err != nil {
			return nil, err
		}
		if p != nil {
			proxy[t] = p
		} else {
			delete(proxy, t)
		}
	}
	return proxy, nil
}

func (s *Topom) refillCacheSentinel(product string, sentinel *models.Sentinel) (*models.Sentinel, error) {
	if sentinel != nil {
		return sentinel, nil
	}
	p, err := s.store.LoadSentinel(product, false)
	if err != nil {
		return nil, err
	}
	if p != nil {
		return p, nil
	}
	return &models.Sentinel{}, nil
}

func (s *Topom) storeCreateProduct(p *models.Product) error {
	log.Warnf("create product %s:\n%s", p.Name, p.Encode())
	if err := s.store.UpdateProduct(p); err != nil {
		log.ErrorErrorf(err, "store: create product %s failed", p.Name)
		return errors.Errorf("store: create product %s failed", p.Name)
	}
	return nil
}

func (s *Topom) storeUpdateProduct(p *models.Product) error {
	log.Warnf("update product %s:\n%s", p.Name, p.Encode())
	if err := s.store.UpdateProduct(p); err != nil {
		log.ErrorErrorf(err, "store: update product %s failed", p.Name)
		return errors.Errorf("store: update product %s failed", p.Name)
	}
	return nil
}

func (s *Topom) storeRemoveProduct(product string) error {
	log.Warnf("remove product %s:\n", product)
	if err := s.store.DeleteProduct(product); err != nil {
		log.ErrorErrorf(err, "store: remove product %s failed", product)
		return errors.Errorf("store: remove product %s failed", product)
	}
	if err := s.store.DeleteProductData(product); err != nil {
		log.ErrorErrorf(err, "store: remove product data %s failed", product)
		return errors.Errorf("store: remove product data %s failed", product)
	}
	return nil
}

func (s *Topom) storeUpdateSlotMapping(product string, m *models.SlotMapping) error {
	log.Warnf("update slot-[%d]:\n%s", m.Id, m.Encode())
	if err := s.store.UpdateSlotMapping(product, m); err != nil {
		log.ErrorErrorf(err, "store: update product %s slot-[%d] failed", product, m.Id)
		return errors.Errorf("store: update product %s slot-[%d] failed", product, m.Id)
	}
	return nil
}

func (s *Topom) storeCreateGroup(product string, g *models.Group) error {
	log.Warnf("create group-[%d]:\n%s", g.Id, g.Encode())
	if err := s.store.UpdateGroup(product, g); err != nil {
		log.ErrorErrorf(err, "store: create product %s group-[%d] failed", product, g.Id)
		return errors.Errorf("store: create product %s group-[%d] failed", product, g.Id)
	}
	return nil
}

func (s *Topom) storeUpdateGroup(product string, g *models.Group) error {
	log.Warnf("update group-[%d]:\n%s", g.Id, g.Encode())
	if err := s.store.UpdateGroup(product, g); err != nil {
		log.ErrorErrorf(err, "store: update product %s group-[%d] failed", product, g.Id)
		return errors.Errorf("store: update product %s group-[%d] failed", product, g.Id)
	}
	return nil
}

func (s *Topom) storeRemoveGroup(product string, g *models.Group) error {
	log.Warnf("remove group-[%d]:\n%s", g.Id, g.Encode())
	if err := s.store.DeleteGroup(product, g.Id); err != nil {
		log.ErrorErrorf(err, "store: remove product %s group-[%d] failed", product, g.Id)
		return errors.Errorf("store: remove product %s group-[%d] failed", product, g.Id)
	}
	return nil
}

func (s *Topom) storeCreateProxy(product string, p *models.Proxy) error {
	log.Warnf("create proxy-[%s]:\n%s", p.Token, p.Encode())
	if err := s.store.UpdateProxy(product, p); err != nil {
		log.ErrorErrorf(err, "store: create product %s proxy-[%s] failed", product, p.Token)
		return errors.Errorf("store: create product %s proxy-[%s] failed", product, p.Token)
	}
	return nil
}

func (s *Topom) storeUpdateProxy(product string, p *models.Proxy) error {
	log.Warnf("update proxy-[%s]:\n%s", p.Token, p.Encode())
	if err := s.store.UpdateProxy(product, p); err != nil {
		log.ErrorErrorf(err, "store: update product %s proxy-[%s] failed", product, p.Token)
		return errors.Errorf("store: update product %s proxy-[%s] failed", product, p.Token)
	}
	return nil
}

func (s *Topom) storeRemoveProxy(product string, p *models.Proxy) error {
	log.Warnf("remove proxy-[%s]:\n%s", p.Token, p.Encode())
	if err := s.store.DeleteProxy(product, p.Token); err != nil {
		log.ErrorErrorf(err, "store: remove product %s proxy-[%s] failed", product, p.Token)
		return errors.Errorf("store: remove product %s proxy-[%s] failed", product, p.Token)
	}
	return nil
}

func (s *Topom) storeUpdateSentinel(product string, p *models.Sentinel) error {
	log.Warnf("update sentinel:\n%s", p.Encode())
	if err := s.store.UpdateSentinel(product, p); err != nil {
		log.ErrorErrorf(err, "store: update product %s sentinel failed", product)
		return errors.Errorf("store: update product %s sentinel failed", product)
	}
	return nil
}
