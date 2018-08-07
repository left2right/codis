// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"fmt"
	"path/filepath"
	"regexp"

	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

func init() {
	if filepath.Separator != '/' {
		log.Panicf("bad Separator = '%c', must be '/'", filepath.Separator)
	}
}

const (
	CodisDir         = "/codis3"
	CodisProductsDir = "/products/codis"
	TopomRootPath    = "/topom"
)

func ProductPath(product string) string {
	return filepath.Join(CodisProductsDir, product)
}

func ProductDataPath(product string) string {
	return filepath.Join(CodisDir, product)
}

func LockPath(product string) string {
	return filepath.Join(CodisDir, product, "topom")
}

func SlotPath(product string, sid int) string {
	return filepath.Join(CodisDir, product, "slots", fmt.Sprintf("slot-%04d", sid))
}

func GroupDir(product string) string {
	return filepath.Join(CodisDir, product, "group")
}

func ProxyDir(product string) string {
	return filepath.Join(CodisDir, product, "proxy")
}

func GroupPath(product string, gid int) string {
	return filepath.Join(CodisDir, product, "group", fmt.Sprintf("group-%04d", gid))
}

func ProxyPath(product string, token string) string {
	return filepath.Join(CodisDir, product, "proxy", fmt.Sprintf("proxy-%s", token))
}

func SentinelPath(product string) string {
	return filepath.Join(CodisDir, product, "sentinel")
}

func LoadTopom(client Client, product string, must bool) (*Topom, error) {
	b, err := client.Read(LockPath(product), must)
	if err != nil || b == nil {
		return nil, err
	}
	t := &Topom{}
	if err := jsonDecode(t, b); err != nil {
		return nil, err
	}
	return t, nil
}

type Store struct {
	client Client
}

func NewStore(client Client) *Store {
	return &Store{client}
}

func (s *Store) Close() error {
	return s.client.Close()
}

func (s *Store) Client() Client {
	return s.client
}

func (s *Store) Acquire(product string, topom *Topom) error {
	return s.client.Create(LockPath(product), topom.Encode())
}

func (s *Store) Release(product string) error {
	return s.client.Delete(LockPath(product), false)
}

func (s *Store) LoadTopom(product string, must bool) (*Topom, error) {
	return LoadTopom(s.client, product, must)
}

func (s *Store) SlotMappings(product string) ([]*SlotMapping, error) {
	slots := make([]*SlotMapping, MaxSlotNum)
	for i := range slots {
		m, err := s.LoadSlotMapping(product, i, false)
		if err != nil {
			return nil, err
		}
		if m != nil {
			slots[i] = m
		} else {
			slots[i] = &SlotMapping{Id: i}
		}
	}
	return slots, nil
}

func (s *Store) LoadSlotMapping(product string, sid int, must bool) (*SlotMapping, error) {
	b, err := s.client.Read(SlotPath(product, sid), must)
	if err != nil || b == nil {
		return nil, err
	}
	m := &SlotMapping{}
	if err := jsonDecode(m, b); err != nil {
		return nil, err
	}
	return m, nil
}

func (s *Store) UpdateProduct(p *Product) error {
	return s.client.Update(ProductPath(p.Name), p.Encode())
}

func (s *Store) DeleteProduct(product string) error {
	return s.client.Delete(ProductPath(product), false)
}

func (s *Store) DeleteProductData(product string) error {
	return s.client.Delete(ProductDataPath(product), true)
}

func (s *Store) UpdateSlotMapping(product string, m *SlotMapping) error {
	return s.client.Update(SlotPath(product, m.Id), m.Encode())
}

func (s *Store) LoadProduct(product string, must bool) (*Product, error) {
	b, err := s.client.Read(ProductPath(product), must)
	if err != nil || b == nil {
		return nil, err
	}
	p := &Product{}
	if err := jsonDecode(p, b); err != nil {
		return nil, err
	}
	return p, nil
}

func (s *Store) ListProduct() ([]string, error) {
	products, err := s.client.List(CodisProductsDir, false)
	if err != nil {
		return nil, err
	}
	return products, nil
}

func (s *Store) ListGroup(product string) (map[int]*Group, error) {
	groups, err := s.client.List(GroupDir(product), false)
	if err != nil {
		return nil, err
	}
	group := make(map[int]*Group)
	for _, g := range groups {
		b, err := s.client.Read(filepath.Join(GroupDir(product), g), true)
		if err != nil {
			return nil, err
		}
		g := &Group{}
		if err := jsonDecode(g, b); err != nil {
			return nil, err
		}
		group[g.Id] = g
	}
	return group, nil
}

func (s *Store) LoadGroup(product string, gid int, must bool) (*Group, error) {
	b, err := s.client.Read(GroupPath(product, gid), must)
	if err != nil || b == nil {
		return nil, err
	}
	g := &Group{}
	if err := jsonDecode(g, b); err != nil {
		return nil, err
	}
	return g, nil
}

func (s *Store) UpdateGroup(product string, g *Group) error {
	return s.client.Update(GroupPath(product, g.Id), g.Encode())
}

func (s *Store) DeleteGroup(product string, gid int) error {
	return s.client.Delete(GroupPath(product, gid), false)
}

func (s *Store) ListProxy(product string) (map[string]*Proxy, error) {
	tokens, err := s.client.List(ProxyDir(product), false)
	if err != nil {
		return nil, err
	}
	proxy := make(map[string]*Proxy)
	for _, token := range tokens {
		path := filepath.Join(ProxyDir(product), token)
		b, err := s.client.Read(path, true)
		if err != nil {
			return nil, err
		}
		p := &Proxy{}
		if err := jsonDecode(p, b); err != nil {
			return nil, err
		}
		proxy[p.Token] = p
	}
	return proxy, nil
}

func (s *Store) LoadProxy(product string, token string, must bool) (*Proxy, error) {
	b, err := s.client.Read(ProxyPath(product, token), must)
	if err != nil || b == nil {
		return nil, err
	}
	p := &Proxy{}
	if err := jsonDecode(p, b); err != nil {
		return nil, err
	}
	return p, nil
}

func (s *Store) UpdateProxy(product string, p *Proxy) error {
	return s.client.Update(ProxyPath(product, p.Token), p.Encode())
}

func (s *Store) DeleteProxy(product string, token string) error {
	return s.client.Delete(ProxyPath(product, token), false)
}

func (s *Store) LoadSentinel(product string, must bool) (*Sentinel, error) {
	b, err := s.client.Read(SentinelPath(product), must)
	if err != nil || b == nil {
		return nil, err
	}
	p := &Sentinel{}
	if err := jsonDecode(p, b); err != nil {
		return nil, err
	}
	return p, nil
}

func (s *Store) UpdateSentinel(product string, p *Sentinel) error {
	return s.client.Update(SentinelPath(product), p.Encode())
}

func ValidateProduct(name string) error {
	if regexp.MustCompile(`^\w[\w\.\-]*$`).MatchString(name) {
		return nil
	}
	return errors.Errorf("bad product name = %s", name)
}
