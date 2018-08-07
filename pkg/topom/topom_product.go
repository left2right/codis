// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/errors"
)

func (s *Topom) productExist(product string) bool {
	if _, ok := s.products[product]; ok {
		return true
	}
	return false
}

func (s *Topom) productVerify(product string, auth string) error {
	if s.products[product] == nil {
		return errors.Errorf("product %s does not exist", product)
	} else if s.products[product].cache.product == nil {
		return errors.Errorf("product %s is not in cache", product)
	} else if s.products[product].cache.product.Auth != auth {
		return errors.Errorf("product %s auth %s does not match", product, auth)
	} else {
		return nil
	}
}

func (s *Topom) productAuthMatch(product string, auth string) error {
	if s.products[product] == nil {
		return nil
	} else if s.products[product].cache.product == nil {
		return nil
	} else if s.products[product].cache.product.Auth != auth {
		return errors.Errorf("product %s auth %s does not match", product, auth)
	} else {
		return nil
	}
}

// create only if not exists, need check topom cache and embed etcd
func (s *Topom) CreateProduct(product string, productAuth string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.productExist(product) {
		return errors.Errorf("product %s already exists", product)
	}
	p := &models.Product{
		Name: product,
		Auth: productAuth,
	}
	// need use txn
	if err := s.storeCreateProduct(p); err != nil {
		return err
	} else {
		return s.startProduct(p.Name)
	}
}

func (s *Topom) UpdateProduct(product string, productAuth string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	exists := s.productExist(product)
	if err := s.productAuthMatch(product, productAuth); err != nil {
		return err
	}
	p := &models.Product{
		Name: product,
		Auth: productAuth,
	}
	if err := s.storeCreateProduct(p); err != nil {
		return err
	} else {
		if exists {
			return s.refillProductCache(p.Name)
		} else {
			return s.startProduct(p.Name)
		}

	}
}

func (s *Topom) RemoveProduct(product string, productAuth string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.productVerify(product, productAuth); err != nil {
		return err
	}
	p, err := s.store.LoadProduct(product, false)
	if err != nil {
		return errors.Errorf("load product %s from etcd", product)
	}
	if p == nil {
		return errors.Errorf("product %s doesn't exist in etcd but exist in topom cache, danger, need administrator!!!", product)
	}

	ctx, err := s.newProductContext(product)
	if err != nil {
		return err
	}
	// in order to make sure no user is using the product
	if len(ctx.proxy) != 0 {
		return errors.Errorf("before remove product %s, plz make sure no proxy is in this product", product)
	}

	// clear cache and stop product running goroutines
	if err := s.stopProduct(product); err != nil {
		return err
	}

	return s.storeRemoveProduct(product)
}
