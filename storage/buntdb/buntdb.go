// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package buntdb

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"time"

	"github.com/kelindar/trend"
	bunt "github.com/tidwall/buntdb"
)

type store struct {
	db     *bunt.DB
	prefix string
	own    bool
}

func init() {
	trend.Register("buntdb", Open)
}

func Open(u *url.URL) (trend.Store, error) {
	path := u.Host + u.Path
	if path == "" {
		path = ":memory:"
	}
	if path != ":memory:" {
		path = filepath.Clean(path)
	}
	db, err := bunt.Open(path)
	if err != nil {
		return nil, err
	}
	return &store{db: db, prefix: u.Query().Get("prefix"), own: true}, nil
}

func New(db *bunt.DB, prefix string) trend.Store {
	return &store{db: db, prefix: prefix}
}

func (s *store) Load(ctx context.Context, key string) ([]byte, error) {
	var out []byte
	err := s.db.View(func(tx *bunt.Tx) error {
		v, err := tx.Get(s.prefix + key)
		if err == bunt.ErrNotFound {
			return nil
		}
		out = []byte(v)
		return nil
	})
	return out, err
}

func (s *store) Update(ctx context.Context, key string, merge func([]byte) ([]byte, error)) error {
	return s.db.Update(func(tx *bunt.Tx) error {
		var old []byte
		v, err := tx.Get(s.prefix + key)
		if err == nil {
			old = []byte(v)
		}
		next, err := merge(old)
		if err != nil {
			return err
		}
		_, _, err = tx.Set(s.prefix+key, string(next), nil)
		return err
	})
}

func (s *store) Delete(ctx context.Context, key string) error {
	return s.db.Update(func(tx *bunt.Tx) error {
		_, err := tx.Delete(s.prefix + key)
		if err == bunt.ErrNotFound {
			return nil
		}
		return err
	})
}

func (s *store) Lease(ctx context.Context, key string, ttl time.Duration) (func(context.Context) error, bool, error) {
	key = s.prefix + key
	now := time.Now().UnixNano()
	ok := false
	err := s.db.Update(func(tx *bunt.Tx) error {
		v, err := tx.Get(key)
		if err == nil {
			var until int64
			_, _ = fmt.Sscanf(v, "%d", &until)
			if until > now {
				return nil
			}
		}
		_, _, err = tx.Set(key, fmt.Sprintf("%d", now+int64(ttl)), &bunt.SetOptions{
			Expires: true,
			TTL:     ttl,
		})
		ok = err == nil
		return err
	})
	return func(context.Context) error {
		return s.db.Update(func(tx *bunt.Tx) error {
			_, err := tx.Delete(key)
			if err == bunt.ErrNotFound {
				return nil
			}
			return err
		})
	}, ok, err
}

func (s *store) Close() error {
	if !s.own {
		return nil
	}
	return s.db.Close()
}
