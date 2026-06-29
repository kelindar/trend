// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"time"

	"github.com/kelindar/trend"
	"github.com/redis/go-redis/v9"
)

type store struct {
	db     redis.UniversalClient
	prefix string
	own    bool
}

func init() {
	trend.Register("redis", Open)
}

func Open(u *url.URL) (trend.Store, error) {
	clone := *u
	q := clone.Query()
	prefix := q.Get("prefix")
	q.Del("prefix")
	clone.RawQuery = q.Encode()
	opt, err := redis.ParseURL(clone.String())
	if err != nil {
		return nil, err
	}
	db := redis.NewClient(opt)
	return &store{db: db, prefix: prefix, own: true}, nil
}

func New(db redis.UniversalClient, prefix string) trend.Store {
	return &store{db: db, prefix: prefix}
}

func (s *store) Load(ctx context.Context, key string) ([]byte, error) {
	out, err := s.db.Get(ctx, s.prefix+key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	return out, err
}

func (s *store) Update(ctx context.Context, key string, merge func([]byte) ([]byte, error)) error {
	key = s.prefix + key
	return s.db.Watch(ctx, func(tx *redis.Tx) error {
		old, err := tx.Get(ctx, key).Bytes()
		if err == redis.Nil {
			err = nil
		}
		if err != nil {
			return err
		}
		next, err := merge(old)
		if err != nil {
			return err
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, key, next, 0)
			return nil
		})
		return err
	}, key)
}

func (s *store) Delete(ctx context.Context, key string) error {
	return s.db.Del(ctx, s.prefix+key).Err()
}

func (s *store) Lease(ctx context.Context, key string, ttl time.Duration) (func(context.Context) error, bool, error) {
	key = s.prefix + key
	token := make([]byte, 16)
	_, _ = rand.Read(token)
	value := hex.EncodeToString(token)
	ok, err := s.db.SetNX(ctx, key, value, ttl).Result()
	return func(ctx context.Context) error {
		return s.db.Eval(ctx, `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) end return 0`, []string{key}, value).Err()
	}, ok, err
}

func (s *store) Close() error {
	if !s.own {
		return nil
	}
	return s.db.Close()
}
