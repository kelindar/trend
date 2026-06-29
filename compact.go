// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"context"
	"math/rand"
	"time"
)

const leaseTTL = 30 * time.Second

func (db *DB) compact(ctx context.Context, key string) error {
	if db.compactAfter <= 0 || db.compactSpan <= 0 {
		return nil
	}

	if leaser, ok := db.store.(leaser); ok {
		release, locked, err := leaser.Lease(ctx, "compact:"+key, leaseTTL)
		if err != nil || !locked {
			return err
		}
		defer func() { _ = release(ctx) }()
	}

	cutoff := time.Now().Add(-db.compactAfter)
	err := db.store.Update(ctx, key, func(old []byte) ([]byte, error) {
		current, err := decode(old)
		if err != nil {
			return nil, err
		}
		current.compact(cutoff, db.compactSpan)
		return current.marshal()
	})
	if err == nil {
		db.dropCache(key)
	}
	return err
}

func (db *DB) compactLoop(ctx context.Context) {
	for {
		wait := db.compactEvery
		if db.compactJitter > 0 {
			wait += time.Duration(rand.Int63n(int64(db.compactJitter)))
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
			db.seen.Range(func(k, _ any) bool {
				_ = db.compact(ctx, k.(string))
				return true
			})
		case <-ctx.Done():
			timer.Stop()
			return
		}
	}
}
