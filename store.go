// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"
)

// Store is the atomic byte store used by trend.
type Store interface {
	Load(context.Context, string) ([]byte, error)
	Update(context.Context, string, func([]byte) ([]byte, error)) error
	Delete(context.Context, string) error
	Close() error
}

type leaser interface {
	Lease(context.Context, string, time.Duration) (release func(context.Context) error, ok bool, err error)
}

var stores sync.Map

// Register registers a storage adapter.
func Register(scheme string, open func(*url.URL) (Store, error)) {
	stores.Store(scheme, open)
}

func openStore(uri string) (Store, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	v, ok := stores.Load(u.Scheme)
	if !ok {
		return nil, fmt.Errorf("trend: store %q is not registered", u.Scheme)
	}
	return v.(func(*url.URL) (Store, error))(u)
}
