// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"errors"
	"testing"
)

func TestOption(t *testing.T) {
	store := newMemStore()
	_, err := New(store, func(*DB) error { return errTest })
	if !errors.Is(err, errTest) || !store.closed {
		t.Fatal("expected option error and close")
	}
}
