// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOption(t *testing.T) {
	store := newMemStore()
	_, err := New(store, func(*DB) error { return errTest })
	assert.ErrorIs(t, err, errTest)
	assert.True(t, store.closed)
}
