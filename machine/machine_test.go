// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package machine

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestID(t *testing.T) {
	id := ID()
	assert.Positive(t, id)
	assert.Equal(t, id, ID())
	assert.NotZero(t, uint32(id))
}

func TestPack(t *testing.T) {
	id := pack(0x81234567, 42)
	assert.Equal(t, int64(0x012345670000002a), id)
}
