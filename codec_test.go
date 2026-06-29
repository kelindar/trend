// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import "testing"

func TestCodec(t *testing.T) {
	if _, err := decode([]byte{version, 0xff}); err == nil {
		t.Fatal("expected s2 decode error")
	}
}
