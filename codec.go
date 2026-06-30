// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"fmt"

	"github.com/kelindar/binary"
	"github.com/klauspost/compress/s2"
)

const version = 1

func decode(b []byte) (*series, error) {
	switch {
	case len(b) == 0:
		return &series{}, nil
	case b[0] != version:
		return nil, fmt.Errorf("trend: unsupported version %d", b[0])
	}

	out, err := s2.Decode(nil, b[1:])
	if err != nil {
		return nil, err
	}
	var s series
	_ = binary.Unmarshal(out, &s)
	return &s, nil
}

func (s *series) Marshal() ([]byte, error) {
	encoded, _ := binary.Marshal(s)
	dst := make([]byte, s2.MaxEncodedLen(len(encoded))+1)
	dst[0] = version
	out := s2.EncodeBetter(dst[1:], encoded)
	return dst[:len(out)+1], nil
}
