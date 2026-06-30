// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlock(t *testing.T) {
	t.Run("block time", func(t *testing.T) {
		assert.Equal(t, time.Unix(42, 0), blockTime(42))
	})

	t.Run("sample raw roundtrip", func(t *testing.T) {
		times := []uint64{1, 2, 5}
		data := []float64{1.5, 2.5, 3.5}
		clock := []uint64{1, 2, 3}
		replica := []uint64{9, 9, 9}
		raw := appendSampleRaw(nil, times, data, clock, replica)

		gotTimes := make([]uint64, len(times))
		gotData := make([]float64, len(data))
		gotClock := make([]uint64, len(clock))
		gotReplica := make([]uint64, len(replica))
		require.NoError(t, decodeSampleRaw(raw, gotTimes, gotData, gotClock, gotReplica))
		assert.Equal(t, times, gotTimes)
		assert.Equal(t, data, gotData)
		assert.Equal(t, clock, gotClock)
		assert.Equal(t, replica, gotReplica)
	})

	t.Run("decode errors", func(t *testing.T) {
		var data sampleData
		assert.Error(t, decodeSamples([]byte{1}, 2, &data))
		var counters counterData
		assert.Error(t, decodeCounters([]byte{1}, 2, &counters))

		r := codecReader{data: []byte{1}}
		assert.ErrorIs(t, r.done(), errLongCodec)
	})
}
