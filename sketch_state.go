// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"errors"
	"math"
	"slices"
)

var errStopSketch = errors.New("trend: stop sketch iteration")

const (
	sketchExact    = byte(1)
	sketchApprox   = byte(2)
	sketchExactXOR = byte(3)
	sketchMaxBins  = 1024
	sketchAlpha    = 0.01
	sketchBinAlpha = 0.009999
	sketchGamma    = (1 + sketchBinAlpha) / (1 - sketchBinAlpha)
)

var sketchScale = 1 / math.Log(sketchGamma)

type sketchOp struct {
	replica uint64
	clock   uint64
}

type sketchData struct {
	Time    []uint64
	Data    []float64
	Clock   []uint64
	Replica []uint64
	Buckets []sketchBucket
}

type sketchBucket struct {
	Time uint64
	Data []byte
}

type sketchValue struct {
	count    uint64
	sum      float64
	min      float64
	max      float64
	exact    []float64
	zero     uint64
	negative map[int]uint64
	positive map[int]uint64
}

func (d *sketchData) Add(t uint64, value float64, clock, replica uint64) {
	d.Time = append(d.Time, t)
	d.Data = append(d.Data, value)
	d.Clock = append(d.Clock, clock)
	d.Replica = append(d.Replica, replica)
}

func (d *sketchData) Reset() {
	d.Time = d.Time[:0]
	d.Data = d.Data[:0]
	d.Clock = d.Clock[:0]
	d.Replica = d.Replica[:0]
	d.Buckets = d.Buckets[:0]
}

func (d sketchData) count() int {
	return len(d.Time) + len(d.Buckets)
}

func (d sketchData) appendable() bool {
	return nondecreasing(d.Time) && sketchBucketsIncreasing(d.Buckets)
}

func (d sketchData) minTime() (uint64, bool) {
	var out uint64
	var ok bool
	for _, t := range d.Time {
		if !ok || t < out {
			out = t
		}
		ok = true
	}
	for _, b := range d.Buckets {
		if !ok || b.Time < out {
			out = b.Time
		}
		ok = true
	}
	return out, ok
}

func sketchBucketsIncreasing(buckets []sketchBucket) bool {
	for i := 1; i < len(buckets); i++ {
		if buckets[i].Time <= buckets[i-1].Time {
			return false
		}
	}
	return true
}

func (d *sketchData) Merge(delta sketchData) {
	ops := make(map[uint64]map[sketchOp]uint64, len(d.Time)+len(delta.Time))
	add := func(t, replica, clock uint64, value float64) {
		if ops[t] == nil {
			ops[t] = make(map[sketchOp]uint64)
		}
		op := sketchOp{replica: replica, clock: clock}
		bits := math.Float64bits(value)
		if previous, ok := ops[t][op]; !ok || bits > previous {
			ops[t][op] = bits
		}
	}
	for i, t := range d.Time {
		add(t, d.Replica[i], d.Clock[i], d.Data[i])
	}
	for i, t := range delta.Time {
		add(t, delta.Replica[i], delta.Clock[i], delta.Data[i])
	}
	times := sortedTimes(ops)
	d.Time = d.Time[:0]
	d.Data = d.Data[:0]
	d.Clock = d.Clock[:0]
	d.Replica = d.Replica[:0]
	for _, t := range times {
		for op, value := range ops[t] {
			d.Add(t, math.Float64frombits(value), op.clock, op.replica)
		}
	}
	d.Buckets = mergeSketchBuckets(d.Buckets, delta.Buckets)
}

func mergeSketchBuckets(a, b []sketchBucket) []sketchBucket {
	buckets := make(map[uint64]*sketchValue, len(a)+len(b))
	merge := func(items []sketchBucket) {
		for _, x := range items {
			if buckets[x.Time] == nil {
				buckets[x.Time] = new(sketchValue)
			}
			_ = buckets[x.Time].mergeBytes(x.Data)
		}
	}
	merge(a)
	merge(b)
	times := sortedTimes(buckets)
	out := make([]sketchBucket, 0, len(times))
	for _, t := range times {
		out = append(out, sketchBucket{Time: t, Data: buckets[t].encodeApprox()})
	}
	return out
}

func (d *sketchData) Compact(cutoff, span uint64) {
	if len(d.Time) == 0 {
		return
	}
	buckets := make(map[uint64]*sketchValue, len(d.Buckets))
	for _, b := range d.Buckets {
		v := new(sketchValue)
		_ = v.mergeBytes(b.Data)
		buckets[b.Time] = v
	}
	keep := 0
	for i, t := range d.Time {
		if t >= cutoff {
			d.Time[keep] = d.Time[i]
			d.Data[keep] = d.Data[i]
			d.Clock[keep] = d.Clock[i]
			d.Replica[keep] = d.Replica[i]
			keep++
			continue
		}
		bt := bucketOf(t, span)
		if buckets[bt] == nil {
			buckets[bt] = new(sketchValue)
		}
		buckets[bt].addApprox(d.Data[i])
	}
	d.Time = d.Time[:keep]
	d.Data = d.Data[:keep]
	d.Clock = d.Clock[:keep]
	d.Replica = d.Replica[:keep]
	times := sortedTimes(buckets)
	d.Buckets = d.Buckets[:0]
	for _, t := range times {
		d.Buckets = append(d.Buckets, sketchBucket{Time: t, Data: buckets[t].encodeApprox()})
	}
}

func (v *sketchValue) addStats(value float64) {
	if v.count == 0 {
		v.min, v.max = value, value
	}
	v.count++
	v.sum += value
	if value < v.min {
		v.min = value
	}
	if value > v.max {
		v.max = value
	}
}

func (v *sketchValue) addExact(value float64) {
	v.addStats(value)
	v.exact = append(v.exact, value)
}

func (v *sketchValue) addApprox(value float64) {
	v.addStats(value)
	v.addBin(value, 1)
	v.collapse()
}

func (v *sketchValue) addBin(value float64, count uint64) {
	if value == 0 {
		v.zero += count
		return
	}
	key := int(math.Ceil(math.Log(math.Abs(value)) * sketchScale))
	if value < 0 {
		if v.negative == nil {
			v.negative = make(map[int]uint64)
		}
		v.negative[key] += count
		return
	}
	if v.positive == nil {
		v.positive = make(map[int]uint64)
	}
	v.positive[key] += count
}

func (v *sketchValue) approximate() {
	if v.exact == nil {
		return
	}
	for _, value := range v.exact {
		v.addBin(value, 1)
	}
	v.exact = nil
	v.collapse()
}

func (v *sketchValue) collapse() {
	for len(v.negative)+len(v.positive) > sketchMaxBins {
		nk, hasNegative := minKey(v.negative)
		pk, hasPositive := minKey(v.positive)
		if hasNegative && (!hasPositive || nk <= pk) {
			v.zero += v.negative[nk]
			delete(v.negative, nk)
			continue
		}
		v.zero += v.positive[pk]
		delete(v.positive, pk)
	}
}

func minKey(values map[int]uint64) (int, bool) {
	var out int
	var ok bool
	for k := range values {
		if !ok || k < out {
			out, ok = k, true
		}
	}
	return out, ok
}

func (v *sketchValue) mergeBytes(data []byte) error {
	other, err := decodeSketch(data)
	if err != nil {
		return err
	}
	if other.count == 0 {
		return nil
	}
	wasEmpty := v.count == 0
	if v.count == 0 {
		v.min, v.max = other.min, other.max
	} else {
		v.min = min(v.min, other.min)
		v.max = max(v.max, other.max)
	}
	v.count += other.count
	v.sum += other.sum
	if wasEmpty && other.exact != nil {
		v.exact = append(v.exact, other.exact...)
		return nil
	}
	if other.exact != nil && v.exact != nil {
		v.exact = append(v.exact, other.exact...)
		return nil
	}
	v.approximate()
	other.approximate()
	v.zero += other.zero
	if v.negative == nil && len(other.negative) > 0 {
		v.negative = make(map[int]uint64, len(other.negative))
	}
	if v.positive == nil && len(other.positive) > 0 {
		v.positive = make(map[int]uint64, len(other.positive))
	}
	for k, n := range other.negative {
		v.negative[k] += n
	}
	for k, n := range other.positive {
		v.positive[k] += n
	}
	v.collapse()
	return nil
}

func (v *sketchValue) encode() Sketch {
	if v.exact != nil {
		slices.Sort(v.exact)
		return v.sketch(sketchExact, appendExactValues(nil, v.exact))
	}
	return v.sketch(sketchApprox, appendSketchApproxData(nil, v))
}

func (v *sketchValue) sketch(kind byte, data []byte) Sketch {
	return Sketch{
		data:  data,
		count: v.count,
		sum:   v.sum,
		min:   v.min,
		max:   v.max,
		kind:  kind,
	}
}

func (v *sketchValue) encodeApprox() []byte {
	v.approximate()
	return appendSketchApprox(nil, v)
}

func sketchBinValue(key int) float64 {
	return 2 * math.Pow(sketchGamma, float64(key)) / (sketchGamma + 1)
}

func sortedIntKeys(values map[int]uint64) []int {
	keys := make([]int, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func appendSketchHeader(dst []byte, kind byte, v *sketchValue) []byte {
	dst = append(dst, kind)
	dst = appendUvarint(dst, v.count)
	dst, _ = appendFloat(dst, v.sum, 0)
	dst, _ = appendFloat(dst, v.min, 0)
	dst, _ = appendFloat(dst, v.max, 0)
	return dst
}

func appendSketchExact(dst []byte, v *sketchValue) []byte {
	dst = appendSketchHeader(dst, sketchExact, v)
	return appendFloatXor(dst, v.exact)
}

func appendSketchApprox(dst []byte, v *sketchValue) []byte {
	dst = appendSketchHeader(dst, sketchApprox, v)
	return appendSketchApproxData(dst, v)
}

func appendSketchApproxData(dst []byte, v *sketchValue) []byte {
	dst = appendUvarint(dst, v.zero)
	dst = appendSketchBins(dst, v.negative)
	return appendSketchBins(dst, v.positive)
}

func appendSketchBins(dst []byte, bins map[int]uint64) []byte {
	keys := sortedIntKeys(bins)
	dst = appendUvarint(dst, uint64(len(keys)))
	var previous int64
	for i, key := range keys {
		current := int64(key)
		if i == 0 {
			dst = appendVarint(dst, current)
		} else {
			dst = appendUvarint(dst, uint64(current-previous))
		}
		dst = appendUvarint(dst, bins[key])
		previous = current
	}
	return dst
}

func appendVarint(dst []byte, value int64) []byte {
	return appendUvarint(dst, uint64(value<<1)^uint64(value>>63))
}

func decodeSketch(data []byte) (sketchValue, error) {
	kind, out, r, err := decodeSketchHeader(data)
	switch {
	case err != nil:
		return sketchValue{}, err
	case kind == 0:
		return out, nil
	}
	switch kind {
	case sketchExact:
		if out.count > uint64(int(^uint(0)>>1)) {
			return sketchValue{}, errLargeCodec
		}
		out.exact = make([]float64, int(out.count))
		r.floatXor(out.exact)
	case sketchApprox:
		out.zero = r.uvarint()
		out.negative = r.sketchBins()
		out.positive = r.sketchBins()
	default:
		return sketchValue{}, errVarintCodec
	}
	if err := r.done(); err != nil {
		return sketchValue{}, err
	}
	switch {
	case len(out.negative)+len(out.positive) > sketchMaxBins:
		return sketchValue{}, errLargeCodec
	case out.count != uint64(len(out.exact))+out.zero+sumBins(out.negative)+sumBins(out.positive):
		return sketchValue{}, errShapeCodec
	}
	return out, nil
}

func storedSketch(data []byte) (Sketch, error) {
	kind, out, r, err := decodeSketchHeader(data)
	if err != nil {
		return Sketch{}, err
	}
	payload := r.data
	var count uint64
	switch kind {
	case sketchExact:
		for range out.count {
			r.uvarint()
		}
		count = out.count
	case sketchApprox:
		count = r.uvarint()
		var bins int
		r.sketchBinsEach(func(_ int, n uint64) bool {
			count += n
			bins++
			return true
		})
		r.sketchBinsEach(func(_ int, n uint64) bool {
			count += n
			bins++
			return true
		})
		if bins > sketchMaxBins {
			return Sketch{}, errLargeCodec
		}
	default:
		return Sketch{}, errVarintCodec
	}
	if err := r.done(); err != nil {
		return Sketch{}, err
	}
	if count != out.count {
		return Sketch{}, errShapeCodec
	}
	if kind == sketchExact {
		kind = sketchExactXOR
	}
	return out.sketch(kind, payload), nil
}

func decodeSketchHeader(data []byte) (byte, sketchValue, codecReader, error) {
	if len(data) == 0 {
		return 0, sketchValue{}, codecReader{}, nil
	}
	r := codecReader{data: data}
	kind := r.byte()
	out := sketchValue{
		count: r.uvarint(),
		sum:   r.float(),
		min:   r.float(),
		max:   r.float(),
	}
	if r.err != nil {
		return 0, sketchValue{}, codecReader{}, r.err
	}
	return kind, out, r, nil
}

func (r *codecReader) float() float64 {
	return floatFromReversed(r.uvarint())
}

func (r *codecReader) sketchBins() map[int]uint64 {
	out := make(map[int]uint64)
	r.sketchBinsEach(func(key int, count uint64) bool {
		out[key] = count
		return true
	})
	return out
}

func (r *codecReader) sketchBinsEach(yield func(int, uint64) bool) {
	n := r.count()
	if n > sketchMaxBins {
		r.err = errLargeCodec
		return
	}
	var current int64
	for i := range n {
		if i == 0 {
			current = r.varint()
		} else {
			current += int64(r.uvarint())
		}
		count := r.uvarint()
		if count == 0 || int64(int(current)) != current {
			r.err = errShapeCodec
			return
		}
		if !yield(int(current), count) {
			return
		}
	}
}

func (r *codecReader) varint() int64 {
	value := r.uvarint()
	return int64(value>>1) ^ -int64(value&1)
}

func sumBins(bins map[int]uint64) uint64 {
	var out uint64
	for _, count := range bins {
		out += count
	}
	return out
}
