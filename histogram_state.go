// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package trend

import (
	"errors"
	"math"
	"slices"
)

var errStopHistogram = errors.New("trend: stop histogram iteration")

const (
	histogramExact    = byte(1)
	histogramApprox   = byte(2)
	histogramExactXOR = byte(3)
	histogramMaxBins  = 1024
	histogramAlpha    = 0.01
	histogramBinAlpha = 0.009999
	histogramGamma    = (1 + histogramBinAlpha) / (1 - histogramBinAlpha)
)

var histogramScale = 1 / math.Log(histogramGamma)

type histogramOp struct {
	replica uint64
	clock   uint64
}

type histogramData struct {
	Time    []uint64
	Data    []float64
	Clock   []uint64
	Replica []uint64
	Buckets []histogramBucket
}

type histogramBucket struct {
	Time uint64
	Data []byte
}

type histogramValue struct {
	count    uint64
	sum      float64
	min      float64
	max      float64
	exact    []float64
	zero     uint64
	negative map[int]uint64
	positive map[int]uint64
}

func (d *histogramData) Add(t uint64, value float64, clock, replica uint64) {
	d.Time = append(d.Time, t)
	d.Data = append(d.Data, value)
	d.Clock = append(d.Clock, clock)
	d.Replica = append(d.Replica, replica)
}

func (d *histogramData) Reset() {
	d.Time = d.Time[:0]
	d.Data = d.Data[:0]
	d.Clock = d.Clock[:0]
	d.Replica = d.Replica[:0]
	d.Buckets = d.Buckets[:0]
}

func (d histogramData) count() int {
	return len(d.Time) + len(d.Buckets)
}

func (d histogramData) appendable() bool {
	return nondecreasing(d.Time) && histogramBucketsIncreasing(d.Buckets)
}

func (d histogramData) minTime() (uint64, bool) {
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

func histogramBucketsIncreasing(buckets []histogramBucket) bool {
	for i := 1; i < len(buckets); i++ {
		if buckets[i].Time <= buckets[i-1].Time {
			return false
		}
	}
	return true
}

func (d *histogramData) Merge(delta histogramData) {
	ops := make(map[uint64]map[histogramOp]uint64, len(d.Time)+len(delta.Time))
	add := func(t, replica, clock uint64, value float64) {
		if ops[t] == nil {
			ops[t] = make(map[histogramOp]uint64)
		}
		op := histogramOp{replica: replica, clock: clock}
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
	d.Buckets = mergeHistogramBuckets(d.Buckets, delta.Buckets)
}

func mergeHistogramBuckets(a, b []histogramBucket) []histogramBucket {
	buckets := make(map[uint64]*histogramValue, len(a)+len(b))
	merge := func(items []histogramBucket) {
		for _, x := range items {
			if buckets[x.Time] == nil {
				buckets[x.Time] = new(histogramValue)
			}
			_ = buckets[x.Time].mergeBytes(x.Data)
		}
	}
	merge(a)
	merge(b)
	times := sortedTimes(buckets)
	out := make([]histogramBucket, 0, len(times))
	for _, t := range times {
		out = append(out, histogramBucket{Time: t, Data: buckets[t].encodeApprox()})
	}
	return out
}

func (d *histogramData) Compact(cutoff, span uint64) {
	if len(d.Time) == 0 {
		return
	}
	buckets := make(map[uint64]*histogramValue, len(d.Buckets))
	for _, b := range d.Buckets {
		v := new(histogramValue)
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
			buckets[bt] = new(histogramValue)
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
		d.Buckets = append(d.Buckets, histogramBucket{Time: t, Data: buckets[t].encodeApprox()})
	}
}

func (v *histogramValue) addStats(value float64) {
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

func (v *histogramValue) addExact(value float64) {
	v.addStats(value)
	v.exact = append(v.exact, value)
}

func (v *histogramValue) addApprox(value float64) {
	v.addStats(value)
	v.addBin(value, 1)
	v.collapse()
}

func (v *histogramValue) addBin(value float64, count uint64) {
	if value == 0 {
		v.zero += count
		return
	}
	key := int(math.Ceil(math.Log(math.Abs(value)) * histogramScale))
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

func (v *histogramValue) approximate() {
	if v.exact == nil {
		return
	}
	for _, value := range v.exact {
		v.addBin(value, 1)
	}
	v.exact = nil
	v.collapse()
}

func (v *histogramValue) collapse() {
	for len(v.negative)+len(v.positive) > histogramMaxBins {
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

func (v *histogramValue) mergeBytes(data []byte) error {
	other, err := decodeHistogram(data)
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

func (v *histogramValue) encode() Histogram {
	if v.exact != nil {
		slices.Sort(v.exact)
		return v.histogram(histogramExact, appendExactValues(nil, v.exact))
	}
	return v.histogram(histogramApprox, appendHistogramApproxData(nil, v))
}

func (v *histogramValue) histogram(kind byte, data []byte) Histogram {
	return Histogram{
		data:  data,
		count: v.count,
		sum:   v.sum,
		min:   v.min,
		max:   v.max,
		kind:  kind,
	}
}

func (v *histogramValue) encodeApprox() []byte {
	v.approximate()
	return appendHistogramApprox(nil, v)
}

func histogramBinValue(key int) float64 {
	return 2 * math.Pow(histogramGamma, float64(key)) / (histogramGamma + 1)
}

func sortedIntKeys(values map[int]uint64) []int {
	keys := make([]int, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func appendHistogramHeader(dst []byte, kind byte, v *histogramValue) []byte {
	dst = append(dst, kind)
	dst = appendUvarint(dst, v.count)
	dst, _ = appendFloat(dst, v.sum, 0)
	dst, _ = appendFloat(dst, v.min, 0)
	dst, _ = appendFloat(dst, v.max, 0)
	return dst
}

func appendHistogramExact(dst []byte, v *histogramValue) []byte {
	dst = appendHistogramHeader(dst, histogramExact, v)
	return appendFloatXor(dst, v.exact)
}

func appendHistogramApprox(dst []byte, v *histogramValue) []byte {
	dst = appendHistogramHeader(dst, histogramApprox, v)
	return appendHistogramApproxData(dst, v)
}

func appendHistogramApproxData(dst []byte, v *histogramValue) []byte {
	dst = appendUvarint(dst, v.zero)
	dst = appendHistogramBins(dst, v.negative)
	return appendHistogramBins(dst, v.positive)
}

func appendHistogramBins(dst []byte, bins map[int]uint64) []byte {
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

func decodeHistogram(data []byte) (histogramValue, error) {
	kind, out, r, err := decodeHistogramHeader(data)
	switch {
	case err != nil:
		return histogramValue{}, err
	case kind == 0:
		return out, nil
	}
	switch kind {
	case histogramExact:
		if out.count > uint64(int(^uint(0)>>1)) {
			return histogramValue{}, errLargeCodec
		}
		out.exact = make([]float64, int(out.count))
		r.floatXor(out.exact)
	case histogramApprox:
		out.zero = r.uvarint()
		out.negative = r.histogramBins()
		out.positive = r.histogramBins()
	default:
		return histogramValue{}, errVarintCodec
	}
	if err := r.done(); err != nil {
		return histogramValue{}, err
	}
	switch {
	case len(out.negative)+len(out.positive) > histogramMaxBins:
		return histogramValue{}, errLargeCodec
	case out.count != uint64(len(out.exact))+out.zero+sumBins(out.negative)+sumBins(out.positive):
		return histogramValue{}, errShapeCodec
	}
	return out, nil
}

func storedHistogram(data []byte) (Histogram, error) {
	kind, out, r, err := decodeHistogramHeader(data)
	if err != nil {
		return Histogram{}, err
	}
	payload := r.data
	var count uint64
	switch kind {
	case histogramExact:
		for range out.count {
			r.uvarint()
		}
		count = out.count
	case histogramApprox:
		count = r.uvarint()
		var bins int
		r.histogramBinsEach(func(_ int, n uint64) bool {
			count += n
			bins++
			return true
		})
		r.histogramBinsEach(func(_ int, n uint64) bool {
			count += n
			bins++
			return true
		})
		if bins > histogramMaxBins {
			return Histogram{}, errLargeCodec
		}
	default:
		return Histogram{}, errVarintCodec
	}
	if err := r.done(); err != nil {
		return Histogram{}, err
	}
	if count != out.count {
		return Histogram{}, errShapeCodec
	}
	if kind == histogramExact {
		kind = histogramExactXOR
	}
	return out.histogram(kind, payload), nil
}

func decodeHistogramHeader(data []byte) (byte, histogramValue, codecReader, error) {
	if len(data) == 0 {
		return 0, histogramValue{}, codecReader{}, nil
	}
	r := codecReader{data: data}
	kind := r.byte()
	out := histogramValue{
		count: r.uvarint(),
		sum:   r.float(),
		min:   r.float(),
		max:   r.float(),
	}
	if r.err != nil {
		return 0, histogramValue{}, codecReader{}, r.err
	}
	return kind, out, r, nil
}

func (r *codecReader) float() float64 {
	return floatFromReversed(r.uvarint())
}

func (r *codecReader) histogramBins() map[int]uint64 {
	out := make(map[int]uint64)
	r.histogramBinsEach(func(key int, count uint64) bool {
		out[key] = count
		return true
	})
	return out
}

func (r *codecReader) histogramBinsEach(yield func(int, uint64) bool) {
	n := r.count()
	if n > histogramMaxBins {
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
