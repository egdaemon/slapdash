package slapdash

import (
	"crypto/md5"
	"encoding/binary"
	"math/rand/v2"
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
)

func genkey(a uint64, b []byte) []byte {
	buf := make([]byte, 0, binary.Size(a)+len(b))
	binary.NativeEndian.AppendUint64(buf, a)
	copy(buf, b)
	return buf
}

func calcpartition(n int, id []byte) int {
	digested := md5.Sum(id)
	index := binary.LittleEndian.Uint64(digested[:]) % uint64(n)
	return int(index)
}

func poolFromTracker(ratio float32, t *tracker) *pool {
	return newpool(
		ratio,
		duptracker(t),
		maskFromPartitions(t.partitions...),
	)
}

func newbannedpool(mask *bloom.BloomFilter) *pool {
	return newpool(1.0, trackerPresetBanned(), mask)
}

func newpool(ratio float32, t *tracker, mask *bloom.BloomFilter) *pool {
	return &pool{
		m:       &sync.Mutex{},
		ratio:   ratio,
		tracker: t,
		mask:    mask,
	}
}

type pool struct {
	m        *sync.Mutex
	ratio    float32
	tracker  *tracker
	mask     *bloom.BloomFilter
	inserted *bloom.BloomFilter
}

func (t *pool) Test(id []byte) bool {
	if !t.mask.Test(id) {
		return false
	}

	if t.inserted != nil && !t.inserted.Test(id) {
		return false
	}

	if blocked := t.ratio > rand.Float32(); !blocked {
		return false
	}

	t.tracker.insert(id)
	return true
}

func (t *pool) Insert(id []byte) bool {
	t.m.Lock()
	defer t.m.Unlock()

	if t.inserted == nil {
		t.inserted = t.mask.Copy().ClearAll()
	}

	t.tracker.insert(id)
	t.inserted.Add(id)

	return true
}

// Presets for the warm mask guarding the sieves.
// The goal for this is to limit the number of ids we have to check.
// but we can be a little more sloppy here.
func presetMaskSieve() *bloom.BloomFilter {
	return bloom.NewWithEstimates(128, 0.001)
}

// generate a mask for a given set of constraints.
func maskBounded(partitions, upperbound uint, falsepos float64) *bloom.BloomFilter {
	psize := upperbound / partitions
	return bloom.NewWithEstimates(psize, falsepos)
}

// Presets for tracking ids through the various stages.
// The goal for these is to rapidly identify misbehaving ids.
// Therefore we want a low false positive rate and a larger number of partitions.
func trackerPresetSieve() *tracker {
	return autotracker(32, presetMaskSieve())
}

// Presets for tracking banned ids.
// by the time an id gets into this set we should be fairly certain
// that its misbehaving. Therefore we tune for a large pool of ids
// and a large number of partitions to allow for ids to be released.
func trackerPresetBanned() *tracker {
	const partitions = 128
	return autotracker(partitions, maskBounded(partitions, 8192, 0.001))
}

// generate a tracker with the given number of partions each with a copy
// of the given mask.
func autotracker(partitions uint, m *bloom.BloomFilter) *tracker {
	return gentracker(partitions, m)
}

// create a duplicate tracker but with a unique id.
func duptracker(o *tracker) *tracker {
	partitions := make([]*partition, 0, len(o.partitions))
	for _, mask := range o.partitions {
		partitions = append(partitions, autopartition(mask.mask.Copy().ClearAll()))
	}

	return &tracker{
		id:         rand.Uint64(),
		partitions: partitions,
	}
}

func gentracker(n uint, proto *bloom.BloomFilter) *tracker {
	partitions := make([]*partition, 0, n)
	for i := uint(0); i < n; i++ {
		partitions = append(partitions, autopartition(proto.Copy()))
	}

	return &tracker{
		id:         rand.Uint64(),
		partitions: partitions,
	}
}

// tracker is used to track poorly behaving ids
type tracker struct {
	id         uint64
	partitions []*partition
}

func (t *tracker) active() (remaining []*partition) {
	remaining = make([]*partition, 0, len(t.partitions))
	for _, p := range t.partitions {
		if p.blocked.Load() == 0 {
			continue
		}

		remaining = append(remaining, p)
	}
	return remaining
}

func (t *tracker) insert(id []byte) {
	idx := calcpartition(len(t.partitions), genkey(t.id, id))
	t.partitions[idx].touch(id)
}
