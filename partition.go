package slapdash

import (
	"sync"
	"sync/atomic"

	"github.com/bits-and-blooms/bloom/v3"
)

type partition struct {
	m       *sync.Mutex
	blocked *atomic.Uint64     // count of the number of blocked requests by this partition.
	mask    *bloom.BloomFilter // mask representing the members of the partition.
}

func (t *partition) touch(id []byte) *partition {
	t.m.Lock()
	defer t.m.Unlock()
	t.mask.Add(id)
	t.blocked.Add(1)
	return t
}

func autopartition(mask *bloom.BloomFilter) *partition {
	return &partition{
		m:       &sync.Mutex{},
		blocked: &atomic.Uint64{},
		mask:    mask,
	}
}
