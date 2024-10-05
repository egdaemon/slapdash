package slapdash

import "github.com/bits-and-blooms/bloom/v3"

func maskFromPools(pools ...*pool) (mask *bloom.BloomFilter) {
	for _, p := range pools {
		if mask == nil {
			mask = p.mask.Copy()
		} else if err := mask.Merge(p.mask); err != nil {
			panic(err)
		}
	}

	return mask
}

func maskFromPartitions(partitions ...*partition) (mask *bloom.BloomFilter) {
	for _, p := range partitions {
		if mask == nil {
			mask = p.mask.Copy()
		} else if err := mask.Merge(p.mask); err != nil {
			panic(err)
		}
	}

	return mask
}
