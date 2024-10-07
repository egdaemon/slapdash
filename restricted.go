package slapdash

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
)

func newRestricted() *restricted {
	var (
		banned atomic.Pointer[pool]
	)
	banned.Store(poolFromTracker(1.0, trackerPresetBanned()))

	return &restricted{
		mutfreq:        100,
		bannedwindow:   200 * time.Millisecond,
		bannedreaped:   time.Now(),
		m:              &sync.Mutex{},
		onehot:         trackerPresetSieve(),
		banned:         &banned,
		recentlybanned: poolFromTracker(1.0, trackerPresetSieve()),
		ring: []*pool{
			poolFromTracker(1.0, trackerPresetSieve()),
			poolFromTracker(1.0, trackerPresetSieve()),
			poolFromTracker(1.0, trackerPresetSieve()),
			poolFromTracker(1.0, trackerPresetSieve()),
		},
		warm: presetMaskSieve(),
	}
}

type restricted struct {
	m *sync.Mutex
	// the number of blocked requests before updating statistics
	mutfreq      int64
	bannedwindow time.Duration
	bannedreaped time.Time
	// onehot set represents agents who have been limited this cycle due to the rate limit being reached.
	// and represent the next set of agents to be added to the restrictive cycle.
	onehot *tracker
	// pool representing the banned agents
	banned *atomic.Pointer[pool]
	// recently banned, allows us to maintain the banned pool
	// with different settings from the sieve ring.
	// this contains the most recently banned set.
	recentlybanned *pool
	// ring buffer of pools that are increasingly prohibitive.
	ring []*pool
	// represents agents in the ring
	warm *bloom.BloomFilter
	// number of requests blocked
	blocked atomic.Int64
}

func (t *restricted) debug() {
	t.m.Lock()
	defer t.m.Unlock()

	for idx, p := range t.onehot.active() {
		log.Printf("onehot partition %d %d %d", t.onehot.id, idx, p.mask.ApproximatedSize())
	}

	for idx, p := range t.ring {
		log.Printf("warm partition %d %d %f %d", p.tracker.id, idx, p.ratio, p.mask.ApproximatedSize())
	}

	banned := t.banned.Load()
	for idx, p := range banned.tracker.partitions {
		if p.blocked.Load() == 0 {
			continue
		}
		log.Printf("banned partition %d %d %d %d", banned.tracker.id, idx, p.blocked.Load(), p.mask.ApproximatedSize())
	}

}

// we periodically disturb the banned pool to shuffle the agent distribution across the partitions
// and to release masks from partitions that are no longer generating blocked requests.
func (t *restricted) prunebanned() {
	if elapsed := time.Since(t.bannedreaped); elapsed < t.bannedwindow {
		return
	}

	banned := t.banned.Load()
	pool := newbannedpool(maskFromPartitions(banned.tracker.active()...))
	sieve := poolFromTracker(1.0, trackerPresetSieve())
	ts := time.Now()

	t.m.Lock()
	defer t.m.Unlock()
	t.banned.Store(pool)
	t.recentlybanned = sieve
	t.bannedreaped = ts

}

func (t *restricted) promote() {
	t.m.Lock()
	defer t.m.Unlock()

	promote := t.ring[0]

	updated := make([]*pool, 0, len(t.ring))
	updated = append(updated, t.ring[1:len(t.ring)]...)
	updated = append(updated, poolFromTracker(1.0, t.onehot))
	warmmask := maskFromPools(updated...)
	bannedmask := maskFromPools(promote)

	if err := bannedmask.Merge(maskFromPartitions(t.recentlybanned.tracker.partitions...)); err != nil {
		log.Println("unable to updated banned mask", err)
	}

	t.onehot = duptracker(t.onehot)
	t.recentlybanned = newpool(1.0, duptracker(t.recentlybanned.tracker), bannedmask)
	t.warm = warmmask
	t.ring = updated
}

// bookkeeping the internal ring buffer periodically
// when warm agents are blocked, since in theory
// the more often warm agents hit the faster they'll
// promoted to more restrictive tiers until they're
// banned entirely.
func (t *restricted) bookkeeping() {
	if t.blocked.Add(-1) != -1 {
		return
	}
	defer t.blocked.Swap(t.mutfreq)

	t.prunebanned()
	t.promote()
}

// Insert the agent into the restricted tree, incrementing metadata as we go.
func (t *restricted) Insert(ctx context.Context, id []byte) error {
	if t.banned.Load().Test(id) {
		t.bookkeeping()
		return NewBanned(id, t.bannedwindow)
	}

	if t.recentlybanned.Test(id) {
		t.banned.Load().Insert(id)
		t.bookkeeping()
		return NewBanned(id, t.bannedwindow)
	}

	if !t.warm.Test(id) {
		t.onehot.insert(id)
		t.bookkeeping()
		return NewBlocked(id)
	}

	for _, p := range t.ring {
		if p.Test(id) {
			t.bookkeeping()
			return NewBlocked(id)
		}
	}

	return nil
}

// check if the agent is allowed to make the request
func (t *restricted) Allow(ctx context.Context, id []byte) error {
	if t.banned.Load().Test(id) {
		t.bookkeeping()
		return NewBanned(id, t.bannedwindow)
	}

	if t.recentlybanned.Test(id) {
		t.banned.Load().Insert(id)
		t.bookkeeping()
		return NewBanned(id, t.bannedwindow)
	}

	if t.warm.Test(id) {
		for _, p := range t.ring {
			if !p.Test(id) {
				continue
			}

			t.bookkeeping() // update the restricted structures if necessary
			return NewBlocked(id)
		}
	}

	return nil
}
