package slapdash_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/egdaemon/slapdash"
	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
)

func goodID(l *rate.Limiter) TestIDRate {
	return TestIDRate{
		ID:     []byte(fmt.Sprintf("good-%d", time.Now().UnixNano())),
		Limit:  l,
		Blocks: &atomic.Uint64{},
		Banned: &atomic.Bool{},
	}
}

func badID(l *rate.Limiter) TestIDRate {
	return TestIDRate{
		ID:          []byte(fmt.Sprintf("bad-%d", time.Now().UnixNano())),
		Limit:       l,
		BanExpected: true,
		Blocks:      &atomic.Uint64{},
		Banned:      &atomic.Bool{},
	}
}

func checkResults(t *testing.T, ids ...TestIDRate) {
	for _, agent := range ids {
		assert.Equal(t, agent.Banned.Load(), agent.BanExpected, "agent banned status does not match expected: %s", string(agent.ID))
	}
}

type TestIDRate struct {
	ID          []byte
	Limit       *rate.Limiter
	BanExpected bool
	Blocks      *atomic.Uint64
	Banned      *atomic.Bool
}

func TestSingleID(t *testing.T) {
	limit := slapdash.NewLimiter(rate.NewLimiter(rate.Every(time.Millisecond), 1))
	err := limit.Allow(context.Background(), []byte("agent"))
	assert.Nil(t, err, "expected the rate limiter to allow the given agent")
}

func TestBlocked(t *testing.T) {
	limit := slapdash.NewLimiter(rate.NewLimiter(rate.Every(time.Millisecond), 0))
	err := limit.Allow(context.Background(), []byte("agent"))
	assert.EqualError(t, err, "rate limiter blocked: b33aed8f3134996703dc39f9a7c95783")
}

func TestBanned(t *testing.T) {
	limit := slapdash.NewLimiter(rate.NewLimiter(rate.Every(time.Hour), 1))
	agent := badID(rate.NewLimiter(rate.Inf, 1))
	for i := 0; i < 2000; i++ {
		var banned slapdash.Banned
		err := limit.Allow(context.Background(), agent.ID)
		if err != nil {
			agent.Blocks.Add(1)
		}
		if errors.As(err, &banned) {
			agent.Banned.Store(true)
		}
	}

	checkResults(t, agent)
}

func TestWellBehavedShould(t *testing.T) {
	mgood := rate.NewLimiter(rate.Every(100*time.Millisecond), 1)
	limit := slapdash.NewLimiter(rate.NewLimiter(rate.Every(10*time.Millisecond), 5))

	// preseed agent1 into the banned state.
	for ts := time.Now(); time.Since(ts) > 50*time.Millisecond; {
		_ = limit.Allow(context.Background(), []byte("agent1"))
	}

	// then ensure agent2 is still allowed even with agent1 present
	for i := 0; i < 2000; i++ {
		_ = limit.Allow(context.Background(), []byte("agent1"))
		if mgood.Allow() {
			err := limit.Allow(context.Background(), []byte("agent2"))
			if !assert.Nil(t, err) {
				return
			}
		}
	}
}

func TestShouldDetectMisbehavingIDs(t *testing.T) {
	workset := []TestIDRate{
		goodID(rate.NewLimiter(rate.Every(20*time.Millisecond), 1)),
		goodID(rate.NewLimiter(rate.Every(20*time.Millisecond), 1)),
		goodID(rate.NewLimiter(rate.Every(20*time.Millisecond), 1)),
		badID(rate.NewLimiter(rate.Inf, 1)),
		badID(rate.NewLimiter(rate.Inf, 1)),
		badID(rate.NewLimiter(rate.Inf, 1)),
	}

	limit := slapdash.NewLimiter(rate.NewLimiter(rate.Every(10*time.Millisecond), 5))

	for i := 0; i < 200000; i++ {
		for _, agent := range workset {
			if !agent.Limit.Allow() {
				continue
			}

			var banned slapdash.Banned
			err := limit.Allow(context.Background(), agent.ID)
			if err != nil {
				agent.Blocks.Add(1)
			}
			if errors.As(err, &banned) {
				agent.Banned.Store(true)
			}
		}
	}

	for _, agent := range workset {
		assert.Equal(t, agent.Banned.Load(), agent.BanExpected, "agent banned status does not match expected: %s", string(agent.ID))
	}

	assert.Equal(t, uint32(3), limit.Analytics().Banned)
}

func TestShouldProperlyHandleManyWellBehavedID(t *testing.T) {
	workset := []TestIDRate{
		goodID(rate.NewLimiter(rate.Every(2*time.Millisecond), 1)),
		goodID(rate.NewLimiter(rate.Every(2*time.Millisecond), 1)),
		goodID(rate.NewLimiter(rate.Every(2*time.Millisecond), 1)),
		goodID(rate.NewLimiter(rate.Every(2*time.Millisecond), 1)),
		goodID(rate.NewLimiter(rate.Every(2*time.Millisecond), 1)),
		goodID(rate.NewLimiter(rate.Every(2*time.Millisecond), 1)),
		goodID(rate.NewLimiter(rate.Every(2*time.Millisecond), 1)),
		goodID(rate.NewLimiter(rate.Every(2*time.Millisecond), 1)),
	}

	limit := slapdash.NewLimiter(rate.NewLimiter(rate.Every(1*time.Millisecond), 5))

	for i := 0; i < 200000; i++ {
		for _, agent := range workset {
			if !agent.Limit.Allow() {
				continue
			}

			var banned slapdash.Banned
			err := limit.Allow(context.Background(), agent.ID)
			if err != nil {
				agent.Blocks.Add(1)
			}
			if errors.As(err, &banned) {
				agent.Banned.Store(true)
			}
		}
	}

	for _, agent := range workset {
		assert.Equal(t, agent.Banned.Load(), agent.BanExpected, "agent banned status does not match expected: %s", string(agent.ID))
	}

	assert.Equal(t, uint32(0), limit.Analytics().Banned)
}

func BenchmarkLimiterNoRestrictions(b *testing.B) {
	limit := slapdash.NewLimiter(rate.NewLimiter(rate.Inf, 20))
	for i := 0; i < b.N; i++ {
		if err := limit.Allow(context.Background(), []byte("agent")); err != nil {
			_ = err
		}
	}
}

func BenchmarkLimiterBanned(b *testing.B) {
	limit := slapdash.NewLimiter(rate.NewLimiter(rate.Every(time.Minute), 20))
	for i := 0; i < b.N; i++ {
		_ = limit.Allow(context.Background(), []byte("agent"))
	}
}
