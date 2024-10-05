package slapdash

import (
	"context"

	"golang.org/x/time/rate"
)

func NewLimiter(r *rate.Limiter) *Limiter {
	return &Limiter{
		limit:      r,
		restricted: newRestricted(),
	}
}

type Limiter struct {
	limit      *rate.Limiter
	restricted *restricted
}

func (t *Limiter) Allow(ctx context.Context, id []byte) error {
	// if request is going to be blocked regardless we just limit update the restricted set appropriately.
	// if the agent is well behaved they'll obey the rate limit response and slow down.
	if !t.limit.Allow() {
		return t.restricted.Insert(ctx, id)
	}

	// check if the id is restricted from making requests.
	if err := t.restricted.Allow(ctx, id); err != nil {
		return err
	}

	return nil
}

func (t *Limiter) Debug() {
	t.restricted.debug()
}

func (t *Limiter) Analytics() Analytics {
	return Analytics{
		Banned: t.restricted.banned.Load().mask.ApproximatedSize(),
	}
}

type Analytics struct {
	Banned uint32
}
