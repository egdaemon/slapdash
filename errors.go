package slapdash

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"time"
)

// represents a banned event.
type Banned interface {
	error
	Banned() time.Duration // returns the duration of the ban.
}

// represents a blocked event.
type Blocked interface {
	error
	Blocked()
}

func NewBlocked(id []byte) Blocked {
	return blocked{id: id}
}

type blocked struct {
	id []byte
}

func (t blocked) Blocked() {}
func (t blocked) Error() string {
	digest := md5.Sum(t.id)
	return fmt.Sprintf("rate limiter blocked: %s", hex.EncodeToString(digest[:]))
}

func NewBanned(id []byte, duration time.Duration) Banned {
	return banned{id: id}
}

type banned struct {
	id       []byte
	duration time.Duration
}

func (t banned) Banned() time.Duration { return t.duration }
func (t banned) Error() string {
	digest := md5.Sum(t.id)
	return fmt.Sprintf("rate limiter banned: %s", hex.EncodeToString(digest[:]))
}
