// Package ratelimit provides a sliding-window counter used to bound how often
// an expensive recovery action may be retried.
package ratelimit

import (
	"sync"
	"time"
)

// SlidingWindow allows at most Limit events in any Window-long period.
//
// Unlike a lifetime cap it forgets old events, so a long-running call that
// recovers from an outage every few hours is never starved. Unlike a plain
// backoff it puts a hard ceiling on a tight failure loop, which is what stops a
// permanently broken call from hammering the coordinator forever.
//
// The zero value is not usable; construct with New.
type SlidingWindow struct {
	limit  int
	window time.Duration
	// now is swappable so tests do not have to sleep.
	now func() time.Time

	mu     sync.Mutex
	events []time.Time
}

// New returns a limiter allowing limit events per window. A limit of zero or
// less allows everything.
func New(limit int, window time.Duration) *SlidingWindow {
	return &SlidingWindow{
		limit:  limit,
		window: window,
		now:    time.Now,
	}
}

// SetClock replaces the time source. For tests only.
func (s *SlidingWindow) SetClock(now func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = now
}

// Allow records an event and reports whether it was within budget. A rejected
// event is not recorded, so the window reflects permitted attempts only.
func (s *SlidingWindow) Allow() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.limit <= 0 {
		return true
	}
	s.evictLocked(s.now())
	if len(s.events) >= s.limit {
		return false
	}
	s.events = append(s.events, s.now())
	return true
}

// Remaining reports how many further events the current window permits.
func (s *SlidingWindow) Remaining() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.limit <= 0 {
		return -1
	}
	s.evictLocked(s.now())
	if remaining := s.limit - len(s.events); remaining > 0 {
		return remaining
	}
	return 0
}

// Reset forgets every recorded event, restoring the full budget. Called once a
// call is healthy again so the next outage starts from a clean slate.
func (s *SlidingWindow) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = nil
}

func (s *SlidingWindow) evictLocked(now time.Time) {
	cutoff := now.Add(-s.window)
	keep := 0
	for _, at := range s.events {
		if at.After(cutoff) {
			break
		}
		keep++
	}
	if keep > 0 {
		s.events = append(s.events[:0], s.events[keep:]...)
	}
}
