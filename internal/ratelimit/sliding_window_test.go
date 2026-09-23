package ratelimit_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/internal/ratelimit"
)

// fakeClock lets the tests move time without sleeping.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newLimiter(limit int, window time.Duration) (*ratelimit.SlidingWindow, *fakeClock) {
	clock := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	limiter := ratelimit.New(limit, window)
	limiter.SetClock(clock.Now)
	return limiter, clock
}

func TestAllowsUpToTheLimitThenRejects(t *testing.T) {
	t.Parallel()

	limiter, _ := newLimiter(3, time.Minute)

	require.True(t, limiter.Allow())
	require.True(t, limiter.Allow())
	require.True(t, limiter.Allow())
	require.False(t, limiter.Allow(), "the budget is spent")
	require.Zero(t, limiter.Remaining())
}

// The window is rolling, not a lifetime cap: a call that recovers from one
// outage per hour must never run out of budget.
func TestBudgetReturnsAsTheWindowSlidesPast(t *testing.T) {
	t.Parallel()

	limiter, clock := newLimiter(2, time.Minute)

	require.True(t, limiter.Allow())
	clock.Advance(30 * time.Second)
	require.True(t, limiter.Allow())
	require.False(t, limiter.Allow())

	// The first event ages out, so exactly one slot frees up.
	clock.Advance(31 * time.Second)
	require.Equal(t, 1, limiter.Remaining())
	require.True(t, limiter.Allow())
	require.False(t, limiter.Allow())
}

// A rejected attempt must not consume budget, otherwise a caller polling a
// spent limiter would keep the window permanently full.
func TestRejectedAttemptsDoNotExtendTheWindow(t *testing.T) {
	t.Parallel()

	limiter, clock := newLimiter(1, time.Minute)

	require.True(t, limiter.Allow())
	clock.Advance(30 * time.Second)
	require.False(t, limiter.Allow())

	clock.Advance(31 * time.Second)
	require.True(t, limiter.Allow(), "the rejected attempt did not push the window forward")
}

func TestResetRestoresTheFullBudget(t *testing.T) {
	t.Parallel()

	limiter, _ := newLimiter(2, time.Minute)

	require.True(t, limiter.Allow())
	require.True(t, limiter.Allow())
	require.False(t, limiter.Allow())

	limiter.Reset()

	require.Equal(t, 2, limiter.Remaining())
	require.True(t, limiter.Allow())
}

func TestNonPositiveLimitAllowsEverything(t *testing.T) {
	t.Parallel()

	limiter, _ := newLimiter(0, time.Minute)

	for range 100 {
		require.True(t, limiter.Allow())
	}
	require.Equal(t, -1, limiter.Remaining(), "an unlimited window reports no remaining count")
}

func TestConcurrentAllowNeverExceedsTheLimit(t *testing.T) {
	t.Parallel()

	const limit = 10
	limiter := ratelimit.New(limit, time.Minute)

	var (
		mu      sync.Mutex
		allowed int
		wg      sync.WaitGroup
	)
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if limiter.Allow() {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	require.Equal(t, limit, allowed)
}
