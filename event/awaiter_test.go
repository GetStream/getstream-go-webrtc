package event

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The two payload types stand in for two SfuEvent oneof wrappers: the awaiter
// has to pick its own out of the stream and ignore everything else.
type migrationComplete struct{ sessionID string }

type participantJoined struct{ userID string }

func TestAwaitReturnsTheMatchingEvent(t *testing.T) {
	t.Parallel()

	store := identityStore()
	awaiter := NewEventAwaiter[*migrationComplete](store, func(*migrationComplete) bool { return true })
	require.Len(t, store.eventAwaiters, 1, "the constructor registers the awaiter")

	store.Intercept(testEvent{payload: &migrationComplete{sessionID: "session-1"}})

	got, err := awaiter.Await(time.Second)
	require.NoError(t, err)
	require.Equal(t, "session-1", got.sessionID)
}

func TestAwaitIgnoresOtherEventTypes(t *testing.T) {
	t.Parallel()

	store := identityStore()
	awaiter := NewEventAwaiter[*migrationComplete](store, func(*migrationComplete) bool { return true })

	// This is the failure mode the migration await had: matching on the inner
	// message type while the store projects the wrapper means no event ever
	// matches and the caller burns its whole timeout.
	store.Intercept(testEvent{payload: &participantJoined{userID: "alice"}})
	require.Len(t, store.eventAwaiters, 1, "a non-matching event must not consume the awaiter")

	store.Intercept(testEvent{payload: &migrationComplete{sessionID: "session-2"}})

	got, err := awaiter.Await(time.Second)
	require.NoError(t, err)
	require.Equal(t, "session-2", got.sessionID)
}

func TestAwaitAppliesThePredicate(t *testing.T) {
	t.Parallel()

	store := identityStore()
	awaiter := NewEventAwaiter[*participantJoined](store, func(e *participantJoined) bool {
		return e.userID == "bob"
	})

	store.Intercept(testEvent{payload: &participantJoined{userID: "alice"}})
	require.Len(t, store.eventAwaiters, 1, "a rejected event must not consume the awaiter")

	store.Intercept(testEvent{payload: &participantJoined{userID: "bob"}})

	got, err := awaiter.Await(time.Second)
	require.NoError(t, err)
	require.Equal(t, "bob", got.userID)
}

func TestAwaitTimesOut(t *testing.T) {
	t.Parallel()

	store := identityStore()
	awaiter := NewEventAwaiter[*migrationComplete](store, func(*migrationComplete) bool { return true })

	got, err := awaiter.Await(10 * time.Millisecond)
	require.ErrorIs(t, err, ErrAwaitTimeout)
	require.Nil(t, got)
}

// A timed-out awaiter has to be reaped, otherwise every event for the rest of
// the call keeps walking a growing list of dead awaiters — and the reaping
// happens on the next event, not on the timeout itself.
func TestTimedOutAwaiterIsDroppedOnTheNextEvent(t *testing.T) {
	t.Parallel()

	store := identityStore()
	awaiter := NewEventAwaiter[*migrationComplete](store, func(*migrationComplete) bool { return true })

	_, err := awaiter.Await(10 * time.Millisecond)
	require.ErrorIs(t, err, ErrAwaitTimeout)
	require.Len(t, store.eventAwaiters, 1)

	store.Intercept(testEvent{payload: &participantJoined{userID: "alice"}})
	require.Empty(t, store.eventAwaiters)
}

// The store drops an awaiter as soon as it matches. If it did not, delivering a
// second matching event would send on the awaiter's closed channel and panic
// the websocket read loop.
func TestSecondMatchingEventDoesNotReachAClosedAwaiter(t *testing.T) {
	t.Parallel()

	store := identityStore()
	awaiter := NewEventAwaiter[*migrationComplete](store, func(*migrationComplete) bool { return true })

	store.Intercept(testEvent{payload: &migrationComplete{sessionID: "first"}})
	require.Empty(t, store.eventAwaiters)

	require.NotPanics(t, func() {
		store.Intercept(testEvent{payload: &migrationComplete{sessionID: "second"}})
	})

	got, err := awaiter.Await(time.Second)
	require.NoError(t, err)
	require.Equal(t, "first", got.sessionID)
}

func TestCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	store := identityStore()
	awaiter := NewEventAwaiter[*migrationComplete](store, func(*migrationComplete) bool { return true })

	awaiter.Close()
	require.NotPanics(t, awaiter.Close)
}

// Several awaiters can be outstanding at once — the reconnect paths keep one per
// awaited SFU event — and a match must consume only the awaiters that matched.
func TestOnlyMatchingAwaitersAreConsumed(t *testing.T) {
	t.Parallel()

	store := identityStore()
	migration := NewEventAwaiter[*migrationComplete](store, func(*migrationComplete) bool { return true })
	joined := NewEventAwaiter[*participantJoined](store, func(*participantJoined) bool { return true })
	require.Len(t, store.eventAwaiters, 2)

	store.Intercept(testEvent{payload: &migrationComplete{sessionID: "session-3"}})
	require.Len(t, store.eventAwaiters, 1)

	got, err := migration.Await(time.Second)
	require.NoError(t, err)
	require.Equal(t, "session-3", got.sessionID)

	_, err = joined.Await(10 * time.Millisecond)
	require.ErrorIs(t, err, ErrAwaitTimeout)
}

// Two awaiters for the same event both get it: Intercept walks the whole list
// rather than stopping at the first match.
func TestEveryMatchingAwaiterGetsTheEvent(t *testing.T) {
	t.Parallel()

	store := identityStore()
	first := NewEventAwaiter[*migrationComplete](store, func(*migrationComplete) bool { return true })
	second := NewEventAwaiter[*migrationComplete](store, func(*migrationComplete) bool { return true })

	store.Intercept(testEvent{payload: &migrationComplete{sessionID: "session-4"}})
	require.Empty(t, store.eventAwaiters)

	got, err := first.Await(time.Second)
	require.NoError(t, err)
	require.Equal(t, "session-4", got.sessionID)

	got, err = second.Await(time.Second)
	require.NoError(t, err)
	require.Equal(t, "session-4", got.sessionID)
}
