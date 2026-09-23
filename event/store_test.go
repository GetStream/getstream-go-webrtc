package event

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// testEvent stands in for a coordinator or SFU event: the store never looks at
// the event itself, only at what mapFunc projects out of it.
type testEvent struct {
	kind    string
	payload any
}

// identityStore maps an event to its payload, mirroring how the SDK maps an
// SfuEvent to its oneof wrapper and a coordinator frame to its concrete event.
func identityStore() *Store[testEvent] {
	return NewStore(func(e testEvent) any { return e.payload })
}

func TestInterceptorsRunInRegistrationOrder(t *testing.T) {
	t.Parallel()

	store := identityStore()

	var order []string
	store.AddInterceptor(func(testEvent) { order = append(order, "first") })
	store.AddInterceptor(func(testEvent) { order = append(order, "second") })

	store.Intercept(testEvent{kind: "call.ended"})

	require.Equal(t, []string{"first", "second"}, order)
}

func TestInterceptorsSeeEveryEvent(t *testing.T) {
	t.Parallel()

	store := identityStore()

	var seen []string
	store.AddInterceptor(func(e testEvent) { seen = append(seen, e.kind) })

	store.Intercept(testEvent{kind: "call.created"})
	store.Intercept(testEvent{kind: "call.ended"})

	require.Equal(t, []string{"call.created", "call.ended"}, seen)
}

func TestRemoveInterceptorStopsDelivery(t *testing.T) {
	t.Parallel()

	store := identityStore()

	calls := 0
	remove := store.AddInterceptor(func(testEvent) { calls++ })

	store.Intercept(testEvent{})
	remove()
	store.Intercept(testEvent{})

	require.Equal(t, 1, calls)
	require.Empty(t, store.interceptors)
}

func TestRemoveInterceptorLeavesOthersRegistered(t *testing.T) {
	t.Parallel()

	store := identityStore()

	removedCalls, keptCalls := 0, 0
	removeFirst := store.AddInterceptor(func(testEvent) { removedCalls++ })
	store.AddInterceptor(func(testEvent) { keptCalls++ })

	removeFirst()
	store.Intercept(testEvent{})

	require.Zero(t, removedCalls)
	require.Equal(t, 1, keptCalls)
}

func TestRemovingTwiceIsANoop(t *testing.T) {
	t.Parallel()

	store := identityStore()

	calls := 0
	remove := store.AddInterceptor(func(testEvent) { calls++ })
	store.AddInterceptor(func(testEvent) {})

	remove()
	require.NotPanics(t, func() { remove() })

	store.Intercept(testEvent{})
	require.Zero(t, calls)
	require.Len(t, store.interceptors, 1, "removing twice must not take another handler with it")
}

// addCounter has the shape of coordinator.HandleEvent and signal.HandleEvent:
// it wraps the caller's callback in a closure over one func literal, so every
// registration made through it shares that literal's code pointer. //go:noinline
// keeps the compiler from duplicating the literal per call site, which is what
// made the sharing invisible in some builds and not others.
//
//go:noinline
func addCounter(store *Store[testEvent], seen *int) RemoveHandler {
	return store.AddInterceptor(func(testEvent) { *seen++ })
}

func TestRemoveInterceptorTellsIndistinguishableHandlersApart(t *testing.T) {
	t.Parallel()

	store := identityStore()

	var firstCalls, secondCalls int
	addCounter(store, &firstCalls)
	removeSecond := addCounter(store, &secondCalls)

	removeSecond()
	store.Intercept(testEvent{})

	require.Equal(t, 1, firstCalls, "removing the second handler must not silence the first")
	require.Zero(t, secondCalls, "the removed handler must stop being called")
}

func TestInterceptWithNothingRegistered(t *testing.T) {
	t.Parallel()

	store := identityStore()
	store.Intercept(testEvent{kind: "call.ended"})
}

// The store is written from the websocket read loop and read from application
// goroutines, so registration must be safe while events are in flight.
func TestStoreIsSafeUnderConcurrentUse(t *testing.T) {
	t.Parallel()

	store := identityStore()

	var mu sync.Mutex
	delivered := 0
	store.AddInterceptor(func(testEvent) {
		mu.Lock()
		delivered++
		mu.Unlock()
	})

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			store.Intercept(testEvent{kind: "health.check"})
		}()
		go func() {
			defer wg.Done()
			store.AddInterceptor(func(testEvent) {})()
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 8, delivered)
}
